package io.github.retz.digdag.plugin;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigElement;
import io.digdag.spi.CommandLogger;
import io.digdag.spi.OperatorContext;
import io.digdag.spi.TaskExecutionException;
import io.digdag.spi.TaskResult;
import io.digdag.util.BaseOperator;
import io.github.retz.cli.ClientCLIConfig;
import io.github.retz.cli.TimestampHelper;
import io.github.retz.protocol.GetFileResponse;
import io.github.retz.protocol.GetJobResponse;
import io.github.retz.protocol.Response;
import io.github.retz.protocol.ScheduleResponse;
import io.github.retz.protocol.data.Job;
import io.github.retz.protocol.exception.JobNotFoundException;
import io.github.retz.web.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.text.ParseException;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import static java.nio.charset.StandardCharsets.UTF_8;

public class RetzRunApiOperator extends BaseOperator {

    private static final Logger LOGGER = LoggerFactory.getLogger(RetzRunApiOperator.class);

    private final CommandLogger clog;
    private final RetzOperatorConfig config;

    RetzRunApiOperator(OperatorContext context, RetzOperatorConfig config, CommandLogger clog) {
        super(context);
        this.config = config;
        this.clog = clog;
    }

    private static final String STATE_JOB_ID = "jobId";
    private static final String STATE_JOB_STATE = "jobState";
    private static final String STATE_POLL_ITERATION = "pollIteration";
    private static final String STATE_OFFSET = "offset";
    private static final String STATE_RESULT_CODE = "result";
    private static final String STATE_REASON = "reason";

    private static final int MAX_FETCH_FILE_LENGTH = 65536;

    @Override
    public TaskResult runTask() {

        Config state = request.getLastStateParams().deepCopy();
        TaskResult taskResult;

        Optional<Integer> maybeEcode = state.getOptional(STATE_RESULT_CODE, Integer.class);
        if (!maybeEcode.isPresent()) {
            try (Client webClient = createClient()) {
                Optional<Integer> maybeJobId = state.getOptional(STATE_JOB_ID, Integer.class);
                Job job;
                if (!maybeJobId.isPresent()) {
                    job = processSchedule(webClient, state);
                } else {
                    job = getJob(maybeJobId.get(), webClient);
                }
                state.set(STATE_JOB_STATE, job.state().toString());
                LOGGER.debug("job: {}", job);

                TaskExecutionException nextPolling = processGetFile(job, webClient, state);
                LOGGER.debug("next polling: {}", state);

                throw nextPolling;
            }
        } else {
            taskResult = processFinish(maybeEcode.get(), state);
        }
        return taskResult;
    }

    private TaskResult processFinish(int result, Config state) {
        if (result != 0) {
            //TODO state params do not show when task failure
            throw new TaskExecutionException(String.format(
                    "retz_run: Job(id=%s) failed. " +
                            "| retz-info: state=%s, reason=%s " +
                            "| log: `digdag log %s %s`",
                    state.get(STATE_JOB_ID, String.class),
                    state.get(STATE_JOB_STATE, String.class), state.get(STATE_REASON, String.class),
                    request.getAttemptId(), request.getTaskName()));
        }

        TaskResult taskResult = TaskResult.empty(request);
        taskResult.getStoreParams()
                .getNestedOrSetEmpty(RetzOperatorConfig.KEY_CONFIG_ROOT)
                .set("last_job_id", state.get(STATE_JOB_ID, String.class));

        return taskResult;
    }

    private Job processSchedule(Client webClient, Config state) {
        Job job = createJob();
        Response res;
        try {
            res = webClient.schedule(job);
        } catch (IOException ex) {
            throw new RuntimeException("Failed to schedule Retz job", ex);
        }
        if (!(res instanceof ScheduleResponse)) {
            throw new TaskExecutionException(String.format(
                    "Failed to schedule Retz job: %s",
                    res.status()));
        }

        Job scheduled = ((ScheduleResponse) res).job();
        LOGGER.info("Job(id={}) scheduled: {}", scheduled.id(), scheduled.state());

        initializeTaskState(scheduled, state);

        return scheduled;
    }

    private void initializeTaskState(Job job, Config state) {
        state.set(STATE_JOB_ID, job.id());
        state.set(STATE_POLL_ITERATION, 0);
        state.set(STATE_OFFSET, 0L);
    }


    private TaskExecutionException processGetFile(Job job, Client webClient, Config state) {

        switch(job.state()) {
            case QUEUED:
                checkTimeout(job, webClient);
                return nextPolling(state);
            case STARTING:
            case STARTED:
                checkTimeout(job, webClient);
                getWholeFileByState(job, webClient, "stdout", state);
                return nextPolling(state);
            case FINISHED:
            case KILLED:
                getWholeFileByState(job, webClient, "stdout", state);
                if (config.getVerbose()) {
                    LOGGER.info("Job(id={}) finished to get stdout, will get stderr", job.id());
                }
                getWholeFile(job, webClient, "stderr", 0);
                return finishJob(job, state);
            default:
                throw new IllegalStateException("unexpected status: " + job.state());
        }
    }

    private void checkTimeout(Job job, Client webClient) {
        int timeout = config.getTimeout();
        if (timeout > 0) {
            Date scheduled;
            try {
                scheduled = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX").parse(job.scheduled());
            } catch (ParseException ex) {
                throw Throwables.propagate(ex);
            }
            Calendar limit = Calendar.getInstance();
            limit.setTime(scheduled);
            limit.add(Calendar.MINUTE, timeout);

            if (limit.compareTo(Calendar.getInstance()) < 0) {
                try {
                    webClient.kill(job.id());
                    throw new TaskExecutionException(String.format(
                            "Job(id=%s) has been killed due to timeout after %d minute(s)",
                            job.id(), timeout));
                } catch (IOException ex) {
                    throw new RuntimeException(String.format(
                            "Job(id=%s) failed to kill timedout job", job.id()), ex);
                }
            }
        }
    }

    private TaskExecutionException nextPolling(Config state) {
        int iteration = state.get(STATE_POLL_ITERATION, Integer.class);
        int interval = exponentialBackoffInterval(iteration);
        state.set(STATE_POLL_ITERATION, ++iteration);

        return TaskExecutionException.ofNextPolling(interval, ConfigElement.copyOf(state));
    }

    private void getWholeFileByState(Job job, Client webClient, String filename, Config state) {
            long offset = state.get(STATE_OFFSET, Long.class);
            long bytesRead = getWholeFile(job, webClient, filename, offset);

            state.set(STATE_OFFSET, offset + bytesRead);
            if (bytesRead != 0) {
                state.set(STATE_POLL_ITERATION, 0);
            }
    }

    private long getWholeFile(Job job, Client webClient, String filename, long offset) {
        try {
            return readFileUntilEmpty(webClient, job.id(), filename, offset, new CommandLoggerBridge(clog, System.out));
        } catch (IOException ex) {
            throw new RuntimeException(String.format(
                    "Job(id=%s) failed with unexpected error", job.id()), ex);

        }
    }

    private static long readFileUntilEmpty(Client c, int id, String filename, long offset, OutputStream out) throws IOException {
        long current = offset;

        while (true) {
            Response res = c.getFile(id, filename, current, MAX_FETCH_FILE_LENGTH);
            if (res instanceof GetFileResponse) {
                GetFileResponse getFileResponse = (GetFileResponse) res;

                // Check data
                if (getFileResponse.file().isPresent()) {
                    if (getFileResponse.file().get().data().isEmpty()) {
                        // All contents fetched
                        return current - offset;

                    } else {
                        byte[] data = getFileResponse.file().get().data().getBytes(UTF_8);
                        LOGGER.debug("Fetched data length={}, current={}", data.length, current);
                        out.write(data);
                        current = current + data.length;
                    }
                } else {
                    //LOG.info("{}: ,{}", filename, current);
                    return current - offset;
                }
            } else {
                LOGGER.error(res.status());
                throw new IOException(res.status());
            }
        }
    }

    private TaskExecutionException finishJob(Job job, Config state) {
        LOGGER.info("Job(id={}) finished in {} seconds. status: {}",
                job.id(),
                getElapsed(job.started(), job.finished()),
                job.state());

        state.set(STATE_RESULT_CODE, job.result());
        state.remove(STATE_POLL_ITERATION);
        state.remove(STATE_OFFSET);

        if (job.result() != 0) {
            state.set(STATE_REASON, job.reason());
        }

        return TaskExecutionException.ofNextPolling(0, ConfigElement.copyOf(state));
    }

    private String getElapsed(String started, String finished) {
        String elapsed;
        try {
            if (started == null || finished == null) {
                elapsed = "-";
            } else {
                elapsed = String.valueOf(TimestampHelper.diffMillisec(finished, started) / 1000.0);
            }
        } catch(ParseException ex) {
            throw Throwables.propagate(ex);
        }
        return elapsed;
    }


    private Client createClient() {
        ClientCLIConfig fileConfig;
        try {
            fileConfig = new ClientCLIConfig(config.getClientConfig(workspace));
        } catch (IOException | URISyntaxException ex) {
            throw Throwables.propagate(ex);
        }

        return Client.newBuilder(fileConfig.getUri())
                .setAuthenticator(fileConfig.getAuthenticator())
                .checkCert(!fileConfig.insecure())
                .setVerboseLog(config.getVerbose())
                .build();
    }

    private Job createJob() {
        String appName = config.getAppName();
        String remoteCmd = config.getRemoteCommand();
        Properties envProps = config.getEnvProps();

        int cpu = config.getCpu();
        int mem = config.getMemory();
        int disk = config.getDisk();
        int gpu = config.getGpu();
        int ports = config.getPorts();
        int priority = config.getPriority();
        String name = config.getJobName();
        List<String> tags = config.getTags();

        boolean verbose = config.getVerbose();

        Job job = new Job(appName, remoteCmd, envProps, cpu, mem, disk, gpu, ports);
        job.setPriority(priority);
        job.setName(name);
        job.addTags(tags);

        if (verbose) {
            LOGGER.info("Job created: {}", job);
        }

        return job;
    }

    private Job getJob(int id, Client webClient) {
        Response res;
        try {
            res = webClient.getJob(id);
        } catch (IOException ex) {
            throw Throwables.propagate(ex);
        }

        if (res instanceof GetJobResponse) {
            GetJobResponse getJobResponse = (GetJobResponse) res;
            if (getJobResponse.job().isPresent()) {
                return getJobResponse.job().get();
            } else {
                throw Throwables.propagate(new JobNotFoundException(id));
            }
        } else {
            throw new TaskExecutionException(String.format(
                    "Job(id=%s) getJob received invalid response: %s",
                    id, res.status()));
        }
    }

    private int exponentialBackoffInterval(int iteration) {
        return Math.min(Math.max(config.getMinPollInterval(), (int)Math.pow(2, iteration)), config.getMaxPollInterval());
    }

    static class CommandLoggerBridge extends OutputStream {

        private static final ThreadLocal<byte[]> CACHE = ThreadLocal.withInitial(() -> new byte[1]);

        private final CommandLogger commandLogger;
        private final OutputStream out;

        CommandLoggerBridge(CommandLogger commandLogger, OutputStream out) {
            this.commandLogger = commandLogger;
            this.out = out;
        }

        @Override
        public void write(int b) throws IOException {
            byte[] bytes = CACHE.get();
            bytes[0] = (byte) b;
            write(bytes, 0, 1);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            commandLogger.copy(new ByteArrayInputStream(b, off, len), out);
        }
    }
}