package io.github.retz.digdag.plugin;

import com.google.common.base.Throwables;
import io.digdag.spi.CommandLogger;
import io.digdag.spi.OperatorContext;
import io.digdag.spi.TaskExecutionException;
import io.digdag.spi.TaskResult;
import io.digdag.util.BaseOperator;
import io.github.retz.cli.ClientCLIConfig;
import io.github.retz.cli.TimestampHelper;
import io.github.retz.protocol.Response;
import io.github.retz.protocol.ScheduleResponse;
import io.github.retz.protocol.data.Job;
import io.github.retz.protocol.exception.JobNotFoundException;
import io.github.retz.web.Client;
import io.github.retz.web.ClientHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class RetzRunApiOperator extends BaseOperator {

    private static final Logger LOGGER = LoggerFactory.getLogger(RetzRunApiOperator.class);

    private final CommandLogger clog;
    private final RetzOperatorConfig config;

    RetzRunApiOperator(OperatorContext context, RetzOperatorConfig config, CommandLogger clog) {
        super(context);
        this.config = config;
        this.clog = clog;
    }

    @Override
    public TaskResult runTask() {

        ClientCLIConfig fileConfig;
        try {
            fileConfig = new ClientCLIConfig(config.getClientConfig(workspace));
        } catch (IOException | URISyntaxException ex) {
            throw Throwables.propagate(ex);
        }

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
        int timeout = config.getTimeout();

        boolean verbose = config.getVerbose();

        Job job = new Job(appName, remoteCmd, envProps, cpu, mem, disk, gpu, ports);
        job.setPriority(priority);
        job.setName(name);
        job.addTags(tags);

        if (verbose) {
            LOGGER.info("Job created: {}", job);
        }

        Job scheduled;
        Job finished;
        int ecode;
        try (Client webClient = Client.newBuilder(fileConfig.getUri())
                .setAuthenticator(fileConfig.getAuthenticator())
                .checkCert(!fileConfig.insecure())
                .setVerboseLog(verbose)
                .build()) {

            Response res;
            try {
                res = webClient.schedule(job);
            } catch (IOException ex) {
                throw new RuntimeException("Failed to schedule Retz job", ex);
            }
            if (!(res instanceof ScheduleResponse)) {
                throw new RuntimeException(String.format(
                        "Failed to schedule Retz job: %s",
                        res.status()));
            }

            long start = System.currentTimeMillis();
            Callable<Boolean> timedout;
            if (timeout > 0) {
                timedout = () -> {
                    long now = System.currentTimeMillis();
                    long elapsed = now - start;
                    return elapsed > TimeUnit.MINUTES.toMillis(timeout);
                };
            } else {
                timedout = () -> false;
            }

            scheduled = ((ScheduleResponse) res).job();
            LOGGER.info("Job(id={}) scheduled: {}", scheduled.id(), scheduled.state());

            try {
                Job running = ClientHelper.waitForStart(scheduled, webClient, timedout);
                if (verbose) {
                    LOGGER.info("Job(id={}) started: {}, Timeout = {} minutes", running.id(), running.state(), timeout);
                }

                Optional<Job> maybeFinished = ClientHelper.getWholeFileWithTerminator(
                        webClient, running.id(), "stdout", true, new CommandLoggerBridge(clog, System.out), timedout);

                if (verbose) {
                    LOGGER.info("Job(id={}) finished to get stdout, will get stderr", running.id());
                }

                ClientHelper.getWholeFileWithTerminator(
                        webClient, running.id(), "stderr", false, new CommandLoggerBridge(clog, System.err), null);

                if (maybeFinished.isPresent()) {
                    finished = maybeFinished.get();
                    LOGGER.debug("{}", finished);
                    LOGGER.info("Job(id={}) finished in {} seconds. status: {}",
                            running.id(),
                            TimestampHelper.diffMillisec(
                                    finished.finished(), finished.started()) / 1000.0,
                            finished.state());
                    ecode = finished.result();
                } else {
                    throw new RuntimeException(String.format(
                            "Job(id=%s) failed to fetch last state of job",
                            running.id()));
                }
            } catch (TimeoutException ex) {
                try {
                    webClient.kill(scheduled.id());
                    throw new TaskExecutionException(String.format(
                            "Job(id=%s) has been killed due to timeout after %d minute(s)",
                            scheduled.id(), timeout));
                } catch (IOException e) {
                    throw new RuntimeException(String.format(
                            "Job(id=%s) failed to kill timedout job", scheduled.id()), ex);
                }
            } catch (IOException | ParseException | JobNotFoundException ex) {
                throw new RuntimeException(String.format(
                        "Job(id=%s) failed with unexpected error", scheduled.id()), ex);
            }
        }

        if (ecode != 0) {
            throw new TaskExecutionException(String.format(
                    "retz_run: Job(id=%s) failed. " +
                            "| retz-info: taskId=%s, state=%s, reason=%s " +
                            "| log: `digdag log %s %s`",
                    finished.id(), finished.taskId(),
                    finished.state(), finished.reason(),
                    request.getAttemptId(), request.getTaskName()));
        }

        TaskResult taskResult = TaskResult.empty(request);
        taskResult.getStoreParams()
                .getNestedOrSetEmpty(RetzOperatorConfig.KEY_CONFIG_ROOT)
                .set("last_job_id", scheduled.id());

        return taskResult;
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