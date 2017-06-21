package io.github.retz.digdag.plugin;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import io.digdag.client.config.ConfigException;
import io.digdag.spi.*;
import io.digdag.util.BaseOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class RetzRunCliOperator extends BaseOperator {

    private static final Pattern VALID_ENV_KEY = Pattern.compile("[a-zA-Z_][a-zA-Z_0-9]*");

    private static final Logger LOGGER = LoggerFactory.getLogger(RetzRunCliOperator.class);

    private final RetzOperatorConfig config;
    private final CommandExecutor exec;
    private final CommandLogger clog;

    RetzRunCliOperator(OperatorContext context, RetzOperatorConfig config, CommandExecutor exec, CommandLogger clog) {
        super(context);
        this.config = config;
        this.exec = exec;
        this.clog = clog;
    }

    @Override
    public TaskResult runTask() {

        ImmutableList.Builder<String> command = ImmutableList.builder();
        config.addClientCommand(command);
        config.addClientConfig(command, workspace);
        config.addVerbose(command);
        config.addRunSubCommand(command);
        config.addAppName(command);
        config.addJobName(command);
        config.addCpu(command);
        config.addMemory(command);
        config.addDisk(command);
        config.addPorts(command);
        config.addGpu(command);
        config.addPriority(command);
        config.addStdErr(command);
        config.addTimeout(command);
        config.addEnv(command);
        config.addTags(command);
        config.addRemoteCommand(command);

        LOGGER.info("Running in retz_run: {}", command.build().stream().collect(Collectors.joining(" ")));

        ProcessBuilder pb = new ProcessBuilder(command.build());
        pb.directory(workspace.getPath().toFile());

        final Map<String, String> env = pb.environment();
        config.getRetzConfig().getKeys()
                .forEach(key -> {
                    if (isValidEnvKey(key)) {
                        try {
                            String value = config.getRetzConfig().get(key, String.class);
                            env.put(key, value);
                        } catch (ConfigException e) {
                            LOGGER.trace("Ignoring key failing to convert value to String", e);
                        }
                    } else {
                        LOGGER.trace("Ignoring invalid env var key: {}", key);
                    }
                });

        collectEnvironmentVariables(env, context.getPrivilegedVariables());

        pb.redirectErrorStream(true);

        int ecode;
        try {
            Process p = exec.start(workspace.getPath(), request, pb);
            try {
                //try (Writer writer = new BufferedWriter(new OutputStreamWriter(p.getOutputStream()))) {
                //    writer.write(taskCommand);
                //}

                clog.copyStdout(p, System.out);

                ecode = p.waitFor();
            } finally {
                p.destroy();
            }
        } catch (IOException | InterruptedException ex) {
            throw Throwables.propagate(ex);
        }

        if (ecode != 0) {
            throw new RuntimeException(String.format(
                    "retz_run: command failed with code %d " +
                            "| log: `digdag log %s %s`",
                    ecode, request.getAttemptId(), request.getTaskName()));
        }

        return TaskResult.empty(request);
    }

    private static void collectEnvironmentVariables(Map<String, String> env, PrivilegedVariables variables) {
        for (String name : variables.getKeys()) {
            if (!VALID_ENV_KEY.matcher(name).matches()) {
                throw new ConfigException("Invalid _env key name: " + name);
            }
            env.put(name, variables.get(name));
        }
    }

    private static boolean isValidEnvKey(String key) {
        return VALID_ENV_KEY.matcher(key).matches();
    }

}
