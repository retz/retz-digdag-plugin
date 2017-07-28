package io.github.retz.digdag.plugin;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.spi.TaskRequest;
import io.digdag.util.Workspace;
import io.github.retz.cli.FileConfiguration;
import io.github.retz.cli.SubCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.text.MessageFormat;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class RetzOperatorConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(RetzOperatorConfig.class);

    static final String KEY_CONFIG_ROOT = "retz";

    static final String KEY_SYSCONF_SERVER_URI = KEY_CONFIG_ROOT + ".server.uri";
    static final String KEY_SYSCONF_AUTH_ENABLED = KEY_CONFIG_ROOT + ".authentication";
    static final String KEY_SYSCONF_ACCESS_KEY = KEY_CONFIG_ROOT + ".access.key";
    static final String KEY_SYSCONF_ACCESS_SECRET = KEY_CONFIG_ROOT + ".access.secret";

    private static final String KEY_SYSCONF_MIN_POLL_INTERVAL = KEY_CONFIG_ROOT + ".min-poll-interval";
    private static final String KEY_SYSCONF_MAX_POLL_INTERVAL = KEY_CONFIG_ROOT + ".max-poll-interval";
    private static final int DEFAULT_MIN_POLL_INTERVAL = 1;
    private static final int DEFAULT_MAX_POLL_INTERVAL = 20;

    private static final String DEFAULT_CLIENT_CMD = "/opt/retz-client/bin/retz-client";
    private static final int DEFAULT_CPU = 1;
    private static final String DEFAULT_MEM = "32MB";
    private static final String DEFAULT_DISK = "32MB";
    private static final int DEFAULT_GPU = 0;
    private static final int DEFAULT_PORTS = 0;
    private static final int DEFAULT_PRIORITY = 0;
    private static final int DEFAULT_TIMEOUT = 24 * 60;

    private static final Pattern RETZ_SIZE_PATTERN = Pattern.compile("(\\d+)(\\D*)");

    private static final int RETZ_NAME_MAX = 32;

    private static final String NAME_ELLIPSIS = "..";

    private final TaskRequest taskRequest;
    private final Config retzConfig;
    private final Config systemConfig;

    RetzOperatorConfig(TaskRequest taskRequest, Config systemConfig) {
        this.taskRequest = taskRequest;
        this.retzConfig = taskRequest.getConfig().mergeDefault(
                taskRequest.getConfig().getNestedOrGetEmpty(KEY_CONFIG_ROOT));
        this.systemConfig = systemConfig;

        // TODO interim fix for error log: "Parameter 'xx' is not used at task ..."
        dumpConfig();
    }

    Config getRetzConfig() {
        return retzConfig;
    }

    String getClientMode() {
        return retzConfig.get("client_mode", String.class, "api");
    }

    Optional<String> getClientConfig(Workspace workspace) {
        Optional<String> config = retzConfig.getOptional("client_config", String.class);
        if (config.isPresent()) {
            LOGGER.warn("'client_config' is deprecated. use '{}' on digdag configuration property instead.",
                    KEY_SYSCONF_SERVER_URI);
            if (!new File(config.get()).isAbsolute()) {
                config = Optional.of(workspace.getPath(config.get()).toString());
            }
        }
        return config;
    }

    String getAppName() {
        if (!retzConfig.has("appname")) {
            throw new ConfigException("retz: 'appname' config is required");
        }
        return retzConfig.get("appname", String.class);
    }

    String getRemoteCommand() {
        return retzConfig.get("_command", String.class);
    }

    Properties getEnvProps() {
        return SubCommand.parseKeyValuePairs(
                retzConfig.getListOrEmpty("env", String.class));
    }

    int getCpu() {
        return retzConfig.get("cpu", Integer.class, DEFAULT_CPU);
    }

    int getMemory() {
        String value = retzConfig.get("mem", String.class, DEFAULT_MEM);
        return convertMB(value);
    }

    int getDisk() {
        String value = retzConfig.get("disk", String.class, DEFAULT_DISK);
        return convertMB(value);
    }

    int getGpu() {
        return retzConfig.get("gpu", Integer.class, DEFAULT_GPU);
    }

    int getPorts() {
        int ports = retzConfig.get("ports", Integer.class, DEFAULT_PORTS);
        if (ports < 0 || 1000 < ports) {
            throw new ConfigException(String.format(
                    "retz: --ports must be within 0 to 1000: %s",
                    ports));
        }
        return ports;
    }

    int getPriority() {
        return retzConfig.get("priority", Integer.class, DEFAULT_PRIORITY);
    }

    String getJobName() {
        return retzConfig.get("name", String.class, generateDefaultJobName(taskRequest));
    }

    List<String> getTags() {
        return retzConfig.getListOrEmpty("tags", String.class);
    }

    int getTimeout() {
        return retzConfig.get("timeout", Integer.class, DEFAULT_TIMEOUT);
    }

    boolean getVerbose() {
        return retzConfig.get("verbose", Boolean.class, false);
    }

    Optional<String> getServerUri() {
        return systemConfig.getOptional(KEY_SYSCONF_SERVER_URI, String.class);
    }

    Optional<String> getAuthenticationEnabled() {
        return systemConfig.getOptional(KEY_SYSCONF_AUTH_ENABLED, String.class);
    }

    Optional<String> getAccessKey() {
        return systemConfig.getOptional(KEY_SYSCONF_ACCESS_KEY, String.class);
    }

    Optional<String> getAccessSecret() {
        return systemConfig.getOptional(KEY_SYSCONF_ACCESS_SECRET, String.class);
    }

    int getMinPollInterval() {
        return systemConfig.get(KEY_SYSCONF_MIN_POLL_INTERVAL, Integer.class, DEFAULT_MIN_POLL_INTERVAL);
    }

    int getMaxPollInterval() {
        return systemConfig.get(KEY_SYSCONF_MAX_POLL_INTERVAL, Integer.class, DEFAULT_MAX_POLL_INTERVAL);
    }

    private boolean getStdErr() {
        return retzConfig.get("stderr", Boolean.class, true);
    }

    void addClientCommand(ImmutableList.Builder<String> command) {
        File clientPath = new File(retzConfig.get("client_cmd", String.class, DEFAULT_CLIENT_CMD));
        command.add(clientPath.getAbsolutePath());
    }

    void addClientConfig(ImmutableList.Builder<String> command, Workspace workspace) {
        if (getClientConfig(workspace).isPresent()) {
            command.add("-C").add(getClientConfig(workspace).get());
        }
    }

    void addVerbose(ImmutableList.Builder<String> command) {
        boolean verbose = getVerbose();
        if (verbose) {
            command.add("-v");
        }
    }

    void addRunSubCommand(ImmutableList.Builder<String> command) {
        command.add("run");
    }

    void addAppName(ImmutableList.Builder<String> command) {
        command.add("-A").add(getAppName());
    }

    void addJobName(ImmutableList.Builder<String> command) {
        command.add("-N").add(getJobName());
    }

    void addCpu(ImmutableList.Builder<String> command) {
        if (retzConfig.has("cpu")) {
            command.add("--cpu").add(String.valueOf(getCpu()));
        }
    }

    void addMemory(ImmutableList.Builder<String> command) {
        if (retzConfig.has("mem")) {
            command.add("--mem").add(String.valueOf(getMemory()));
        }
    }

    void addDisk(ImmutableList.Builder<String> command) {
        if (retzConfig.has("disk")) {
            command.add("--disk").add(String.valueOf(getDisk()));
        }
    }

    void addPorts(ImmutableList.Builder<String> command) {
        if (retzConfig.has("ports")) {
            command.add("--ports").add(String.valueOf(getPorts()));
        }
    }

    void addGpu(ImmutableList.Builder<String> command) {
        if (retzConfig.has("gpu")) {
            command.add("--gpu").add(String.valueOf(getGpu()));
        }
    }

    void addPriority(ImmutableList.Builder<String> command) {
        if (retzConfig.has("priority")) {
            command.add("--prio").add(String.valueOf(getPriority()));
        }
    }

    void addStdErr(ImmutableList.Builder<String> command) {
        boolean stderr = getStdErr();
        if (stderr) {
            command.add("--stderr");
        }
    }

    void addTimeout(ImmutableList.Builder<String> command) {
        if (retzConfig.has("timeout")) {
            command.add("--timeout").add(String.valueOf(getTimeout()));
        }
    }

    void addEnv(ImmutableList.Builder<String> command) {
        if (retzConfig.has("env")) {
            List<String> envList = retzConfig.getList("env", String.class);
            for (String env : envList) {
                command.add("-E").add(env);
            }
        }
    }

    void addTags(ImmutableList.Builder<String> command) {
        if (retzConfig.has("tags")) {
            command.add("--tags").add(String.join(",", getTags()));
        }

    }

    void addRemoteCommand(ImmutableList.Builder<String> command) {
        command.add("-c").add(getRemoteCommand());
        // command.add("-c").add("-");
        // UserSecretTemplate.of(getRemoteCommand()).format(context.getSecrets());
    }

    private String generateDefaultJobName(TaskRequest request) {
        /*
         * prefix of default job name:
         *   <attempt-id>
         *
         * default job name:
         *   (1) <prefix-head>..
         *   (2) <prefix>
         *   (3) <prefix>:..<task-name-tail>
         *   (4) <prefix>:<task-name>
         */

        String prefix = String.format("%d", request.getAttemptId());
        String taskName = request.getTaskName();

        // (1) <prefix-head>..
        if (prefix.length() > RETZ_NAME_MAX) {
            return prefix.substring(0, RETZ_NAME_MAX - NAME_ELLIPSIS.length()) + NAME_ELLIPSIS;
        }

        // (2) <prefix>
        //   we avoid non-ASCII characters for Retz database
        if (prefix.length() > RETZ_NAME_MAX - 2 - NAME_ELLIPSIS.length()
                || taskName.chars().anyMatch(it -> it > 0x7f)) {
            return prefix;
        }

        String candidate = String.format("%s:%s", prefix, taskName);

        // (3) <prefix>:..<task-name-tail>
        if (candidate.length() > RETZ_NAME_MAX) {
            String prefixPlus = String.format("%s:%s", prefix, NAME_ELLIPSIS);
            String taskNameTail = taskName.substring(
                    taskName.length() - (RETZ_NAME_MAX - prefixPlus.length()), taskName.length());
            return String.format("%s%s", prefixPlus, taskNameTail);
        }

        // (4) <prefix>:<task-name>
        return candidate;
    }

    private Integer convertMB(String value) {
        final Matcher matcher = RETZ_SIZE_PATTERN.matcher(value);
        if (!matcher.matches()) {
            throw new ConfigException(MessageFormat.format(
                    "retz: Invalid size: {0}",
                    value));
        }
        int count = Integer.parseInt(matcher.group(1));
        String unit = matcher.group(2);
        switch (unit.toLowerCase(Locale.ENGLISH)) {
            case "":
            case "m":
            case "mb":
                return count;
            case "g":
            case "gb":
                return count * 1024;
            case "t":
            case "tb":
                return count * 1024 * 1024;
            default:
                throw new ConfigException(MessageFormat.format(
                        "retz: Invalid size (unsupported size unit): {0}",
                        value));
        }
    }

    private void dumpConfig() {
        LOGGER.debug(
                "client_mode:{}, client_config:{}, appname:{}, env:{}, cpu:{}, mem:{}, disk:{}, gpu:{}, ports:{}, priority:{}, name:{}, tags:{}, timeout:{}, verbose:{}, stderr:{}",
                retzConfig.getOptional("client_mode", String.class),
                retzConfig.getOptional("client_config", String.class),
                retzConfig.getOptional("appname", String.class),
                retzConfig.getListOrEmpty("env", String.class),
                retzConfig.getOptional("cpu", Integer.class),
                retzConfig.getOptional("mem", String.class),
                retzConfig.getOptional("disk", String.class),
                retzConfig.getOptional("gpu", Integer.class),
                retzConfig.getOptional("ports", Integer.class),
                retzConfig.getOptional("priority", Integer.class),
                retzConfig.getOptional("name", String.class),
                retzConfig.getListOrEmpty("tags", String.class),
                retzConfig.getOptional("timeout", Integer.class),
                retzConfig.getOptional("verbose", Boolean.class),
                retzConfig.getOptional("stderr", Boolean.class)
        );
    }

    static class ClientConfig extends FileConfiguration {

        ClientConfig(Properties p) {
            super(p);
        }
    }

}
