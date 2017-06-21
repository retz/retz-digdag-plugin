package io.github.retz.digdag.plugin;

import java.text.MessageFormat;

import com.google.inject.Inject;
import io.digdag.client.config.ConfigException;
import io.digdag.spi.*;

public class RetzRunOperatorFactory implements OperatorFactory {

    private final CommandExecutor exec;
    private final CommandLogger clog;

    @Inject
    RetzRunOperatorFactory(CommandExecutor exec, CommandLogger clog) {
        this.exec = exec;
        this.clog = clog;
    }

    @Override
    public String getType() {
        return "retz_run";
    }

    @Override
    public Operator newOperator(OperatorContext context) {
        RetzOperatorConfig config = new RetzOperatorConfig(context.getTaskRequest());
        String clientMode = config.getClientMode();
        switch (clientMode) {
            case "api":
                return new RetzRunApiOperator(context, config, clog);
            case "cli":
                return new RetzRunCliOperator(context, config, exec, clog);
            default:
                throw new ConfigException(MessageFormat.format(
                        "retz: invalid client_mode: {0}",
                        clientMode));
        }
    }

}
