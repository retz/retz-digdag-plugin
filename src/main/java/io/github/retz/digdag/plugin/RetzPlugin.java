package io.github.retz.digdag.plugin;

import io.digdag.spi.*;

import java.util.Arrays;
import java.util.List;

import javax.inject.Inject;

public class RetzPlugin implements Plugin {
    @Override
    public <T> Class<? extends T> getServiceProvider(Class<T> type) {
        if (type == OperatorProvider.class) {
            return RetzOperatorProvider.class.asSubclass(type);
        } else {
            return null;
        }
    }

    public static class RetzOperatorProvider implements OperatorProvider {

        @Inject
        CommandExecutor exec;
        @Inject
        CommandLogger cLog;

        @Override
        public List<OperatorFactory> get() {
            return Arrays.asList(
                    new RetzRunOperatorFactory(exec, cLog)
            );
        }
    }
}