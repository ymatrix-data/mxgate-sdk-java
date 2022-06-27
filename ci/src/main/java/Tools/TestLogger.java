package Tools;

import cn.ymatrix.logger.LoggerLevel;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.ConsoleAppender;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.config.builder.api.*;
import org.apache.logging.log4j.core.config.builder.impl.BuiltConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.InvalidParameterException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class TestLogger {
    private static Level LOG_LEVEL = Level.INFO;

    private static String timestampNow() {
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd_HHmmss");
        return dtf.format(LocalDateTime.now());
    }

    public static void loggerLevel(LoggerLevel level) throws InvalidParameterException {
        switch (level) {
            case DEBUG:
                LOG_LEVEL = Level.DEBUG;
                break;
            case INFO:
                LOG_LEVEL = Level.INFO;
                break;
            case WARN:
                LOG_LEVEL = Level.WARN;
                break;
            case ERROR:
                LOG_LEVEL = Level.ERROR;
                break;
            case FATAL:
                LOG_LEVEL = Level.FATAL;
                break;
            default:
                throw new InvalidParameterException("invalid logger level configuration " + level);
        }
    }

    public static Logger init(Class<?> clz) {
        initLogger();
        return LoggerFactory.getLogger(clz);
    }

    private static void initLogger() {
        ConfigurationBuilder<BuiltConfiguration> configBuilder = ConfigurationBuilderFactory.newConfigurationBuilder();
        configBuilder.setStatusLevel(LOG_LEVEL);
        configBuilder.add(configBuilder.newFilter("ThresholdFilter", Filter.Result.ACCEPT, Filter.Result.NEUTRAL).addAttribute("level", LOG_LEVEL));

        RootLoggerComponentBuilder rootBuilder = configBuilder.newRootLogger(LOG_LEVEL);
        LoggerComponentBuilder loggerBuilder = configBuilder.newLogger("org.apache.logging.log4j", LOG_LEVEL).addAttribute("additivity", false);

        // to std out
        AppenderComponentBuilder appenderStdoutBuilder = configBuilder.newAppender("Stdout", "CONSOLE").addAttribute("target", ConsoleAppender.Target.SYSTEM_OUT);
        appenderStdoutBuilder.add(configBuilder.newLayout("PatternLayout").addAttribute("pattern", "%d [%t] %-5level: %msg%n%throwable"));
        appenderStdoutBuilder.add(configBuilder.newFilter("MarkerFilter", Filter.Result.DENY, Filter.Result.NEUTRAL).addAttribute("marker", "FLOW"));
        configBuilder.add(appenderStdoutBuilder);
        loggerBuilder.add(configBuilder.newAppenderRef("Stdout"));
        rootBuilder.add(configBuilder.newAppenderRef("Stdout"));

        configBuilder.add(loggerBuilder);
        configBuilder.add(rootBuilder);

        LoggerContext ctx = Configurator.initialize(configBuilder.build());
        ctx.getLogger(TestLogger.class);
    }
}
