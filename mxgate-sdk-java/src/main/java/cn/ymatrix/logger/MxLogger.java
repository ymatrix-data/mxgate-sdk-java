package cn.ymatrix.logger;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.ConsoleAppender;
import org.apache.logging.log4j.core.appender.FileAppender;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.config.builder.api.*;
import org.apache.logging.log4j.core.config.builder.impl.BuiltConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.InvalidParameterException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class MxLogger {
    private static Level LOG_LEVEL = Level.INFO;
    private static String LOG_FILE_NAME = "/tmp/mxgate_sdk_java_" + timestampNow() + ".log";
    private static boolean TO_STDOUT = true;
    private static String timestampNow() {
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd_HHmmss");
        return dtf.format(LocalDateTime.now());
    }

    public static void writeToFile(String logFileName) {
        LOG_FILE_NAME = logFileName;
    }

    public static void enableStdout(boolean toStdout) {
        TO_STDOUT = toStdout;
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

        if (TO_STDOUT) {
            AppenderComponentBuilder appenderStdoutBuilder = configBuilder.newAppender("Stdout", "CONSOLE").addAttribute("target", ConsoleAppender.Target.SYSTEM_OUT);
            appenderStdoutBuilder.add(configBuilder.newLayout("PatternLayout").addAttribute("pattern", "%d [%t] %-5level: %msg%n%throwable"));
            appenderStdoutBuilder.add(configBuilder.newFilter("MarkerFilter", Filter.Result.DENY, Filter.Result.NEUTRAL).addAttribute("marker", "FLOW"));
            configBuilder.add(appenderStdoutBuilder);
            loggerBuilder.add(configBuilder.newAppenderRef("Stdout"));
            rootBuilder.add(configBuilder.newAppenderRef("Stdout"));
        }

        if (LOG_FILE_NAME != null && ! LOG_FILE_NAME.isEmpty()) {
            AppenderComponentBuilder appenderFileBuilder = configBuilder.newAppender("LOGFILE", FileAppender.PLUGIN_NAME).addAttribute("fileName", LOG_FILE_NAME);
            appenderFileBuilder.add(configBuilder.newLayout("PatternLayout").addAttribute("pattern", "%d [%t] %-5level: %msg%n%throwable"));
            appenderFileBuilder.add(configBuilder.newFilter("MarkerFilter", Filter.Result.DENY, Filter.Result.NEUTRAL).addAttribute("marker", "FLOW"));
            configBuilder.add(appenderFileBuilder);
            loggerBuilder.add(configBuilder.newAppenderRef("LOGFILE"));
            rootBuilder.add(configBuilder.newAppenderRef("LOGFILE"));
        }

        configBuilder.add(loggerBuilder);
        configBuilder.add(rootBuilder);

        LoggerContext ctx = Configurator.initialize(configBuilder.build());
        ctx.getLogger(MxLogger.class);
    }


}
