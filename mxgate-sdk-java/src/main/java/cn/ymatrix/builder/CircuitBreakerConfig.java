package cn.ymatrix.builder;

import cn.ymatrix.logger.MxLogger;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CircuitBreakerConfig {
    private static final Logger l = MxLogger.init(CircuitBreakerConfig.class);
    private boolean enable;
    // minimum number of calls which are required (per sliding window period) before the CircuitBreaker can calculate the error rate.
    private int minimumNumberOfCalls;
    // size of the sliding window which is used to record the outcome of calls when the CircuitBreaker is closed
    private int slidingWindowSize;
    // failure rate threshold in percentage
    private float failureRateThreshold;

    // the duration threshold above which calls are considered as slow and increase the slow calls percentage
    private int slowCallDurationThresholdMillis;
    // the slow-call rate threshold in percentage
    private float slowCallRateThreshold;

    private List<Class<? extends Throwable>> ignoredExceptions;

    public CircuitBreakerConfig() {
        enable = false;
    }

    public boolean isEnable() {
        return enable;
    }

    public CircuitBreakerConfig setEnable() {
        this.minimumNumberOfCalls = 10;
        this.slidingWindowSize = 100;
        this.failureRateThreshold = 50.0f;
        this.slowCallDurationThresholdMillis = 60000; // 1min
        this.slowCallRateThreshold = 100.0f;
        this.enable = true;
        return this;
    }

    public CircuitBreakerConfig setMinimumNumberOfCalls(int m) throws IllegalArgumentException {
        if (!isEnable()) {
            throw new IllegalStateException("should enable CircuitBreaker before set minimum number of calls");
        }
        if (m < 1) {
            throw new IllegalArgumentException("minimumNumberOfCalls must be greater than 0");
        }
        this.minimumNumberOfCalls = m;
        return this;
    }

    public CircuitBreakerConfig setSlidingWindowSize(int w) throws IllegalArgumentException {
        if (!isEnable()) {
            throw new IllegalStateException("should enable CircuitBreaker before set sliding window size");
        }
        if (w < 1) {
            throw new IllegalArgumentException("slidingWindowSize must be greater than 0");
        }
        this.slidingWindowSize = w;
        return this;
    }

    public CircuitBreakerConfig setFailureRateThreshold(float t) throws IllegalArgumentException {
        if (!isEnable()) {
            throw new IllegalStateException("should enable CircuitBreaker before set failure rate threshold");
        }
        if (t <= 0 || t > 100) {
            throw new IllegalArgumentException("failureRateThreshold must be between 1 and 100");
        }
        this.failureRateThreshold = t;
        return this;
    }

    public CircuitBreakerConfig setSlowCallDurationThresholdMillis(int m) throws IllegalArgumentException {
        if (!isEnable()) {
            throw new IllegalStateException("should enable CircuitBreaker before set slow call duration threshold");
        }
        if (m < 1) {
            throw new IllegalArgumentException("slowCallDurationThreshold must be at least 1ms");
        }
        this.slowCallDurationThresholdMillis = m;
        return this;
    }

    public CircuitBreakerConfig setSlowCallRateThreshold(float t) throws IllegalArgumentException {
        if (!isEnable()) {
            throw new IllegalStateException("should enable CircuitBreaker before set slow call rate threshold");
        }
        if (t <= 0 || t > 100) {
            throw new IllegalArgumentException("slowCallRateThreshold must be between 1 and 100");
        }
        this.slowCallRateThreshold = t;
        return this;
    }

    public List<Class<? extends Throwable>> getIgnoredExceptions() {
        return ignoredExceptions;
    }

    public void setIgnoredExceptions(Class<? extends Throwable>... clazz) {
        if (!isEnable()) {
            throw new IllegalStateException("should enable CircuitBreaker before set ignored exceptions class");
        }
        if (clazz == null) {
            l.warn("try to set a list of nullable ignored exceptions into Circuit Breaker Configuration.");
            return;
        }
        if (this.ignoredExceptions == null) {
            this.ignoredExceptions = new ArrayList<>();
        }
        this.ignoredExceptions.addAll(Arrays.asList(clazz));
    }

    public int getMinimumNumberOfCalls() {
        return this.minimumNumberOfCalls;
    }

    public int getSlidingWindowSize() {
        return this.slidingWindowSize;
    }

    public float getFailureRateThreshold() {
        return this.failureRateThreshold;
    }

    public int getSlowCallDurationThresholdMillis() {
        return this.slowCallDurationThresholdMillis;
    }

    public float getSlowCallRateThreshold() {
        return this.slowCallRateThreshold;
    }
}
