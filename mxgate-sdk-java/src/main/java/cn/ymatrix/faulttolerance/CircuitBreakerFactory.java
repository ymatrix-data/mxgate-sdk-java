package cn.ymatrix.faulttolerance;

import cn.ymatrix.utils.StrUtil;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;

import java.lang.reflect.Array;
import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;

public class CircuitBreakerFactory {
    private final ConcurrentHashMap<String, CircuitBreaker> breakerMap;

    public static CircuitBreakerFactory getInstance() {
        return CircuitBreakerFactory.CircuitBreakerManagerSingleton.instance;
    }

    private CircuitBreakerFactory() {
        breakerMap = new ConcurrentHashMap<>();
    }

    public CircuitBreaker prepareCircuitBreaker(String uniqueKey, String schema, String table, cn.ymatrix.builder.CircuitBreakerConfig cbConfig) {
        String key = key(uniqueKey, schema, table);
        if (this.breakerMap.get(key) != null) {
            return this.breakerMap.get(key);
        }
        if (cbConfig == null || !cbConfig.isEnable()) {
            return null;
        }
        CircuitBreakerConfig.Builder configBuilder = CircuitBreakerConfig
                .custom()
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .minimumNumberOfCalls(cbConfig.getMinimumNumberOfCalls())
                .slidingWindowSize(cbConfig.getSlidingWindowSize())
                .failureRateThreshold(cbConfig.getFailureRateThreshold())
                .slowCallDurationThreshold(Duration.ofMillis(cbConfig.getSlowCallDurationThresholdMillis()))
                .slowCallRateThreshold(cbConfig.getSlowCallRateThreshold())
                .enableAutomaticTransitionFromOpenToHalfOpen()
                .waitDurationInOpenState(Duration.ofSeconds(30));

        if (cbConfig.getIgnoredExceptions() != null && cbConfig.getIgnoredExceptions().size() > 0) {
            Class<? extends Throwable>[] classes = (Class<? extends Throwable>[])new Class[cbConfig.getIgnoredExceptions().size()];
            for (int i = 0; i < cbConfig.getIgnoredExceptions().size(); i++) {
                classes[i] = cbConfig.getIgnoredExceptions().get(i);
            }
            configBuilder.ignoreExceptions(classes);
        }
        CircuitBreakerConfig config = configBuilder.build();
        CircuitBreakerRegistry registry = CircuitBreakerRegistry.of(config);
        CircuitBreaker circuitBreaker = registry.circuitBreaker(key);
        this.breakerMap.put(key, circuitBreaker);
        return circuitBreaker;
    }

    public boolean isBreakerExist(String uniqueKey, String schema, String table) {
        if (this.breakerMap.isEmpty()) {
            return false;
        }
        return this.breakerMap.get(key(uniqueKey, schema, table)) != null;
    }

    public boolean isCircuitBreak(String uniqueKey, String schema, String table) {
        CircuitBreaker breaker = this.breakerMap.get(key(uniqueKey, schema, table));
        if (breaker == null) {
            return false;
        }
        CircuitBreaker.State curState = breaker.getState();
        return  curState == CircuitBreaker.State.OPEN || curState == CircuitBreaker.State.FORCED_OPEN;
    }

    public float getFailureRate(String uniqueKey, String schema, String table) {
        CircuitBreaker breaker = this.breakerMap.get(key(uniqueKey, schema, table));
        if (breaker == null) {
            return 0;
        }
        return breaker.getMetrics().getFailureRate();
    }

    public float getSlowCallRate(String uniqueKey, String schema, String table) {
        CircuitBreaker breaker = this.breakerMap.get(key(uniqueKey, schema, table));
        if (breaker == null) {
            return 0;
        }

        return breaker.getMetrics().getSlowCallRate();
    }

    private String key(String uniqueKey, String schema, String table) {
        return StrUtil.connect(uniqueKey, ".", schema, ".", table);
    }

    private static class CircuitBreakerManagerSingleton {
        private static final CircuitBreakerFactory instance = new CircuitBreakerFactory();
    }
}
