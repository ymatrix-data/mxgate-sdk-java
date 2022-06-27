package cn.ymatrix.faulttolerance;

import cn.ymatrix.builder.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import org.junit.Assert;
import org.junit.Test;
import java.util.concurrent.TimeUnit;

public class TestCircuitBreakerFactory {
    @Test
    public void testPrepareCircuitBreakerNull() {
        Assert.assertNull(CircuitBreakerFactory.getInstance().prepareCircuitBreaker("localhost:8086","public", "test1", null));
    }

    @Test
    public void testPrepareCircuitBreakerNotEnable() {
        CircuitBreakerConfig config = new CircuitBreakerConfig();
        Assert.assertNull(CircuitBreakerFactory.getInstance().prepareCircuitBreaker("localhost:8086", "public", "test2", config));
    }

    @Test
    public void testPrepareCircuitBreakerEnable() {
        String targetURL = "localhost:9200";
        String schema = "public";
        String table = "test3";
        Assert.assertFalse(CircuitBreakerFactory.getInstance().isBreakerExist(targetURL, schema, table));
        Assert.assertFalse(CircuitBreakerFactory.getInstance().isCircuitBreak(targetURL, schema, table));

        CircuitBreakerConfig config = new CircuitBreakerConfig();
        config.setEnable();
        CircuitBreaker breaker = CircuitBreakerFactory.getInstance().prepareCircuitBreaker(targetURL, schema, table, config);
        Assert.assertNotNull(breaker);
        Assert.assertTrue(CircuitBreakerFactory.getInstance().isBreakerExist(targetURL, schema, table));
    }

    @Test
    public void testIsCircuitBreakFailureRate() {
        String targetURL = "localhost:9201";
        String schema = "public";
        String table = "test4";
        Assert.assertFalse(CircuitBreakerFactory.getInstance().isBreakerExist(targetURL, schema, table));
        Assert.assertFalse(CircuitBreakerFactory.getInstance().isCircuitBreak(targetURL, schema, table));

        CircuitBreakerConfig config = new CircuitBreakerConfig();
        config.setEnable();
        config.setMinimumNumberOfCalls(1);
        config.setSlidingWindowSize(10);
        config.setFailureRateThreshold(50.0f);

        CircuitBreaker breaker = CircuitBreakerFactory.getInstance().prepareCircuitBreaker(targetURL, schema, table, config);
        Assert.assertNotNull(breaker);
        Assert.assertTrue(CircuitBreakerFactory.getInstance().isBreakerExist(targetURL, schema, table));

        breaker.onSuccess(1, TimeUnit.MILLISECONDS);
        Assert.assertFalse(CircuitBreakerFactory.getInstance().isCircuitBreak(targetURL, schema, table));
        breaker.onSuccess(1, TimeUnit.MILLISECONDS);
        breaker.onError(1, TimeUnit.MILLISECONDS, new RuntimeException("test"));
        Assert.assertFalse(CircuitBreakerFactory.getInstance().isCircuitBreak(targetURL, schema, table));
        breaker.onSuccess(1, TimeUnit.MILLISECONDS);
        breaker.onSuccess(1, TimeUnit.MILLISECONDS);
        breaker.onError(1, TimeUnit.MILLISECONDS, new RuntimeException("test"));
        Assert.assertFalse(CircuitBreakerFactory.getInstance().isCircuitBreak(targetURL, schema, table));
        breaker.onError(1, TimeUnit.MILLISECONDS, new RuntimeException("test"));
        breaker.onError(1, TimeUnit.MILLISECONDS, new RuntimeException("test"));
        Assert.assertTrue(CircuitBreakerFactory.getInstance().isCircuitBreak(targetURL, schema, table));
        Assert.assertEquals(0, Float.compare(50.0f, CircuitBreakerFactory.getInstance().getFailureRate(targetURL, schema, table)));
    }

    @Test
    public void testIsCircuitBreakSlowCallRate() {
        String targetURL = "localhost:9202";
        String schema = "public";
        String table = "test5";
        Assert.assertFalse(CircuitBreakerFactory.getInstance().isBreakerExist(targetURL, schema, table));
        Assert.assertFalse(CircuitBreakerFactory.getInstance().isCircuitBreak(targetURL, schema, table));

        int slowDuration = 5;
        CircuitBreakerConfig config = new CircuitBreakerConfig();
        config.setEnable();
        config.setMinimumNumberOfCalls(3);
        config.setSlidingWindowSize(10);
        config.setFailureRateThreshold(100.0f);
        config.setSlowCallDurationThresholdMillis(slowDuration);
        config.setSlowCallRateThreshold(70.0f);

        CircuitBreaker breaker = CircuitBreakerFactory.getInstance().prepareCircuitBreaker(targetURL, schema, table, config);
        Assert.assertNotNull(breaker);
        Assert.assertTrue(CircuitBreakerFactory.getInstance().isBreakerExist(targetURL, schema, table));

        breaker.onSuccess(2*slowDuration, TimeUnit.MILLISECONDS);
        breaker.onSuccess(2*slowDuration, TimeUnit.MILLISECONDS);
        Assert.assertFalse(CircuitBreakerFactory.getInstance().isCircuitBreak(targetURL, schema, table));
        breaker.onSuccess(slowDuration-1, TimeUnit.MILLISECONDS);
        breaker.onError(slowDuration-1, TimeUnit.MILLISECONDS, new RuntimeException("test"));
        Assert.assertEquals(0, Float.compare(50.0f, CircuitBreakerFactory.getInstance().getSlowCallRate(targetURL, schema, table)));
        Assert.assertFalse(CircuitBreakerFactory.getInstance().isCircuitBreak(targetURL, schema, table));
        breaker.onSuccess(2*slowDuration, TimeUnit.MILLISECONDS);
        breaker.onSuccess(slowDuration-1, TimeUnit.MILLISECONDS);
        breaker.onError(2*slowDuration, TimeUnit.MILLISECONDS, new RuntimeException("test"));
        Assert.assertFalse(CircuitBreakerFactory.getInstance().isCircuitBreak(targetURL, schema, table));
        breaker.onSuccess(2*slowDuration, TimeUnit.MILLISECONDS);
        breaker.onSuccess(2*slowDuration, TimeUnit.MILLISECONDS);
        breaker.onError(2*slowDuration, TimeUnit.MILLISECONDS, new RuntimeException("test"));
        Assert.assertTrue(CircuitBreakerFactory.getInstance().isCircuitBreak(targetURL, schema, table));
    }
}
