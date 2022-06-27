package cn.ymatrix.builder;

import org.junit.Assert;
import org.junit.Test;

public class TestCircuitBreakerConfig {
    @Test
    public void testNewCircuitBreakerConfigInstance() {
        CircuitBreakerConfig config = new CircuitBreakerConfig();
        Assert.assertFalse(config.isEnable());
    }

    @Test(expected = IllegalStateException.class)
    public void testSetMinimumNumberOfCallsWithoutEnable() {
        CircuitBreakerConfig config = new CircuitBreakerConfig();
        config.setMinimumNumberOfCalls(1);
    }

    @Test(expected = IllegalStateException.class)
    public void testSetSlidingWindowSizeOfCallsWithoutEnable() {
        CircuitBreakerConfig config = new CircuitBreakerConfig();
        config.setSlidingWindowSize(1);
    }

    @Test(expected = IllegalStateException.class)
    public void testSetFailureRateThresholdWithoutEnable() {
        CircuitBreakerConfig config = new CircuitBreakerConfig();
        config.setFailureRateThreshold(70.0f);
    }

    @Test(expected = IllegalStateException.class)
    public void testSetSlowCallDurationThresholdMillisWithoutEnable() {
        CircuitBreakerConfig config = new CircuitBreakerConfig();
        config.setSlowCallDurationThresholdMillis(1);
    }

    @Test(expected = IllegalStateException.class)
    public void testSetSlowCallRateThresholdWithoutEnable() {
        CircuitBreakerConfig config = new CircuitBreakerConfig();
        config.setSlowCallRateThreshold(50.0f);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetMinimumNumberOfCallsInvalid() {
        CircuitBreakerConfig config = new CircuitBreakerConfig();
        config.setEnable();
        config.setMinimumNumberOfCalls(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetSlidingWindowSizeInvalid() {
        CircuitBreakerConfig config = new CircuitBreakerConfig();
        config.setEnable();
        config.setSlidingWindowSize(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetFailureRateThresholdInvalid1() {
        CircuitBreakerConfig config = new CircuitBreakerConfig();
        config.setEnable();
        config.setFailureRateThreshold(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetFailureRateThresholdInvalid2() {
        CircuitBreakerConfig config = new CircuitBreakerConfig();
        config.setEnable();
        config.setFailureRateThreshold(200);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetSlowCallDurationThresholdMillisInvalid() {
        CircuitBreakerConfig config = new CircuitBreakerConfig();
        config.setEnable();
        config.setSlowCallDurationThresholdMillis(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetSlowCallRateThresholdInvalid1() {
        CircuitBreakerConfig config = new CircuitBreakerConfig();
        config.setEnable();
        config.setSlowCallRateThreshold(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetSlowCallRateThresholdInvalid2() {
        CircuitBreakerConfig config = new CircuitBreakerConfig();
        config.setEnable();
        config.setSlowCallRateThreshold(101);
    }

    @Test
    public void testNormal() {
        int m = 3;
        int w = 100;
        float r1 = 60.0f;
        int d = 1000;
        float r2 = 80.0f;
        CircuitBreakerConfig config = new CircuitBreakerConfig()
                .setEnable()
                .setMinimumNumberOfCalls(m)
                .setSlidingWindowSize(w)
                .setFailureRateThreshold(r1)
                .setSlowCallDurationThresholdMillis(d)
                .setSlowCallRateThreshold(r2);

        Assert.assertTrue(config.isEnable());
        Assert.assertEquals(m, config.getMinimumNumberOfCalls());
        Assert.assertEquals(w, config.getSlidingWindowSize());
        Assert.assertEquals(0, Float.compare(r1, config.getFailureRateThreshold()));
        Assert.assertEquals(d, config.getSlowCallDurationThresholdMillis());
        Assert.assertEquals(0, Float.compare(r2, config.getSlowCallRateThreshold()));
    }
}
