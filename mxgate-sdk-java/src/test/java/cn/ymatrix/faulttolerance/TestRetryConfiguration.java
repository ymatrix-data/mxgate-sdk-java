package cn.ymatrix.faulttolerance;

import cn.ymatrix.exception.RetryException;
import org.junit.Assert;
import org.junit.Test;

import java.security.InvalidParameterException;

public class TestRetryConfiguration {
    @Test(expected = InvalidParameterException.class)
    public void ConstructionWithException() {
        RetryConfiguration rc = new RetryConfiguration(-1, 3000, RetryException.class);
    }

    @Test(expected = InvalidParameterException.class)
    public void ConstructionWithException2() {
        RetryConfiguration rc = new RetryConfiguration(3, -100, RetryException.class);
    }

    @Test(expected = NullPointerException.class)
    public void ConstructionWithException3() {
        RetryConfiguration rc = new RetryConfiguration(3, 1000, null);
    }

    @Test(expected = InvalidParameterException.class)
    public void ConstructionWithException4() {
        Class<? extends Throwable>[] classes = new Class[0];
        RetryConfiguration rc = new RetryConfiguration(3, 1000, classes);
    }

    @Test
    public void get() {
        RetryConfiguration rc = new RetryConfiguration(3, 1000, RetryException.class, NullPointerException.class);
        Class<? extends Throwable>[] classes = rc.getErrorClassList();
        Assert.assertNotNull(classes);
        Assert.assertEquals(classes.length, 2);
        Assert.assertEquals(rc.getMaxAttempts(), 3);
        Assert.assertEquals(rc.getWaitDurationMillis(), 1000);
    }

}
