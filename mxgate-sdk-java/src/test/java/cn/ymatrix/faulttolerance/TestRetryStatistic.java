package cn.ymatrix.faulttolerance;

import org.junit.Assert;
import org.junit.Test;

import java.security.InvalidParameterException;

public class TestRetryStatistic {

    @Test(expected = InvalidParameterException.class)
    public void TestInvalidParameter() {
        new RetryStatistic(-1);
    }

    @Test
    public void TestIncrease() {
        RetryStatistic rs = new RetryStatistic(3);
        Assert.assertNotNull(rs);
        Assert.assertEquals(rs.actuallyRetryTimes(), 0);
        Assert.assertFalse(rs.exceedMaxRetryTimes());

        rs.increaseRetryTimes();
        Assert.assertEquals(rs.actuallyRetryTimes(), 1);
        Assert.assertFalse(rs.exceedMaxRetryTimes());

        rs.increaseRetryTimes();
        Assert.assertEquals(rs.actuallyRetryTimes(), 2);
        Assert.assertFalse(rs.exceedMaxRetryTimes());

        rs.increaseRetryTimes();
        Assert.assertEquals(rs.actuallyRetryTimes(), 3);
        Assert.assertTrue(rs.exceedMaxRetryTimes());

        rs.increaseRetryTimes();
        Assert.assertEquals(rs.actuallyRetryTimes(), 4);
        Assert.assertTrue(rs.exceedMaxRetryTimes());
    }


}
