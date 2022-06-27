package cn.ymatrix.data;

import org.junit.Assert;
import org.junit.Test;

public class TestTuplesTarget {
    @Test
    public void TestTuplesTargetParametersSetThenGet() {
        TuplesTarget target = new TuplesTarget();
        String URL = "http://localhost:8086";
        target.setURL(URL);
        Assert.assertEquals(URL, target.getURL());
        int timeout = 3;
        target.setTimeout(timeout);
        Assert.assertEquals(timeout, target.getTimeout());
    }

}
