package cn.ymatrix.worker;

import cn.ymatrix.builder.MxBuilder;
import cn.ymatrix.httpclient.SingletonHTTPClient;
import cn.ymatrix.logger.MxLogger;
import cn.ymatrix.utils.CSVConstructor;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;

public class TestTuplesConsumer {
    private static final Logger l = MxLogger.init(TestTuplesConsumer.class);

    @Test
    public void TestTuplesConsumerStop() {
        TuplesConsumer consumer = new TuplesConsumer(SingletonHTTPClient.getInstance(10).getClient(), new CSVConstructor(10));
        Assert.assertFalse(consumer.isStopped());
        consumer.stop();
        Assert.assertTrue(consumer.isStopped());
    }

}
