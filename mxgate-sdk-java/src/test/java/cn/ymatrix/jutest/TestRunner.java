package cn.ymatrix.jutest;

import cn.ymatrix.api.TestGrpcClient;
import cn.ymatrix.api.TestJobMetadataWrapper;
import cn.ymatrix.apiserver.TestMxServer;
import cn.ymatrix.builder.*;
import cn.ymatrix.cache.TestCache;
import cn.ymatrix.concurrencycontrol.TestWorkerPool;
import cn.ymatrix.data.TestColumn;
import cn.ymatrix.logger.MxLogger;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;
import org.slf4j.Logger;

// this is of no use to "mvn test"
public class TestRunner {
    static Logger l = MxLogger.init(TestRunner.class);
    static final String FAILURE_TAG = "[FAILURE_TEST]: ";
    static final String TEST_RESULT = "[TEST_RESULT]: ";

    public static void main(String[] args) {
        Result result = JUnitCore.runClasses(TestWorkerPool.class,
                TestColumn.class,
                TestBuilder.class,
                TestMxClient.class,
                TestCache.class,
                TestMxServer.class,
                TestGrpcClient.class,
                TestTupleImpl.class,
                TestJobMetadataWrapper.class,
                TestScheduledFlushTask.class,
                TestResultMessageRegister.class,
                TestTuplesImpl.class);
        for (Failure failure : result.getFailures()) {
            l.info(FAILURE_TAG + failure);
        }
        l.info(TEST_RESULT + result.wasSuccessful());
    }

}
