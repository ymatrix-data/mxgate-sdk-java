package cn.ymatrix.worker;

import cn.ymatrix.concurrencycontrol.WorkerPool;
import cn.ymatrix.concurrencycontrol.WorkerPoolFactory;
import cn.ymatrix.exception.RetryException;
import cn.ymatrix.faulttolerance.RetryConfiguration;
import org.junit.Assert;
import org.junit.Test;

public class TestTaskWorker {
    @Test(expected = NullPointerException.class)
    public void TestTaskWorkerWorkIn() {
        TaskWorker worker = new TaskWorker();
        worker.workIn(null);
    }

    @Test
    public void TestTaskWorkerWorkInPool() {
        TaskWorker worker = new TaskWorker();
        WorkerPool pool = WorkerPoolFactory.initFixedSizeWorkerPool(100);
        worker.workIn(pool);
        Assert.assertEquals(pool, worker.workerPool);
    }

    @Test
    public void TestWithRetry() {
        RetryConfiguration configuration = new RetryConfiguration(3, 200, RetryException.class);
        TaskWorker worker = new TaskWorker();
        worker.withRetry(configuration);
        Assert.assertEquals(worker.retryConfig, configuration);
    }

    @Test
    public void TestWithRetryException() {
        TaskWorker worker = new TaskWorker();
        // No RetryConfiguration, and will get a nullable return value.
        Assert.assertNull(worker.decorateTaskWithRetry("Task with nullable RetryConfiguration", new Runnable() {
            @Override
            public void run() {

            }
        }));

        worker.withRetry(new RetryConfiguration(3, 200, RetryException.class));
        Assert.assertNotNull(worker.decorateTaskWithRetry("Task with RetryConfiguration", new Runnable() {
            @Override
            public void run() {

            }
        }));
    }

    @Test
    public void TestStop() {
        TaskWorker worker = new TaskWorker();
        Assert.assertFalse(worker.isStopped());
        Assert.assertFalse(worker.stop.get());
        worker.stop();
        Assert.assertTrue(worker.isStopped());
        Assert.assertTrue(worker.stop.get());
    }

    @Test
    public void TestTimeout() {
        TaskWorker worker = new TaskWorker();
        worker.setTimeoutMillis(300);
        Assert.assertEquals(worker.timeoutMillis, 300);
        Assert.assertEquals(worker.getTimeoutMillis(), 300);
    }

}
