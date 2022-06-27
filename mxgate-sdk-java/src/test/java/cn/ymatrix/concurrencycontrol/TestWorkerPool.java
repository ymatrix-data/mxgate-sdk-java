package cn.ymatrix.concurrencycontrol;

import cn.ymatrix.logger.MxLogger;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class TestWorkerPool {

    private static final Logger l = MxLogger.init(TestWorkerPool.class);

    @Test
    public void testWorkerPool() {
        final boolean[] running = {true, true};
        WorkerPool workerPool = WorkerPoolFactory.initFixedSizeWorkerPool(100);
        Runnable r1 = new Runnable() {
            @Override
            public void run() {
                currentThreadID();
                synchronized (TestWorkerPool.class) {
                    running[0] = false;
                }
            }
        };

        Runnable r2 = new Runnable() {
            @Override
            public void run() {
                currentThreadID();
                synchronized (TestWorkerPool.class) {
                    running[1] = false;
                }
            }
        };

        currentThreadID();
        workerPool.join(r1);
        workerPool.join(r2);

        // Wait for running to be false to stop.
        while (true) {
            synchronized (TestWorkerPool.class) {
                if (running[0]) {
                    continue;
                }
                if (running[1]) {
                    continue;
                }
                break;
            }
        }

        // Shutdown the worker pool.
        workerPool.shutdown();
    }

    @Test
    public void testWorkerPool2() {
        // queue is full
        final int cnt = 5; // queue size
        final int sleepCnt = cnt-1;
        final int taskCnt = 10*cnt;

        AtomicInteger runningCnt = new AtomicInteger();
        Map<Integer, Boolean> checkFlag = new ConcurrentHashMap(taskCnt);
        Map<Integer, Boolean> needSleep = new ConcurrentHashMap(cnt);
        for (int i = 0; i < taskCnt; i ++ ){
            if (i < sleepCnt) {
                needSleep.put(i, true);
            } else {
                needSleep.put(i, false);
            }
            checkFlag.put(i, true);
            runningCnt.addAndGet(1);
        }
        WorkerPool workerPool = WorkerPoolFactory.initFixedSizeWorkerPool(cnt);

        for (int i = 0; i < taskCnt; i ++ ){
            int finalI = i;
            workerPool.join(new Runnable() {
                @Override
                public void run() {
                    currentThreadID();
                    checkFlag.put(finalI, false);

                    // make queue full
                    if (needSleep.get(finalI)) {
                        try {
                            Thread.sleep(3000);
                        } catch (InterruptedException e) {
                            l.error("sleep get interrupted: {}", e.getMessage());
                            e.printStackTrace();
                        }
                    }
                    int curV = runningCnt.addAndGet(-1);
                }
            });
        }

        currentThreadID();

        // Wait for running to be false to stop.
        while (runningCnt.get() > sleepCnt) {}
        for (int i = 0; i < taskCnt; i ++ ){
            if (checkFlag.get(i)) {
                l.error("unexpected flag, should be true, some task not execute: {}", i);
            }
        }
        int remain = runningCnt.get();
        if (remain < sleepCnt) {
            l.error("there should be {} tasks remaining sleep, but now it's {}", remain);
        }

        // Shutdown the worker pool.
        workerPool.shutdown();
    }

    @Test
    public void TestCachedWorkerPool() {
        WorkerPool pool = WorkerPoolFactory.getCachedWorkerPool();
        Assert.assertNotNull(pool);

        AtomicInteger total = new AtomicInteger(0);
        int threads = 100;
        CountDownLatch latch = new CountDownLatch(threads);
        for (int i = 0; i < threads; i++) {
            pool.join(new Runnable() {
                @Override
                public void run() {
                    total.addAndGet(1);
                    latch.countDown();
                }
            });
        }

        try {
            Assert.assertTrue(latch.await(2, TimeUnit.SECONDS));
            Assert.assertEquals(total.get(), threads);
        } catch (InterruptedException e) {
            l.error("TestCachedWorkerPool exception ", e);
            throw new RuntimeException(e);
        }

        pool.shutdown();
    }

    @Test
    public void TestCacheWorkerPoolShutDown() {
        // TODO test shutdown
        // TODO test shutdown now
    }

    private void currentThreadID() {
        System.out.println("Current thread " + Thread.currentThread());
    }

}
