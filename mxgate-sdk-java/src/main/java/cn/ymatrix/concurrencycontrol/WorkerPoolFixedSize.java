package cn.ymatrix.concurrencycontrol;

import cn.ymatrix.exception.WorkerPoolShutdownException;
import cn.ymatrix.logger.MxLogger;
import cn.ymatrix.utils.StrUtil;
import org.slf4j.Logger;

import java.security.InvalidParameterException;
import java.util.concurrent.*;

/**
 * Fixed size worker pool which is implemented as a singleton.
 */
class WorkerPoolFixedSize implements WorkerPool {

    private static final String TAG = StrUtil.logTagWrap(WorkerPoolFixedSize.class.getName());
    private static final Logger l = MxLogger.init(WorkerPoolFixedSize.class);
    private final ExecutorService pool;

    private final int workerPoolSize;

    static WorkerPoolFixedSize getInstance(int workerCount) throws InvalidParameterException {
        return new WorkerPoolFixedSize(workerCount);
    }

    private WorkerPoolFixedSize(int threadCount) throws InvalidParameterException {
        if (threadCount <= 0) {
            throw new InvalidParameterException("invalid fixed worker pool size: " + threadCount);
        }
        l.info("{} Init fixed size worker pool with threads size: {}", TAG, threadCount);

        BlockingQueue workingQueue = new ArrayBlockingQueue(threadCount);
        RejectedExecutionHandler rejectedExecutionHandler = new RejectedExecutionHandler() {
            @Override
            public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                // block if the queue is full
                try {
                    executor.getQueue().put(r);
                } catch(InterruptedException e) {
                    throw new RejectedExecutionException("unexpected interruption happened while worker waiting enqueue");
                }

                // check afterwards and log error if pool shutdown
                if (executor.isShutdown()) {
                    throw new RejectedExecutionException("fixed size worker pool has been shutdown while worker waiting enqueue");
                }
            }
        };

        pool = new ThreadPoolExecutor(threadCount, threadCount, 0L, TimeUnit.MILLISECONDS, workingQueue,
                new WorkerThreadFactory("thread-of-fixed-size-thread-pool"), rejectedExecutionHandler);
        this.workerPoolSize = threadCount;
    }

    @Override
    public void join(Runnable worker) throws WorkerPoolShutdownException {
        if (this.pool.isShutdown()) {
            throw new WorkerPoolShutdownException("fixed size worker pool has been shutdown when try to join a new worker");
        }
        this.pool.submit(worker);
    }

    @Override
    public void shutdown() {
        this.pool.shutdown();
        l.info("{} fixed size ({}) worker pool has been shut down", TAG, this.workerPoolSize);
    }

    @Override
    public void shutdownNow() {
        this.pool.shutdownNow();
        l.info("{} fixed size ({}) worker pool has been shut down immediately", TAG, this.workerPoolSize);
    }

    @Override
    public boolean isShutdown() {
        return this.pool.isShutdown();
    }

    @Override
    public boolean isTerminated() {
        return this.pool.isTerminated();
    }

}
