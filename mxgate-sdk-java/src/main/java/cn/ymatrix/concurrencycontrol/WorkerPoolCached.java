package cn.ymatrix.concurrencycontrol;

import cn.ymatrix.exception.WorkerPoolShutdownException;
import cn.ymatrix.logger.MxLogger;
import cn.ymatrix.utils.StrUtil;
import org.slf4j.Logger;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

class WorkerPoolCached implements WorkerPool {
    private static final String TAG = StrUtil.logTagWrap(WorkerPoolCached.class.getName());
    private static final Logger l = MxLogger.init(WorkerPoolCached.class);

    static WorkerPoolCached getInstance() {
        return new WorkerPoolCached();
    }

    private final ExecutorService pool;

    private WorkerPoolCached() {
        l.info("{} Init an cached worker pool.", TAG);
        pool = Executors.newCachedThreadPool();
    }

    @Override
    public void join(Runnable worker) throws WorkerPoolShutdownException {
        if (this.pool.isShutdown()) {
            throw new WorkerPoolShutdownException("cached worker pool has been shutdown when try to join a new worker");
        }
        this.pool.submit(worker);
    }

    @Override
    public void shutdown() {
        this.pool.shutdown();
        l.info("{} cached worker pool has been shut down.", TAG);
    }

    @Override
    public void shutdownNow() {
        this.pool.shutdownNow();
        l.info("{} cached worker pool has been shut down immediately.", TAG);
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
