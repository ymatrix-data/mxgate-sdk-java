package cn.ymatrix.concurrencycontrol;

import java.security.InvalidParameterException;

public class WorkerPoolFactory {
    /**
     * Get a fixed size worker pool instance.
     *
     * @param workerCount how many thread can work concurrently
     * @return WorkerPool instance
     */
    public static WorkerPool initFixedSizeWorkerPool(int workerCount) throws InvalidParameterException {
        if (workerCount <= 0) {
            throw new InvalidParameterException("Create worker pool on " + workerCount + " workers.");
        }
        return WorkerPoolFixedSize.getInstance(workerCount);
    }

    /**
     * The cached worker pool is used for some short time tasks.
     *
     * @return WorkerPool instance.
     */
    public static WorkerPool getCachedWorkerPool() {
        return WorkerPoolCached.getInstance();
    }
}
