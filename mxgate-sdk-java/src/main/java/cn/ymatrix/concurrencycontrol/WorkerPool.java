package cn.ymatrix.concurrencycontrol;

public interface WorkerPool {
    /**
     * A worker thread join this thread pool.
     *
     * @param worker is a thread
     */
    void join(Runnable worker);

    void shutdown();

    void shutdownNow();

    boolean isShutdown();

    boolean isTerminated();

}
