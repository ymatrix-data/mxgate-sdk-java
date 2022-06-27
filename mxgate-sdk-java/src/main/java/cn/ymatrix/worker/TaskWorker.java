package cn.ymatrix.worker;

import cn.ymatrix.builder.CircuitBreakerConfig;
import cn.ymatrix.concurrencycontrol.WorkerPool;
import cn.ymatrix.faulttolerance.ResilienceDecorator;
import cn.ymatrix.faulttolerance.RetryConfiguration;
import cn.ymatrix.logger.MxLogger;
import cn.ymatrix.utils.StrUtil;
import org.slf4j.Logger;

import java.util.concurrent.atomic.AtomicBoolean;

public class TaskWorker {
    private static final String TAG = StrUtil.logTagWrap(TaskWorker.class.getName());

    protected static final Logger l = MxLogger.init(TaskWorker.class);
    final AtomicBoolean stop;
    protected WorkerPool workerPool;
    protected RetryConfiguration retryConfig;
    protected CircuitBreakerConfig circuitBreakerConfig;

    protected int timeoutMillis;

    protected TaskWorker() {
        this.stop = new AtomicBoolean(false);
    }

    // Specify which worker pool this worker will work in.
    public void workIn(WorkerPool workerPool) throws NullPointerException {
        if (workerPool == null) {
            throw new NullPointerException("TaskWorker will work in an null worker pool.");
        }
        this.workerPool = workerPool;
    }

    public void withRetry(RetryConfiguration config) {
        this.retryConfig = config;
    }

    public void withCircuitBreaker(CircuitBreakerConfig config) {
        this.circuitBreakerConfig = config;
    }

    public void stop() {
        this.stop.set(true);
    }

    public boolean isStopped() {
        return this.stop.get();
    }

    protected Runnable decorateTaskWithRetry(String taskName, Runnable taskRaw) {
        try {
            // Wrap the task with retry ability.
            return ResilienceDecorator.retryWithExceptions(taskName, taskRaw, this.retryConfig);
        } catch (Exception e) {
            l.error("{} Decorate retry task exception: ", TAG, e);
            e.printStackTrace();
        }
        return null;
    }

    public int getTimeoutMillis() {
        return timeoutMillis;
    }

    public void setTimeoutMillis(int timeoutMillis) {
        this.timeoutMillis = timeoutMillis;
    }


}
