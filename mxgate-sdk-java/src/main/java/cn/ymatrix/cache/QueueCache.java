package cn.ymatrix.cache;

import cn.ymatrix.data.Tuples;
import cn.ymatrix.exception.EnqueueException;
import cn.ymatrix.logger.MxLogger;
import cn.ymatrix.utils.StrUtil;
import org.slf4j.Logger;

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

class QueueCache implements Cache {
    private static final String TAG = StrUtil.logTagWrap(QueueCache.class.getName());
    private static final Logger l = MxLogger.init(QueueCache.class);
    private final BlockingDeque<Tuples> queue;
    private final long waitTimeoutMS; // timeout for waiting enqueue when queue is full
    private final AtomicBoolean refuse;

    /**
     * Generate a blocking queue with limited capacity and timeout for waiting enqueue.
     */
    static QueueCache getInstance(int capacity, long waitTimeoutMS) {
        return new QueueCache(capacity, waitTimeoutMS);
    }

    static QueueCache getInstance() {
        final int DEFAULT_CACHE_CAPACITY = 1000;
        final int DEFAULT_CACHE_TIMEOUT = 1000;
        return getInstance(DEFAULT_CACHE_CAPACITY, DEFAULT_CACHE_TIMEOUT);
    }

    /**
     * Need limit to the queue size to prevent memory increasing.
     */
    private QueueCache(int capacity, long waitTimeoutMS) {
        l.info("{} Init queue cache instance with capacity {} and wait timeout {}", TAG, capacity, waitTimeoutMS);
        this.waitTimeoutMS = waitTimeoutMS;
        this.queue = new LinkedBlockingDeque<>(capacity);
        this.refuse = new AtomicBoolean(false);
    }

    @Override
    public boolean offer (Tuples tuples) throws EnqueueException {
        if (this.isRefused()) {
            throw new EnqueueException("QueueCache has been set to refused of any new Tuples to enqueue.");
        }
        try {
            boolean added = this.queue.offer(tuples, this.waitTimeoutMS, TimeUnit.MILLISECONDS);
            return added;
        } catch (InterruptedException e) {
            l.error("Queue cache encountered unexpected interruption: {}", e.getMessage());
        }
        return false;
    }

    @Override
    public Tuples get() throws InterruptedException {
        return this.queue.take();
    }

    @Override
    public void clear() {
        this.queue.clear();
    }

    @Override
    public int size() {
        return this.queue.size();
    }

    @Override
    public boolean isEmpty() {
        return this.queue.isEmpty();
    }

    @Override
    public void setRefuse(boolean refuse) {
        this.refuse.set(refuse);
    }

    @Override
    public boolean isRefused() {
        return this.refuse.get();
    }
}
