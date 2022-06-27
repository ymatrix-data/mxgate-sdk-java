package cn.ymatrix.messagecenter;

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;

public class ResultMessageQueue<R> implements MessageQueue<R> {
    private final BlockingDeque<R> queue;

    public ResultMessageQueue() {
        this.queue = new LinkedBlockingDeque<>();
    }

    @Override
    public R get() throws InterruptedException {
        return this.queue.take();
    }

    @Override
    public void add(R result) {
        this.queue.add(result);
    }

    @Override
    public void clear() {
        this.queue.clear();
    }

    @Override
    public boolean isEmpty() {
        return this.queue.isEmpty();
    }
}
