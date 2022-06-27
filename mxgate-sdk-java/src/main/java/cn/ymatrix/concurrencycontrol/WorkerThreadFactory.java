package cn.ymatrix.concurrencycontrol;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

class WorkerThreadFactory implements ThreadFactory {
    private static final AtomicInteger poolNumber = new AtomicInteger(1);
    private ThreadGroup group;
    private String namePrefix;
    private AtomicInteger threadNumber;

    public WorkerThreadFactory(String nameSuffix) {
        threadNumber = new AtomicInteger(1);
        SecurityManager s = System.getSecurityManager();
        group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
        namePrefix = "pool-" + poolNumber.getAndIncrement() + "-" + nameSuffix + "-";
    }

    @Override
    public Thread newThread(Runnable r) {
        Thread thread = new Thread(group, r, namePrefix + threadNumber.getAndIncrement(), 0);
        if (thread.isDaemon()) {
            // Act as a daemon.
            thread.setDaemon(true);
        }
        if (thread.getPriority() != Thread.NORM_PRIORITY) {
            thread.setPriority(Thread.NORM_PRIORITY);
        }
        return thread;
    }
}
