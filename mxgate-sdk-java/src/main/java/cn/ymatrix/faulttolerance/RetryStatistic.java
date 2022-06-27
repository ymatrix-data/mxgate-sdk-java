package cn.ymatrix.faulttolerance;

import java.security.InvalidParameterException;
import java.util.concurrent.atomic.AtomicInteger;

public class RetryStatistic {
    private final int maxRetryTimes;

    private final AtomicInteger retryTimesCounter;

    public RetryStatistic(int maxRetryTimes) throws InvalidParameterException {
        if (maxRetryTimes < 0) {
            throw new InvalidParameterException("maxRetryTimes must >= 0");
        }
        this.maxRetryTimes = maxRetryTimes;
        retryTimesCounter = new AtomicInteger(0);
    }

    /**
     * Increase retry time atomic and compare with the maxRetryTimes.
     *
     * @return True for reach the maxRetryTimes, False for not.
     */
    public boolean increaseRetryTimes() {
        return retryTimesCounter.incrementAndGet() >= this.maxRetryTimes;
    }

    public boolean exceedMaxRetryTimes() {
        return retryTimesCounter.get() >= this.maxRetryTimes;
    }

    public int actuallyRetryTimes() {
        return retryTimesCounter.get();
    }

}
