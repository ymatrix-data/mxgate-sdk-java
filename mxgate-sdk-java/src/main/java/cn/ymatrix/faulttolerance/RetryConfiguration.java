package cn.ymatrix.faulttolerance;

import java.security.InvalidParameterException;

public class RetryConfiguration {

    private final int maxAttempts;

    private final int waitDurationMillis;

    private final Class<? extends Throwable>[] errorClassList;

    public RetryConfiguration(int maxAttempts, int waitDurationMillis, Class<? extends Throwable>... errorClasses)
            throws NullPointerException, InvalidParameterException {
        if (maxAttempts <= 0) {
            throw new InvalidParameterException("MaxAttempts parameter is negative");
        }

        if (waitDurationMillis <= 0) {
            throw new InvalidParameterException("WaitDuration parameter is negative");
        }

        if (errorClasses == null) {
            throw new NullPointerException("Exception classes is null");
        }

        if (errorClasses.length == 0) {
            throw new InvalidParameterException("Exception classes list is empty");
        }

        errorClassList = errorClasses;
        this.maxAttempts = maxAttempts;
        this.waitDurationMillis = waitDurationMillis;
    }

    public int getMaxAttempts() {
        return maxAttempts;
    }

    public int getWaitDurationMillis() {
        return waitDurationMillis;
    }

    public Class<? extends Throwable>[] getErrorClassList() {
        return errorClassList;
    }
}
