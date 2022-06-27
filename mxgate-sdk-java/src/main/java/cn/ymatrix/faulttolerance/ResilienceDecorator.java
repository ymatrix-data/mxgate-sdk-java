package cn.ymatrix.faulttolerance;

import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;

import java.security.InvalidParameterException;
import java.time.Duration;

/**
 * This factory cat give the Runnable or Function the resilience abilities:
 * 1. Retry.
 * 2. Circuit Breaker.
 */
public class ResilienceDecorator {
    /**
     * RetryConfig Sample
     * RetryConfig config = RetryConfig.custom()
     * .maxAttempts(2)
     * .waitDuration(Duration.ofMillis(1000))
     * .retryOnResult(response -> response.getStatus() == 500)
     * .retryOnException(e -> e instanceof WebServiceException)
     * .retryExceptions(IOException.class, TimeoutException.class)
     * .ignoreExceptions(BusinessException.class, OtherBusinessException.class)
     * .failAfterMaxAttempts(true)
     * .build();
     */
    public static Runnable retryWithExceptions(String name, Runnable task, RetryConfiguration configuration) throws NullPointerException, InvalidParameterException {
        if (configuration == null) {
            throw new NullPointerException("Retry on a null runnable task");
        }

        if (task == null) {
            throw new NullPointerException("Retry on a null runnable task");
        }

        if (configuration.getMaxAttempts() <= 0) {
            throw new InvalidParameterException("MaxAttempts parameter is negative");
        }

        if (configuration.getWaitDurationMillis() <= 0) {
            throw new InvalidParameterException("WaitDuration parameter is negative");
        }

        if (configuration.getErrorClassList() == null) {
            throw new NullPointerException("Exception classes is null");
        }

        if (configuration.getErrorClassList().length == 0) {
            throw new InvalidParameterException("Exception classes list is empty");
        }

        RetryConfig config = RetryConfig.custom()
                .maxAttempts(configuration.getMaxAttempts())
                .waitDuration(Duration.ofMillis(configuration.getWaitDurationMillis()))
                .retryExceptions(configuration.getErrorClassList())
                .failAfterMaxAttempts(true)
                .build();
        RetryRegistry registry = RetryRegistry.of(config);
        Retry retry = registry.retry(name);
        return Retry.decorateRunnable(retry, task);
    }


}
