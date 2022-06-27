package cn.ymatrix.builder;

import java.security.InvalidParameterException;

/**
 * The configuration of MxServer.
 */
public class ServerConfig implements Cloneable {
    private int concurrency;

    private int maxRetryAttempts;

    private int waitRetryDurationMillis;

    private int timeoutMillis;

    private CircuitBreakerConfig circuitBreakerConfig;

    private RequestType requestType;

    private boolean dropAll;

    private boolean withAsyncRequest;

    private int csvConstructionParallel;

    public int getCSVConstructionParallel() {
        return csvConstructionParallel;
    }

    public void setCSVConstructionParallel(int csvConstructionPoolSize) throws InvalidParameterException {
        if (csvConstructionPoolSize <= 0) {
            throw new InvalidParameterException("CSV construction parallel is invalid(must be positive) " + csvConstructionPoolSize);
        }
        this.csvConstructionParallel = csvConstructionPoolSize;
    }

    public ServerConfig() {
        requestType = RequestType.WithGRPC; // default
    }

    public int getConcurrency() {
        return concurrency;
    }

    public boolean isDropAll() {
        return dropAll;
    }

    public void setDropAll(boolean dropAll) {
        this.dropAll = dropAll;
    }

    public RequestType getRequestType() {
        return requestType;
    }

    public void setRequestType(RequestType requestType) {
        this.requestType = requestType;
    }

    /**
     * How many thread send data to backend server concurrently.
     * @param concurrency thread size
     */
    public void setConcurrency(int concurrency) {
        this.concurrency = concurrency;
    }

    public int getMaxRetryAttempts() {
        return maxRetryAttempts;
    }

    public void setMaxRetryAttempts(int maxRetryAttempts) {
        this.maxRetryAttempts = maxRetryAttempts;
    }

    public int getWaitRetryDurationMillis() {
        return waitRetryDurationMillis;
    }

    public void setWaitRetryDurationMillis(int waitRetryDurationMillis) {
        this.waitRetryDurationMillis = waitRetryDurationMillis;
    }

    public int getTimeoutMillis() {
        return timeoutMillis;
    }

    public void setTimeoutMillis(int timeoutMillis) {
        this.timeoutMillis = timeoutMillis;
    }

    public void setCircuitBreakerConfig(CircuitBreakerConfig c) {
        this.circuitBreakerConfig = c;
    }

    public CircuitBreakerConfig getCircuitBreakerConfig() {
        return this.circuitBreakerConfig;
    }

    public boolean useAsyncRequest() {
        return withAsyncRequest;
    }

    public void withAsyncRequest(boolean withAsyncRequest) {
        this.withAsyncRequest = withAsyncRequest;
    }

    @Override
    protected Object clone() throws CloneNotSupportedException {
        return super.clone();
    }
}
