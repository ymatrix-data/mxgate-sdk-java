package cn.ymatrix.builder;

/**
 * The configuration of HTTP Request.
 */
public class HTTPConfig {
    private int maxRetryAttempts;
    private int requestTimeoutMillis;
    private int waitRetryDurationLimitation;
    private String serverURLGRPC;

    private String serverURL4DataSending;

    public HTTPConfig() {

    }

    public String getServerURLDataSending() {
        return serverURL4DataSending;
    }

    public void setServerURLDataSending(String serverURLHTTP) {
        this.serverURL4DataSending = serverURLHTTP;
    }

    public String getServerURLGRPC() {
        return serverURLGRPC;
    }

    public void setServerURLGRPC(String URL) {
        this.serverURLGRPC = URL;
    }

    public int getMaxRetryAttempts() {
        return maxRetryAttempts;
    }

    public void setMaxRetryAttempts(int maxRetryAttempts) {
        this.maxRetryAttempts = maxRetryAttempts;
    }

    public int getRequestTimeoutMillis() {
        return requestTimeoutMillis;
    }

    public void setRequestTimeoutMillis(int requestTimeout) {
        this.requestTimeoutMillis = requestTimeout;
    }

    public int getWaitRetryDurationLimitation() {
        return waitRetryDurationLimitation;
    }

    public void setWaitRetryDurationLimitation(int waitRetryDurationLimitation) {
        this.waitRetryDurationLimitation = waitRetryDurationLimitation;
    }
}
