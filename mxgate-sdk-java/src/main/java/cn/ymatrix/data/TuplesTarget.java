package cn.ymatrix.data;

/**
 * This class contains the target info
 * of which these have to be sent to.
 */
public class TuplesTarget {

    /**
     * The server url.
     */
    private String URL;

    /**
     * The request timeout.
     */
    private int timeout;

    public TuplesTarget() {

    }

    public String getURL() {
        return URL;
    }

    public void setURL(String URL) {
        this.URL = URL;
    }

    public int getTimeout() {
        return timeout;
    }

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }
}
