package cn.ymatrix.httpclient;

import cn.ymatrix.logger.MxLogger;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.slf4j.Logger;

import java.security.InvalidParameterException;

/**
 * Jetty HTTP Client is thread safe, and we will use this singleton in the global Application.
 */
public class SingletonHTTPClient {
    private static final String TAG = SingletonHTTPClient.class.getName();
    private static final Logger l = MxLogger.init(SingletonHTTPClient.class);
    private static int maxQueuedConnection = 2048;
    public static SingletonHTTPClient getInstance(int maxQueued) {
        maxQueuedConnection = maxQueued;
        return innerClass.instance;
    }

    private SingletonHTTPClient() {

    }

    private static class innerClass {
        private static final SingletonHTTPClient instance = new SingletonHTTPClient();
    }

    public HttpClient getClient() throws InvalidParameterException {
        QueuedThreadPool threadPool = new QueuedThreadPool(500, 10);
        HttpClient client = new HttpClient();
        client.setExecutor(threadPool);
        client.setMaxRequestsQueuedPerDestination(maxQueuedConnection);
        l.info("{} Set max connection per destination to {} in jetty.", TAG, maxQueuedConnection);
        try {
            client.start();
        } catch (Exception e) {
            l.error("{} HTTP Client(Jetty) start with exception: ", TAG, e);
            return null;
        }
        return client;
    }

}
