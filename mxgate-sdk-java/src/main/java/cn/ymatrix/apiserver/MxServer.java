package cn.ymatrix.apiserver;


import cn.ymatrix.builder.CircuitBreakerConfig;
import cn.ymatrix.cache.Cache;
import cn.ymatrix.data.Tuples;

/**
 * Tuples consumer to consume tuples from cache.
 */
public interface MxServer {
    void consume(Cache cache);

    void getJobMetadata(String gRPCHost, String schema, String table, GetJobMetadataListener listener);

    SendDataResult sendDataBlocking(Tuples tuples);

    void shutdownNow();

    boolean isShutdown();

    boolean isTerminated();

    CircuitBreakerConfig getCircuitBreakerConfig();
}
