package cn.ymatrix.apiserver;

import cn.ymatrix.api.StatusCode;
import cn.ymatrix.builder.CircuitBreakerConfig;
import cn.ymatrix.builder.MxBuilder;
import cn.ymatrix.cache.Cache;
import cn.ymatrix.concurrencycontrol.WorkerPool;
import cn.ymatrix.concurrencycontrol.WorkerPoolFactory;
import cn.ymatrix.builder.ServerConfig;
import cn.ymatrix.data.Tuples;
import cn.ymatrix.exception.RetryException;
import cn.ymatrix.faulttolerance.RetryConfiguration;
import cn.ymatrix.httpclient.SingletonHTTPClient;
import cn.ymatrix.logger.MxLogger;
import cn.ymatrix.utils.CSVConstructor;
import cn.ymatrix.utils.StrUtil;
import cn.ymatrix.worker.JobMetadataManager;
import cn.ymatrix.worker.TuplesConsumer;
import cn.ymatrix.worker.TuplesSendingBlockingWorker;
import org.eclipse.jetty.client.HttpClient;
import org.slf4j.Logger;

import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.List;

/**
 * The implementation of MXServer.
 */
class MxServerInstance implements MxServer {
    private static final String TAG = StrUtil.logTagWrap(MxServerInstance.class.getName());
    private static final Logger l = MxLogger.init(MxServerInstance.class);
    private final int concurrency;
    private final int workConcurrency;
    private final WorkerPool fixedSizeWorkerPool;
    private final WorkerPool cachedWorkerPool;
    private final WorkerPool taskDispatchPool;
    private final ServerConfig serverConfig;
    private static RetryConfiguration retryConfiguration;
    private final TuplesSendingBlockingWorker blockingSender;
    private final List<TuplesConsumer> tuplesConsumers;

    // Each MxServer only contains one HttpClient for Http Request.
    private final HttpClient httpClient;

    private Cache cache;

    private final CSVConstructor constructor;

    public static MxServer getInstance(ServerConfig config) throws InvalidParameterException, NullPointerException {
        if (config == null) {
            throw new NullPointerException("create mx server on an empty server config.");
        }
        int concurrency = config.getConcurrency();
        if (config.getConcurrency() <= 0) {
            throw new InvalidParameterException("invalid concurrency parameter in MxServer " + concurrency);
        }
        // Prepare for retry config.
        try {
            retryConfiguration = needRetry(config);
        } catch (InvalidParameterException | NullPointerException e) {
            l.error("{} MxServer retry policy configuration exception: {}", TAG, e.getMessage());
            throw e;
        }
        return new MxServerInstance(config);
    }

    private MxServerInstance(ServerConfig config) throws InvalidParameterException, NullPointerException {
        l.info("{} Init MxServer single instance with Concurrency = {}, MaxRetryAttempts = {}, WaitRetryDurationMillis = {}, TimeoutMillis = {} .",
                TAG, config.getConcurrency(), config.getMaxRetryAttempts(), config.getWaitRetryDurationMillis(), config.getTimeoutMillis());
        this.serverConfig = config;
        this.concurrency = config.getConcurrency();
        this.workConcurrency = this.concurrency;
        this.fixedSizeWorkerPool = WorkerPoolFactory.initFixedSizeWorkerPool(this.workConcurrency);
        this.taskDispatchPool = WorkerPoolFactory.initFixedSizeWorkerPool(this.concurrency);
        this.cachedWorkerPool = WorkerPoolFactory.getCachedWorkerPool();
        this.httpClient = SingletonHTTPClient.getInstance(MxBuilder.maxQueuedConn).getClient();
        this.constructor = new CSVConstructor(serverConfig.getCSVConstructionParallel());
        if (this.httpClient == null) {
            throw new NullPointerException("Create HttpClient with exception, the client is null.");
        }
        this.blockingSender = new TuplesSendingBlockingWorker(config.getRequestType(), httpClient, this.constructor);
        if (retryConfiguration != null) {
            this.blockingSender.withRetry(retryConfiguration);
        }
        this.tuplesConsumers = new ArrayList<>();
        CircuitBreakerConfig cConfig = serverConfig.getCircuitBreakerConfig();
        if (cConfig != null && cConfig.isEnable()) {
            this.blockingSender.withCircuitBreaker(cConfig);
        }
    }

    /**
     * Multiple work thread consume tuples from cache concurrently.
     * @param cache to get the cached tuples.
     */
    @Override
    public void consume(Cache cache) {
        l.info("{} Begin to consume tuples from cache with {} consumer workers concurrently.", TAG, this.concurrency);
        this.cache = cache;
        for (int i = 0; i < this.workConcurrency; i++) {
            TuplesConsumer tuplesConsumer = new TuplesConsumer(this.httpClient, this.constructor);
            tuplesConsumer.workIn(this.fixedSizeWorkerPool);
            tuplesConsumer.poolToDispatchTask(this.taskDispatchPool);
            tuplesConsumer.dropAll(this.serverConfig.isDropAll());
            tuplesConsumer.setRequestType(this.serverConfig.getRequestType());
            tuplesConsumer.useAsyncRequest(this.serverConfig.useAsyncRequest());

            // Register retry configuration.
            if (retryConfiguration != null) {
                tuplesConsumer.withRetry(retryConfiguration);
            }

            CircuitBreakerConfig cConfig = serverConfig.getCircuitBreakerConfig();
            if (cConfig != null && cConfig.isEnable()) {
                tuplesConsumer.withCircuitBreaker(cConfig);
            }

            // Set timeout.
            if (serverConfig != null) {
                tuplesConsumer.setTimeoutMillis(serverConfig.getTimeoutMillis());
            }

            // Register tuples from cache.
            tuplesConsumer.consume(cache);
            this.tuplesConsumers.add(tuplesConsumer);
        }
    }

    @Override
    public CircuitBreakerConfig getCircuitBreakerConfig() {
        return this.serverConfig.getCircuitBreakerConfig();
    }

    /**
     * Launch a single task to fetch the metadata for target job.
     */
    @Override
    public void getJobMetadata(String gRPCHost, String schema, String table, GetJobMetadataListener listener) throws NullPointerException, InvalidParameterException {
        if (listener == null) {
            throw new NullPointerException("GetJobMetadataListener is null");
        }
        l.info("{} Get job metadata in MxServer for table {}.{}", TAG, schema, table);
        JobMetadataManager manager = new JobMetadataManager();
        manager.workIn(this.cachedWorkerPool);

        // Register retry configuration.
        if (retryConfiguration != null) {
            manager.withRetry(retryConfiguration);
        }

        // Set timeout.
        if (serverConfig != null) {
            manager.setTimeoutMillis(serverConfig.getTimeoutMillis());
        }

        try {
            manager.getJobMetadata(gRPCHost, schema, table, listener);
        } catch (Exception e) {
            l.error("{} getJobMetadata exception", TAG, e);
        }
    }

    /**
     * Shutdown MxServer Immediately.
     */
    @Override
    public void shutdownNow() {
        l.info("{} Begin to shutdown MxServer directly.", TAG);
        this.stopTuplesConsumers();

        if (this.cachedWorkerPool != null) {
            this.cachedWorkerPool.shutdownNow();
        }

        if (this.fixedSizeWorkerPool != null) {
            this.fixedSizeWorkerPool.shutdownNow();
        }

        if (this.taskDispatchPool != null) {
            this.taskDispatchPool.shutdownNow();
        }
        l.info("{} MxServer is shutdown now.", TAG);
    }

    /**
     * Shutdown all the thread pools and stop all the TuplesConsumer's consume loop.
     * @return
     */
    @Override
    public boolean isShutdown() {
        if (!this.cachedWorkerPool.isShutdown()) {
            return false;
        }

        if (!this.fixedSizeWorkerPool.isShutdown()) {
            return false;
        }

        if (!this.taskDispatchPool.isShutdown()) {
            return false;
        }

        return this.isTuplesConsumersStopped();
    }

    @Override
    public boolean isTerminated() {
        if (!this.cachedWorkerPool.isTerminated()) {
            return false;
        }

        if (!this.fixedSizeWorkerPool.isTerminated()) {
            return false;
        }

        if (!this.taskDispatchPool.isTerminated()) {
            return false;
        }

        return this.isTuplesConsumersStopped();
    }

    private boolean isTuplesConsumersStopped() {
        for (TuplesConsumer consumer : this.tuplesConsumers) {
            if (consumer != null && !consumer.isStopped()) {
                return false;
            }
        }
        return true;
    }

    private void stopTuplesConsumers() {
        for (TuplesConsumer consumer : this.tuplesConsumers) {
            if (consumer != null) {
                consumer.stop();
            }
        }
    }

    @Override
    public SendDataResult sendDataBlocking(Tuples tuples) throws NullPointerException {
        if (this.serverConfig.isDropAll()) {
            // Comment out this through output calculation to avoid the impact of data loading performance.
            // ThroughoutCalculator.getInstance().add(tuples);
            l.info("{} Drop tuples in blocking mode.", TAG);
            return new SendDataResult(StatusCode.NORMAL, null, null);
        }
        return this.blockingSender.sendTuplesBlocking(tuples);
    }

    private static RetryConfiguration needRetry(ServerConfig config) throws InvalidParameterException, NullPointerException {
        if (config == null) {
            throw new NullPointerException("ServerConfig must be configured for retry");
        }

        // Invalid retry configuration.
        if (config.getMaxRetryAttempts() > 0 && config.getWaitRetryDurationMillis() <= 0) {
            throw new InvalidParameterException("WaitRetryDurationMillis must be configured for retry.");
        }

        // Invalid retry configuration.
        if (config.getWaitRetryDurationMillis() > 0 && config.getMaxRetryAttempts() <= 0) {
            throw new InvalidParameterException("MaxRetryAttempts must be configured for retry");
        }

        // Both configurations are negative, no need to retry, return null.
        if (config.getMaxRetryAttempts() <= 0 && config.getWaitRetryDurationMillis() <= 0) {
            return null;
        }

        l.info("{} Configure retry policy in MxServer MaxRetryAttempts = {}, WaitRetryAttempts = {} .", TAG, config.getMaxRetryAttempts(), config.getWaitRetryDurationMillis());

        // Both configuration are positive, return a new retry configuration.
        return new RetryConfiguration(config.getMaxRetryAttempts(), config.getWaitRetryDurationMillis(), RetryException.class);
    }
}
