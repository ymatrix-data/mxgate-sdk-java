package cn.ymatrix.builder;

import cn.ymatrix.api.JobMetadataWrapper;
import cn.ymatrix.apiclient.MxClient;
import cn.ymatrix.apiserver.GetJobMetadataListener;
import cn.ymatrix.apiserver.MxServer;
import cn.ymatrix.apiserver.MxServerFactory;
import cn.ymatrix.cache.Cache;
import cn.ymatrix.cache.CacheFactory;
import cn.ymatrix.exception.ConnectTimeoutException;
import cn.ymatrix.logger.MxLogger;
import cn.ymatrix.utils.StrUtil;
import org.slf4j.Logger;

import javax.management.InvalidAttributeValueException;
import java.security.InvalidParameterException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class MxBuilder {
    private static final String TAG = StrUtil.logTagWrap(MxBuilder.class.getName());
    public static final String SDK_VERSION = "1.1.2";
    private static final Logger l = MxLogger.init(MxBuilder.class);
    private static final int CONNECT_BLOCKING_WAIT_TIMEOUT_MILLIS = 20000;
    private static final int CPU_CORES_NUMBER = Runtime.getRuntime().availableProcessors();
    private static final int DEFAULT_TUPLES_CACHE_CAPACITY = 1000;
    private static final int DEFAULT_CACHE_TIMEOUT_MILLIS = 3000;

    /**
     * API Server Config.
     */
    private final ServerConfig serverConfig;

    /**
     * Cache to cache tuples.
     */
    private int cacheCapacity;
    private long enqueueTimeout;

    private final Set<Cache> cacheSet;
    private final Set<MxServer> serverSet;

    // Request timeout millisecond.
    private int requestTimeoutMillis;

    private int concurrency;

    private int waitRetryDurationLimitation;

    private int maxRetryAttempts;

    private RequestType requestType;

    private CircuitBreakerConfig circuitBreakerConfig;

    // This flag is used for debug, is dropAll == true,
    // we will drop all the Tuples and not send them to the backend server.
    public AtomicBoolean dropAll = new AtomicBoolean(false);

    public static int maxQueuedConn = 2048;
    private final AtomicInteger clientSerialNum;

    // Serial Number -> MxClient.
    private final Map<Integer, MxClientImpl> clientsMap;

    private final AtomicBoolean shutdown;

    // Whether to use async requests in the tuples consumer
    private boolean withRequestAsync;

    private final Map<Integer, MxClientGroup> clientGroups;

    private int csvConstructionParallel;

    public static Builder newBuilder() {
        return Builder.BuilderSingleton.instance;
    }

    public static MxBuilder instance() {
        return Builder.BuilderSingleton.instance.mxBuilderInstance();
    }

    private MxBuilder() {
        l.info("{} Init MxBuilder Version = {}", TAG, SDK_VERSION);
        this.requestType = RequestType.WithGRPC;
        this.serverConfig = new ServerConfig();
        this.clientSerialNum = new AtomicInteger(0);
        this.clientsMap = new ConcurrentHashMap<>();
        this.shutdown = new AtomicBoolean(false);
        this.circuitBreakerConfig = new CircuitBreakerConfig();
        this.cacheSet = Collections.synchronizedSet(new HashSet<>());
        this.serverSet = Collections.synchronizedSet(new HashSet<>());
        this.clientGroups = new ConcurrentHashMap<>();
        // The CSV construction is a kind of calculation intensive task.
        // By default, the CSV construction pool size is equals with the CPU core numbers.
        this.csvConstructionParallel = CPU_CORES_NUMBER;
    }

    private void withMaxQueuedConn(int maxConn) {
        maxQueuedConn = maxConn;
    }

    private void build() throws InvalidParameterException, NullPointerException {
        if (this.concurrency <= 0) { // Concurrency must be set.
            l.error("{} Invalid concurrency parameter {}, it must > 0 .", TAG, this.concurrency);
            throw new InvalidParameterException("invalid concurrency parameter " + this.concurrency);
        }

        if (this.requestTimeoutMillis < 0) { // Timeout is not strictly needed, but it could not be negative.
            l.error("{} Invalid request timeout parameter: {} .", TAG, this.requestTimeoutMillis);
            throw new InvalidParameterException("invalid request timout parameter " + this.requestTimeoutMillis);
        }

        if (this.cacheCapacity <= 0) {
            this.cacheCapacity = DEFAULT_TUPLES_CACHE_CAPACITY;
        }

        if (this.enqueueTimeout <= 0) {
            this.enqueueTimeout = DEFAULT_CACHE_TIMEOUT_MILLIS;
        }

        if (this.csvConstructionParallel <= 0) {
            this.csvConstructionParallel = CPU_CORES_NUMBER;
        }

        this.serverConfig.setTimeoutMillis(this.requestTimeoutMillis);
        this.serverConfig.setMaxRetryAttempts(this.maxRetryAttempts);
        this.serverConfig.setConcurrency(this.concurrency);
        this.serverConfig.setWaitRetryDurationMillis(this.waitRetryDurationLimitation);
        this.serverConfig.setDropAll(this.dropAll.get());
        this.serverConfig.setRequestType(this.requestType);
        this.serverConfig.setCircuitBreakerConfig(this.circuitBreakerConfig);
        this.serverConfig.withAsyncRequest(this.withRequestAsync);
        this.serverConfig.setCSVConstructionParallel(this.csvConstructionParallel);

        l.info("{} Builder build with Concurrency = {}, MaxRetryAttempts = {}, WaitRetryDurationMillis = {}, TimeoutMillis = {}, AsyncRequest = {}.", TAG,
                this.concurrency, this.maxRetryAttempts, this.waitRetryDurationLimitation, this.requestTimeoutMillis, this.withRequestAsync);
    }

    private Cache prepareCache() {
        return CacheFactory.getCacheInstance(this.cacheCapacity, this.enqueueTimeout);
    }

    private Cache prepareCacheWithConfig(int cacheCapacity, int enqueueTimeout) {
        return CacheFactory.getCacheInstance(cacheCapacity, enqueueTimeout);
    }

    private MxServer prepareMxServer() {
        return MxServerFactory.getMxServerInstance(this.serverConfig);
    }

    private MxServer prepareMxServerWithNewConfig(int concurrency) {
        try {
            ServerConfig config = (ServerConfig) this.serverConfig.clone();
            config.setConcurrency(concurrency);
            return MxServerFactory.getMxServerInstance(config);
        } catch (CloneNotSupportedException e) {
            l.error("{} ServerConfig clone with exception", TAG, e);
        }
        return null;
    }

    private void checkStrParamShouldNotBeEmpty(String... strs) throws InvalidParameterException {
        for (String str : strs) {
            if (StrUtil.isNullOrEmpty(str)) {
                throw new InvalidParameterException();
            }
        }
    }

    private void checkIntParamShouldBePositive(int... ints) throws InvalidParameterException {
        for (int i : ints) {
            if (i <= 0) {
                throw new InvalidParameterException();
            }
        }
    }

    private MxClientImpl newClientJoinGroupWithConfig(final int serialNum, int cacheCapacity, int enqueueTimeout, int concurrency,
                                                      ClientConfig clientConfig, HTTPConfig httpConfig, int groupNum) {
        // The MxClientGroup is not exists.
        // Create MxServer and Cache
        final MxServer server = prepareMxServerWithNewConfig(concurrency);
        if (server == null) {
            throw new NullPointerException("Prepare MxServer with config with exception, MxServer is null.");
        }
        final Cache cache = prepareCacheWithConfig(cacheCapacity, enqueueTimeout);
        server.consume(cache);

        // Create a new MxClient
        final MxClientImpl newClient = new MxClientImpl(serialNum, clientConfig, httpConfig, cache, server);

        // Create a new MxClientGroup
        MxClientGroup newGroup = new MxClientGroup(server, cache, groupNum);
        newGroup.addClient(newClient);
        this.clientGroups.put(groupNum, newGroup);
        l.info("{} New MxClient({}) join the group which group number is {}", TAG, newClient.getClientName(), newGroup.getGroupNum());
        return newClient;
    }

    private MxClientImpl newClientJoinGroup(final int serialNum, ClientConfig clientConfig, HTTPConfig httpConfig,
                                            int groupNum) {
        final Cache cache = prepareCache();
        final MxServer server = prepareMxServer();
        server.consume(cache);
        // Create a new MxClient
        final MxClientImpl newClient = new MxClientImpl(serialNum, clientConfig, httpConfig, cache, server);
        // Create a new MxClientGroup
        MxClientGroup newGroup = new MxClientGroup(server, cache, groupNum);
        newGroup.addClient(newClient);
        this.clientGroups.put(groupNum, newGroup);
        l.info("{} New MxClient({}) join the group which group number is {}", TAG, newClient.getClientName(), newGroup.getGroupNum());
        return newClient;
    }


    /**
     * Connect backend server and generate a MxClient instance with callback.
     *
     * @param serverURLDataSending backend server URL of gRPC service to send data.
     * @param serverURLGRPC        backend server URL of gRPC service to get JobMetadata.
     * @param schema               table schema.
     * @param table                table name.
     * @param listener             callback.
     */
    public void connect(String serverURLDataSending, String serverURLGRPC, String schema, String table, ConnectionListener listener)
            throws NullPointerException, InvalidParameterException, IllegalStateException {
        this.stateCheck();
        this.checkStrParamShouldNotBeEmpty(serverURLDataSending, serverURLGRPC, schema, table);

        if (listener == null) {
            l.error("{} ConnectionListener is null for connect.", TAG);
            throw new NullPointerException("ConnectionListener is null for connect");
        }

        ClientConfig clientConfig = new ClientConfig(schema, table);
        HTTPConfig httpConfig = prepareHttpConfig(serverURLGRPC, serverURLDataSending);

        final MxServer server = prepareMxServer();
        final Cache cache = prepareCache();
        server.consume(cache);

        final MxClientImpl client = new MxClientImpl(clientSerialNum.incrementAndGet(), clientConfig, httpConfig, cache, server);

        connectWithCallback(client, clientConfig, httpConfig, server, cache, listener);
    }

    /**
     * Connect backend server and generate a MxClient instance with callback.
     * And the concurrency, cacheCapacity and enqueueTimeout can be configured.
     * @param serverURLDataSending
     * @param serverURLGRPC
     * @param schema
     * @param table
     * @param listener
     * @param concurrency
     * @param cacheCapacity
     * @param enqueueTimeout
     * @throws NullPointerException
     * @throws InvalidParameterException
     * @throws IllegalStateException
     */
    public void connect(String serverURLDataSending, String serverURLGRPC, String schema, String table, ConnectionListener listener,
                        int concurrency, int cacheCapacity, int enqueueTimeout)
            throws NullPointerException, InvalidParameterException, IllegalStateException {
        this.stateCheck();
        this.checkStrParamShouldNotBeEmpty(serverURLDataSending, serverURLGRPC, schema, table);
        this.checkIntParamShouldBePositive(concurrency, cacheCapacity, enqueueTimeout);

        if (listener == null) {
            l.error("{} ConnectionListener is null for connect.", TAG);
            throw new NullPointerException("ConnectionListener is null for connect");
        }

        ClientConfig clientConfig = new ClientConfig(schema, table);
        HTTPConfig httpConfig = prepareHttpConfig(serverURLGRPC, serverURLDataSending);

        final MxServer server = prepareMxServerWithNewConfig(concurrency);
        if (server == null) {
            throw new NullPointerException("Prepare MxServer with config with exception, MxServer is null.");
        }
        final Cache cache = prepareCacheWithConfig(cacheCapacity, enqueueTimeout);
        server.consume(cache);

        final MxClientImpl client = new MxClientImpl(clientSerialNum.incrementAndGet(), clientConfig, httpConfig, cache, server);

        connectWithCallback(client, clientConfig, httpConfig, server, cache, listener);
    }

    private void connectWithCallback(final MxClientImpl client, ClientConfig clientConfig, HTTPConfig httpConfig,
                                     MxServer server, Cache cache, ConnectionListener listener) {
        server.getJobMetadata(httpConfig.getServerURLGRPC(), clientConfig.getSchema(), clientConfig.getTable(), new GetJobMetadataListener() {
            @Override
            public void onSuccess(JobMetadataWrapper metadataWrapper) {
                if (metadataWrapper == null) {
                    listener.onFailure("get null job metadata from MxServer for table "
                            + clientConfig.getSchema()
                            + "."
                            + clientConfig.getTable());
                    l.error("{} Get null job metadata from MxServer for table {}", TAG,
                            clientConfig.getSchema() + "." + clientConfig.getTable());
                    return;
                }
                try {
                    client.prepareMetadata(metadataWrapper);
                    // Contains the reference of this MxClient.
                    MxBuilder.this.clientsMap.put(client.getSerialNumber(), client);

                    // Add cache and server references into List synchronized;
                    synchronized (MxBuilder.this) {
                        MxBuilder.this.cacheSet.add(cache);
                        MxBuilder.this.serverSet.add(server);
                    }

                    // Return MxClient instance to the user through the callback.
                    listener.onSuccess(client);
                    l.info("{} Init MxClient({}) for table {}.{}.", TAG, client, clientConfig.getSchema(), clientConfig.getTable());
                } catch (InvalidAttributeValueException e) {
                    l.error("{} Prepare metadata exception: ", TAG, e);
                    listener.onFailure("prepare metadata exception: " + e.getMessage());
                }
            }

            @Override
            public void onFailure(String failureMsg) {
                listener.onFailure(failureMsg);
                l.error("{} Get job meta data failure {}", TAG, failureMsg);
            }
        });
    }

    /**
     * Connect backend server and generate a MxClient instance with blocking mode.
     * @param serverURLDataSending
     * @param serverURLGRPC
     * @param schema
     * @param table
     * @return
     * @throws NullPointerException
     * @throws InvalidParameterException
     * @throws IllegalStateException
     */
    public MxClient connect(String serverURLDataSending, String serverURLGRPC, String schema, String table)
            throws NullPointerException, InvalidParameterException, IllegalStateException {
        this.stateCheck();
        this.checkStrParamShouldNotBeEmpty(serverURLDataSending, serverURLGRPC, schema, table);
        ClientConfig clientConfig = new ClientConfig(schema, table);
        HTTPConfig httpConfig = prepareHttpConfig(serverURLGRPC, serverURLDataSending);
        final MxServer server = prepareMxServer();
        final Cache cache = prepareCache();
        server.consume(cache);
        final MxClientImpl client = new MxClientImpl(clientSerialNum.incrementAndGet(), clientConfig, httpConfig, cache, server);
        return connectBlocking(client, clientConfig, httpConfig, server, cache);
    }

    /**
     * Connect backend server and generate a MxClient instance with blocking mode.
     * And the concurrency, cacheCapacity and enqueueTimeout can be configured.
     * @param serverURLDataSending
     * @param serverURLGRPC
     * @param schema
     * @param table
     * @param concurrency
     * @param cacheCapacity
     * @param enqueueTimeout
     * @return
     */
    public MxClient connect(String serverURLDataSending, String serverURLGRPC, String schema, String table,
                            int concurrency, int cacheCapacity, int enqueueTimeout) {
        this.stateCheck();
        this.checkStrParamShouldNotBeEmpty(serverURLDataSending, serverURLGRPC, schema, table);
        this.checkIntParamShouldBePositive(concurrency, cacheCapacity, enqueueTimeout);
        ClientConfig clientConfig = new ClientConfig(schema, table);
        HTTPConfig httpConfig = prepareHttpConfig(serverURLGRPC, serverURLDataSending);
        final MxServer server = prepareMxServerWithNewConfig(concurrency);
        if (server == null) {
            throw new NullPointerException("Prepare MxServer with config with exception, MxServer is null.");
        }
        final Cache cache = prepareCacheWithConfig(cacheCapacity, enqueueTimeout);
        server.consume(cache);
        final MxClientImpl client = new MxClientImpl(clientSerialNum.incrementAndGet(), clientConfig, httpConfig, cache, server);
        return connectBlocking(client, clientConfig, httpConfig, server, cache);
    }

    private MxClient connectBlocking(final MxClientImpl client, ClientConfig clientConfig, HTTPConfig httpConfig, MxServer server, Cache cache) {
        final CountDownLatch latch = new CountDownLatch(1);
        server.getJobMetadata(httpConfig.getServerURLGRPC(), clientConfig.getSchema(), clientConfig.getTable(), new GetJobMetadataListener() {
            @Override
            public void onSuccess(JobMetadataWrapper metadataWrapper) {
                if (metadataWrapper == null) {
                    l.error("{} Get null job metadata from MxServer for table {}", TAG,
                            clientConfig.getSchema() + "." + clientConfig.getTable());
                    // Will not count down the latch, let latch.await() to timeout.
                    return;
                }
                try {
                    client.prepareMetadata(metadataWrapper);
                    // Return MxClient instance to the user through the callback.
                    l.info("{} Init MxClient({}) for table {}.{}.", TAG, client, clientConfig.getSchema(), clientConfig.getTable());
                    latch.countDown();
                } catch (InvalidAttributeValueException e) {
                    l.error("{} Prepare metadata exception: ", TAG, e);
                    // Will not count down the latch, let latch.await() to timeout.
                }
            }

            @Override
            public void onFailure(String failureMsg) {
                l.error("{} Get job meta data failure {}", TAG, failureMsg);
                // Will not count down the latch, let latch.await() to timeout.
            }
        });

        try {
            // By default, timeout configuration, will wait for default timeout milliseconds.
            int waitTime = this.getCloseWaitTimeMillis();
            if (latch.await(waitTime, TimeUnit.MILLISECONDS)) {
                this.clientsMap.put(client.getSerialNumber(), client);
                // Add cache and server references into List synchronized;
                synchronized (MxBuilder.this) {
                    this.cacheSet.add(cache);
                    this.serverSet.add(server);
                }
                return client;
            }
            l.error("{} Latch await timeout, get MxClient fail.", TAG);
            throw new ConnectTimeoutException("latch await timeout, get MxClient fail");
        } catch (InterruptedException e) {
            l.error("{} Latch await exception, get MxClient fail.", TAG);
            throw new RuntimeException(e);
        }
    }

    /**
     * Connect backend server with callback and join MxClient into MxClientGroup which number is groupNum and
     * the concurrency, cacheCapacity, enqueueTimeout can be configured.
     * @param serverURLDataSending
     * @param serverURLGRPC
     * @param schema
     * @param table
     * @param listener
     * @param concurrency
     * @param cacheCapacity
     * @param enqueueTimeout
     * @param groupNum
     * @throws NullPointerException
     * @throws InvalidParameterException
     * @throws IllegalStateException
     */
    public synchronized void connectWithGroup(String serverURLDataSending, String serverURLGRPC, String schema, String table, ConnectionListener listener,
                                              int concurrency, int cacheCapacity, int enqueueTimeout, int groupNum)
            throws NullPointerException, InvalidParameterException, IllegalStateException {

        this.stateCheck();
        this.checkStrParamShouldNotBeEmpty(serverURLDataSending, serverURLGRPC, schema, table);
        this.checkIntParamShouldBePositive(concurrency, cacheCapacity, enqueueTimeout, groupNum);
        if (listener == null) {
            l.error("{} ConnectionListener is null for connect.", TAG);
            throw new NullPointerException("ConnectionListener is null for connect");
        }

        ClientConfig clientConfig = new ClientConfig(schema, table);
        HTTPConfig httpConfig = prepareHttpConfig(serverURLGRPC, serverURLDataSending);

        MxClientGroup group = this.clientGroups.get(groupNum);

        // The MxClientGroup Already exists.
        if (group != null) {
            final MxClientImpl client = new MxClientImpl(clientSerialNum.incrementAndGet(), clientConfig, httpConfig, group.getCache(), group.getServer());
            group.addClient(client);
            l.info("{} New MxClient({}) join the group which group number is {}", TAG, client.getClientName(), group.getGroupNum());
            connectWithCallback(client, clientConfig, httpConfig, group.getServer(), group.getCache(), listener);
            return;
        }

        // The MxClientGroup is not exists.
        MxClientImpl newClient = newClientJoinGroupWithConfig(clientSerialNum.incrementAndGet(), cacheCapacity, enqueueTimeout, concurrency, clientConfig, httpConfig, groupNum);
        connectWithCallback(newClient, clientConfig, httpConfig, newClient.getServer(), newClient.getCache(), listener);
    }

    /**
     * Connect backend server with callback and join MxClient into MxClientGroup which number is groupNum.
     * @param serverURLDataSending
     * @param serverURLGRPC
     * @param schema
     * @param table
     * @param listener
     * @param groupNum
     * @throws NullPointerException
     * @throws InvalidParameterException
     * @throws IllegalStateException
     */
    public synchronized void connectWithGroup(String serverURLDataSending, String serverURLGRPC, String schema,
                                              String table, ConnectionListener listener, int groupNum)
            throws NullPointerException, InvalidParameterException, IllegalStateException {
        this.stateCheck();
        this.checkStrParamShouldNotBeEmpty(serverURLDataSending, serverURLGRPC, schema, table);
        this.checkIntParamShouldBePositive(groupNum);
        if (listener == null) {
            l.error("{} ConnectionListener is null for connect.", TAG);
            throw new NullPointerException("ConnectionListener is null for connect");
        }

        ClientConfig clientConfig = new ClientConfig(schema, table);
        HTTPConfig httpConfig = prepareHttpConfig(serverURLGRPC, serverURLDataSending);

        MxClientGroup group = this.clientGroups.get(groupNum);

        // The MxClientGroup Already exists.
        if (group != null) {
            final MxClientImpl client = new MxClientImpl(clientSerialNum.incrementAndGet(), clientConfig, httpConfig, group.getCache(), group.getServer());
            group.addClient(client);
            l.info("{} New MxClient({}) join the group which group number is {}", TAG, client.getClientName(), group.getGroupNum());
            connectWithCallback(client, clientConfig, httpConfig, group.getServer(), group.getCache(), listener);
            return;
        }

        // The MxClientGroup is not exists.
        MxClientImpl newClient = newClientJoinGroup(clientSerialNum.incrementAndGet(), clientConfig, httpConfig, groupNum);
        connectWithCallback(newClient, clientConfig, httpConfig, newClient.getServer(), newClient.getCache(), listener);
    }

    /**
     * Connect backend server in blocking mode and join MxClient into MxClientGroup which number is groupNum and
     * the concurrency, cacheCapacity, enqueueTimeout can be configured.
     * @param serverURLDataSending
     * @param serverURLGRPC
     * @param schema
     * @param table
     * @param concurrency
     * @param cacheCapacity
     * @param enqueueTimeout
     * @param groupNum
     * @return
     * @throws NullPointerException
     * @throws InvalidParameterException
     * @throws IllegalStateException
     */
    public synchronized MxClient connectWithGroup(String serverURLDataSending, String serverURLGRPC, String schema, String table,
                                                  int concurrency, int cacheCapacity, int enqueueTimeout, int groupNum)
            throws NullPointerException, InvalidParameterException, IllegalStateException {

        this.stateCheck();
        this.checkStrParamShouldNotBeEmpty(serverURLDataSending, serverURLGRPC, schema, table);
        this.checkIntParamShouldBePositive(concurrency, cacheCapacity, enqueueTimeout, groupNum);

        ClientConfig clientConfig = new ClientConfig(schema, table);
        HTTPConfig httpConfig = prepareHttpConfig(serverURLGRPC, serverURLDataSending);

        MxClientGroup group = this.clientGroups.get(groupNum);
        // The MxClientGroup Already exists.
        if (group != null) {
            final MxClientImpl client = new MxClientImpl(clientSerialNum.incrementAndGet(), clientConfig, httpConfig, group.getCache(), group.getServer());
            group.addClient(client);
            l.info("{} New MxClient({}) join the group which group number is {}", TAG, client.getClientName(), group.getGroupNum());
            return connectBlocking(client, clientConfig, httpConfig, group.getServer(), group.getCache());
        }
        // The MxClientGroup is not exists.
        MxClientImpl newClient = newClientJoinGroupWithConfig(clientSerialNum.incrementAndGet(),
                cacheCapacity, enqueueTimeout, concurrency, clientConfig, httpConfig, groupNum);
        return connectBlocking(newClient, clientConfig, httpConfig, newClient.getServer(), newClient.getCache());
    }

    /**
     * Connect backend server in blocking mode and join MxClient into MxClientGroup which number is groupNum.
     * @param serverURLDataSending
     * @param serverURLGRPC
     * @param schema
     * @param table
     * @param groupNum
     * @return
     * @throws NullPointerException
     * @throws InvalidParameterException
     * @throws IllegalStateException
     */
    public synchronized MxClient connectWithGroup(String serverURLDataSending, String serverURLGRPC, String schema, String table, int groupNum)
            throws NullPointerException, InvalidParameterException, IllegalStateException {
        this.stateCheck();
        this.checkStrParamShouldNotBeEmpty(serverURLDataSending, serverURLGRPC, schema, table);
        this.checkIntParamShouldBePositive(groupNum);

        ClientConfig clientConfig = new ClientConfig(schema, table);
        HTTPConfig httpConfig = prepareHttpConfig(serverURLGRPC, serverURLDataSending);

        MxClientGroup group = this.clientGroups.get(groupNum);
        // The MxClientGroup Already exists.
        if (group != null) {
            final MxClientImpl client = new MxClientImpl(clientSerialNum.incrementAndGet(), clientConfig, httpConfig, group.getCache(), group.getServer());
            group.addClient(client);
            l.info("{} New MxClient({}) join the group which group number is {}", TAG, client.getClientName(), group.getGroupNum());
            return connectBlocking(client, clientConfig, httpConfig, group.getServer(), group.getCache());
        }
        // The MxClientGroup is not exists.
        MxClientImpl newClient = newClientJoinGroup(clientSerialNum.incrementAndGet(), clientConfig, httpConfig, groupNum);
        return connectBlocking(newClient, clientConfig, httpConfig, newClient.getServer(), newClient.getCache());
    }

    /**
     * Skip to get table metadata from backend and generate a lite MxClient instance directly.
     * @param serverURLDataSending
     * @param serverURLGRPC
     * @param schema
     * @param table
     * @param delimiter, without metadata, MxClient need to know the delimiter to separate CSV columns.
     * @return MxClient instance
     * @throws InvalidParameterException
     */
    public MxClient skipConnect(String serverURLDataSending, String serverURLGRPC, String schema, String table, String delimiter) throws InvalidParameterException {
        this.stateCheck();
        this.checkStrParamShouldNotBeEmpty(serverURLDataSending, serverURLGRPC, schema, table, delimiter);
        ClientConfig clientConfig = new ClientConfig(schema, table);
        HTTPConfig httpConfig = prepareHttpConfig(serverURLGRPC, serverURLDataSending);
        final int serialNum = clientSerialNum.incrementAndGet();
        final MxServer server = prepareMxServer();
        final Cache cache = prepareCache();
        server.consume(cache);
        MxClient client = new MxClientImpl(serialNum, clientConfig, httpConfig, cache, server);
        client.setDelimiter(delimiter);
        return client;
    }

    /**
     * Skip to get table metadata from backend and generate a lite MxClient instance directly.
     * The concurrency, cacheCapacity and cacheEnqueueTimeout can be configured separately.
     * @param serverURLDataSending
     * @param serverURLGRPC
     * @param schema
     * @param table
     * @param delimiter
     * @param concurrency
     * @param cacheCapacity
     * @param cacheEnqueueTimeoutMillis
     * @return
     * @throws InvalidParameterException
     * @throws NullPointerException
     */
    public MxClient skipConnect(String serverURLDataSending, String serverURLGRPC, String schema, String table, String delimiter,
                                int concurrency, int cacheCapacity, int cacheEnqueueTimeoutMillis) throws InvalidParameterException, NullPointerException {
        this.stateCheck();
        this.checkStrParamShouldNotBeEmpty(serverURLDataSending, serverURLGRPC, schema, table, delimiter);
        this.checkIntParamShouldBePositive(concurrency, cacheCapacity, cacheEnqueueTimeoutMillis);

        ClientConfig clientConfig = new ClientConfig(schema, table);
        HTTPConfig httpConfig = prepareHttpConfig(serverURLGRPC, serverURLDataSending);
        final int serialNum = clientSerialNum.incrementAndGet();
        final MxServer server = prepareMxServerWithNewConfig(concurrency);
        if (server == null) {
            throw new NullPointerException("Prepare MxServer with config with exception, MxServer is null.");
        }

        final Cache cache = prepareCacheWithConfig(cacheCapacity, cacheEnqueueTimeoutMillis);
        server.consume(cache);

        MxClient client = new MxClientImpl(serialNum, clientConfig, httpConfig, cache, server);
        client.setDelimiter(delimiter);
        return client;
    }

    /**
     * Skip to get table metadata from backend and generate a lite MxClient instance directly.
     * This MxClient also join the MxClient group which number is groupNum.
     * The concurrency, cacheCapacity and cacheEnqueueTimeout can be configured separately.
     * @param serverURLDataSending
     * @param serverURLGRPC
     * @param schema
     * @param table
     * @param delimiter
     * @param concurrency
     * @param cacheCapacity
     * @param cacheEnqueueTimeout
     * @param groupNum
     * @return
     * @throws InvalidParameterException
     * @throws NullPointerException
     */
    public synchronized MxClient skipConnectWithGroup(String serverURLDataSending, String serverURLGRPC, String schema, String table, String delimiter,
                                                      int concurrency, int cacheCapacity, int cacheEnqueueTimeout, int groupNum)
            throws InvalidParameterException, NullPointerException {

        this.stateCheck();
        this.checkStrParamShouldNotBeEmpty(serverURLDataSending, serverURLGRPC, schema, table, delimiter);
        this.checkIntParamShouldBePositive(concurrency, cacheCapacity, cacheEnqueueTimeout, groupNum);

        ClientConfig clientConfig = new ClientConfig(schema, table);
        HTTPConfig httpConfig = prepareHttpConfig(serverURLGRPC, serverURLDataSending);

        MxClientGroup group = this.clientGroups.get(groupNum);
        // Group with groupNum is already exists for this MxClient, create a MxClient and join the group.
        // This MxClient will use the MxServer and Cache that are belong to this group.
        if (group != null) {
            MxClient client = new MxClientImpl(clientSerialNum.incrementAndGet(), clientConfig, httpConfig, group.getCache(), group.getServer());
            client.setDelimiter(delimiter);
            group.addClient(client); // Add this MxClint into this group.
            l.info("{} New MxClient({}) join the group which group number is {}", TAG, client.getClientName(), group.getGroupNum());
            return client;
        }

        // Group is not exists.
        MxClient newClient = newClientJoinGroupWithConfig(clientSerialNum.incrementAndGet(), cacheCapacity, cacheEnqueueTimeout, concurrency, clientConfig, httpConfig, groupNum);
        newClient.setDelimiter(delimiter);
        return newClient;
    }

    /**
     * Skip to get table metadata from backend and generate a lite MxClient instance directly.
     * This MxClient also join the MxClient group which number is groupNum.
     * @param serverURLDataSending
     * @param serverURLGRPC
     * @param schema
     * @param table
     * @param delimiter
     * @return
     * @throws InvalidParameterException
     * @throws NullPointerException
     */
    public synchronized MxClient skipConnectWithGroup(String serverURLDataSending, String serverURLGRPC, String schema, String table, String delimiter, int groupNum)
            throws InvalidParameterException, NullPointerException {
        this.stateCheck();
        this.checkStrParamShouldNotBeEmpty(serverURLDataSending, serverURLGRPC, schema, table, delimiter);
        this.checkIntParamShouldBePositive(groupNum);

        ClientConfig clientConfig = new ClientConfig(schema, table);
        HTTPConfig httpConfig = prepareHttpConfig(serverURLGRPC, serverURLDataSending);
        MxClientGroup group = this.clientGroups.get(groupNum);
        // Group with groupNum is already exists for this MxClient, create a MxClient and join the group.
        // This MxClient will use the MxServer and Cache that are belong to this group.
        if (group != null) {
            MxClient client = new MxClientImpl(clientSerialNum.incrementAndGet(), clientConfig, httpConfig, group.getCache(), group.getServer());
            client.setDelimiter(delimiter);
            group.addClient(client); // Add this MxClint into this group.
            l.info("{} New MxClient({}) join the group which group number is {}", TAG, client.getClientName(), group.getGroupNum());
            return client;
        }
        // Group is not exists.
        MxClient newClient = newClientJoinGroup(clientSerialNum.incrementAndGet(), clientConfig, httpConfig, groupNum);
        newClient.setDelimiter(delimiter);
        return newClient;
    }


    private HTTPConfig prepareHttpConfig(String serverURLGRPC, String serverURLDataSending) {
        HTTPConfig httpConfig = new HTTPConfig();
        httpConfig.setRequestTimeoutMillis(this.requestTimeoutMillis);
        httpConfig.setWaitRetryDurationLimitation(this.waitRetryDurationLimitation);
        httpConfig.setMaxRetryAttempts(this.maxRetryAttempts);
        httpConfig.setServerURLGRPC(serverURLGRPC);
        httpConfig.setServerURLDataSending(serverURLDataSending);
        return httpConfig;
    }

    /**
     * We will wait (request_timeout + retry_wait_duration) * max_retry_attempts
     * to make sure the MxClient instance can be got.
     */
    private int getCloseWaitTimeMillis() {
        int waitTimeMillis = (this.requestTimeoutMillis + this.waitRetryDurationLimitation) * this.maxRetryAttempts;
        return waitTimeMillis > 0 ? waitTimeMillis : CONNECT_BLOCKING_WAIT_TIMEOUT_MILLIS;
    }

    private void stateCheck() throws IllegalStateException {
        if (this.shutdown.get()) {
            throw new IllegalStateException("MxBuilder has been shutdown.");
        }
    }

    /**
     * For pause, we only set Tuples Cache to refused state and will not receive any new Tuples again.
     * This operation can be resumed.
     *
     * @throws InterruptedException
     * @throws IllegalStateException
     */
    public synchronized void pause() throws InterruptedException, IllegalStateException {
        // If MxBuilder has been shutdown, can not be paused again.
        this.stateCheck();
        l.info("{} Begin to Pause MxBuilder.", TAG);
        this.pauseMxClients();
        // Set Tuples Cache to refused state.
        for (Cache cache : this.cacheSet) {
            if (cache != null) {
                cache.setRefuse(true);
            }
        }
        l.info("{} MxBuilder has been paused.", TAG);
    }

    public synchronized void resume() {
        // If MxBuilder has been shutdown, can not be resumed.
        this.stateCheck();
        l.info("{} Begin to Resume MxBuilder.", TAG);
        this.resumeMxClients();
        for (Cache cache : this.cacheSet) {
            if (cache != null) {
                cache.setRefuse(false);
            }
        }
        l.info("{} MxBuilder is in resume state now.", TAG);
    }

    /**
     * @throws InterruptedException
     * @throws IllegalStateException
     */
    public synchronized void shutdownNow() throws InterruptedException, IllegalStateException {
        // Check the state of this MxBuilder, if it has already been shutdown, will throw an exception.
        this.stateCheck();
        l.info("{} Begin to shutdown MxBuilder immediately.", TAG);

        // Set the "shutdown" state to true.
        this.shutdown.set(true);

        // Close all the MxClients.
        this.closeMxClients();

        // Close the Tuples Cache.
        for (Cache cache : this.cacheSet) {
            if (cache != null) {
                cache.setRefuse(true);
                cache.clear();
            }
        }

        // Shutdown the MxServer immediately.
        for (MxServer server : this.serverSet) {
            if (server != null) {
                server.shutdownNow();
            }
        }
    }

    public int getTupleCacheSize() {
        int total = 0;
        for (Cache cache : this.cacheSet) {
            if (cache != null) {
                total += cache.size();
            }
        }
        return total;
    }

    private void closeMxClients() throws InterruptedException {
        if (this.clientsMap != null) {
            for (Map.Entry<Integer, MxClientImpl> clientEntry : this.clientsMap.entrySet()) {
                if (clientEntry != null && clientEntry.getValue() != null) {
                    clientEntry.getValue().close();
                    l.info("{} Close MxClient which serial num = {}", TAG, clientEntry.getKey());
                }
            }
        }
    }

    private void pauseMxClients() {
        if (this.clientsMap != null) {
            for (Map.Entry<Integer, MxClientImpl> clientEntry : this.clientsMap.entrySet()) {
                if (clientEntry != null && clientEntry.getValue() != null) {
                    clientEntry.getValue().setClosedState(true);
                    l.info("{} Pause MxClient which serial num = {}", TAG, clientEntry.getKey());
                }
            }
        }
    }

    private void resumeMxClients() {
        if (this.clientsMap != null) {
            for (Map.Entry<Integer, MxClientImpl> clientEntry : this.clientsMap.entrySet()) {
                if (clientEntry != null && clientEntry.getValue() != null) {
                    clientEntry.getValue().setClosedState(false);
                    l.info("{} Resume MxClient which serial num = {}", TAG, clientEntry.getKey());
                }
            }
        }
    }

    private boolean isServersShutdown() {
        for (MxServer server : this.serverSet) {
            if (!server.isShutdown()) {
                return false;
            }
        }
        return true;
    }

    private boolean isCachesRefused() {
        for (Cache cache : this.cacheSet) {
            if (!cache.isRefused()) {
                return false;
            }
        }
        return true;
    }

    private boolean isServerTerminated() {
        for (MxServer server : this.serverSet) {
            if (!server.isTerminated()) {
                return false;
            }
        }
        return true;
    }


    public boolean isShutdown() {
        return this.shutdown.get() && this.isCachesRefused() && this.isServersShutdown();
    }

    public boolean isTerminated() {
        return this.shutdown.get() && this.isCachesRefused() && this.isServerTerminated();
    }

    private void withCacheCapacity(int capacity) {
        this.cacheCapacity = capacity;
    }

    private void withDropAll(boolean drop) {
        this.dropAll.set(drop);
    }

    private void withCacheEnqueueTimeout(long timeoutMs) {
        this.enqueueTimeout = timeoutMs;
    }

    private void withConcurrency(int concurrency) {
        this.concurrency = concurrency;
    }

    private void withRequestTimeoutMillis(int timeout) {
        this.requestTimeoutMillis = timeout;
    }

    private void withMaxRetryAttempts(int maxRetryAttempts) {
        this.maxRetryAttempts = maxRetryAttempts;
    }

    private void withRetryWaitDurationMillis(int waitRetryDurationLimitation) {
        this.waitRetryDurationLimitation = waitRetryDurationLimitation;
    }

    private void withRequestType(RequestType type) {
        this.requestType = type;
    }

    private void withRequestAsync(boolean requestAsync) {
        this.withRequestAsync = requestAsync;
    }

    private void withCSVConstructionParallel(int parallel) {
        this.csvConstructionParallel = parallel;
    }

    public static final class Builder {
        private final MxBuilder mxBuilder;
        private boolean built;

        private static class BuilderSingleton {
            static final Builder instance = new Builder();
        }

        private Builder() {
            // Singleton, only exec once.
            this.built = false;
            mxBuilder = new MxBuilder();
        }

        public synchronized MxBuilder build() throws InvalidParameterException, NullPointerException, IllegalStateException {
            if (this.built) {
                throw new IllegalStateException("The MxBuilder has already been built.");
            }
            this.mxBuilder.build();
            this.built = true;
            return this.mxBuilder;
        }

        private synchronized MxBuilder mxBuilderInstance() throws IllegalStateException {
            if (!this.built) {
                throw new IllegalStateException("Please build MxBuilder first.");
            }
            return this.mxBuilder;
        }

        /**
         * With how many batch of tuples can hold in cache, and this can affect memory usage
         *
         * @param capacity cache capacity, default 1000
         */
        public Builder withCacheCapacity(int capacity) {
            this.mxBuilder.withCacheCapacity(capacity);
            return this;
        }

        /**
         * With how long a batch can wait to enqueue if cache is full. Throw IllegalStateException if timeout.
         *
         * @param timeoutMS timeout in millisecond, default 1000
         */
        public Builder withCacheEnqueueTimeout(long timeoutMS) {
            this.mxBuilder.withCacheEnqueueTimeout(timeoutMS);
            return this;
        }

        /**
         * With how many thread send data to backend server concurrently.
         *
         * @param concurrency thread size
         */
        public Builder withConcurrency(int concurrency) {
            this.mxBuilder.withConcurrency(concurrency);
            return this;
        }

        /**
         * HTTP request will with these seconds until the timeout.
         *
         * @param timeout milliseconds
         */
        public Builder withRequestTimeoutMillis(int timeout) {
            this.mxBuilder.withRequestTimeoutMillis(timeout);
            return this;
        }

        /**
         * The max times will retry when the request came across the RetryExceptions.
         */
        public Builder withMaxRetryAttempts(int maxRetryAttempts) {
            this.mxBuilder.withMaxRetryAttempts(maxRetryAttempts);
            return this;
        }

        /**
         * The duration(milliseconds) of each retry.
         */
        public Builder withRetryWaitDurationMillis(int waitRetryDurationLimitation) {
            this.mxBuilder.withRetryWaitDurationMillis(waitRetryDurationLimitation);
            return this;
        }

        public Builder withDropAll(boolean drop) {
            this.mxBuilder.withDropAll(drop);
            return this;
        }

        public Builder withRequestType(RequestType type) {
            this.mxBuilder.withRequestType(type);
            return this;
        }

        public Builder withMaxRequestQueued(int maxRequestQueued) {
            this.mxBuilder.withMaxQueuedConn(maxRequestQueued);
            return this;
        }

        /**
         * Configures whether enable CircuitBreaker.Under certain circumstance
         * <p>
         * (e.g. the failure rate is equal to or greater than the threshold),
         * the CircuitBreaker transitions to open and starts short-circuiting calls.
         */
        public Builder withCircuitBreaker() {
            this.mxBuilder.circuitBreakerConfig.setEnable();
            return this;
        }

        /**
         * Configures the minimum number of calls which are required (per sliding window period)
         * before the CircuitBreaker can calculate the error rate. For example, if
         * minimumNumberOfCalls is 10, then at least 10 calls must be recorded, before the failure
         * rate can be calculated. If only 9 calls have been recorded, the CircuitBreaker will not
         * transition to open, even if all 9 calls have failed.
         * <p>
         * Default minimumNumberOfCalls is 10
         *
         * @param m the minimum number of calls that must be recorded before the
         *          failure rate can be calculated.
         * @return the Builder
         * @throws IllegalArgumentException if {@code m < 1}
         */
        public Builder withMinimumNumberOfCalls(int m) throws IllegalArgumentException {
            this.mxBuilder.circuitBreakerConfig.setMinimumNumberOfCalls(m);
            return this;
        }

        /**
         * Configures the size of the sliding window which is used to record the outcome of calls
         * when the CircuitBreaker is closed. slidingWindowSize configures the size of the
         * sliding window.
         * <p>
         * The slidingWindowSize must be greater than 0.
         * <p>
         * Default slidingWindowSize is 100.
         *
         * @param w the size of the sliding window when the CircuitBreaker is closed.
         * @return the Builder
         * @throws IllegalArgumentException if {@code w < 1}
         */
        public Builder withSlidingWindowSize(int w) throws IllegalArgumentException {
            this.mxBuilder.circuitBreakerConfig.setSlidingWindowSize(w);
            return this;
        }

        /**
         * Configures the failure rate threshold in percentage. If the failure rate is equal to or
         * greater than the threshold, the CircuitBreaker transitions to open and starts
         * short-circuiting calls.
         * <p>
         * The threshold must be greater than 0 and not greater than 100.
         * <p>
         * Default value is 50 percentage.
         *
         * @param t the failure rate threshold in percentage
         * @return the Builder
         * @throws IllegalArgumentException if {@code t <= 0 || t > 100}
         */
        public Builder withFailureRateThreshold(float t) {
            this.mxBuilder.circuitBreakerConfig.setFailureRateThreshold(t);
            return this;
        }

        /**
         * Configures the duration threshold above which calls are considered as slow and increase
         * the slow calls percentage.
         * <p>
         * The threshold must be greater than 0 and not greater than 100.
         * <p>
         * Default value is 60000
         *
         * @param m the duration above which calls are considered as slow
         * @return the Builder
         * @throws IllegalArgumentException if {@code m < 1}
         */
        public Builder withSlowCallDurationThresholdMillis(int m) throws IllegalArgumentException {
            this.mxBuilder.circuitBreakerConfig.setSlowCallDurationThresholdMillis(m);
            return this;
        }

        /**
         * Configures a threshold in percentage. The CircuitBreaker considers a call as slow when
         * the call duration is greater than slowCallDurationThreshold. When the
         * percentage of slow calls is equal to or greater than the threshold, the CircuitBreaker
         * transitions to open and starts short-circuiting calls.
         *
         * <p>
         * The threshold must be greater than 0 and not greater than 100.
         * <p>
         * Default value is 100.
         *
         * @param t the slow calls threshold in percentage
         * @return the Builder
         * @throws IllegalArgumentException if {@code t <= 0 || t > 100}
         */
        public Builder withSlowCallRateThreshold(float t) throws IllegalArgumentException {
            this.mxBuilder.circuitBreakerConfig.setSlowCallRateThreshold(t);
            return this;
        }

        /**
         * Use async request(HTTP or gRPC) to send data to backend server.
         * @param requestAsync
         * @return
         */
        public Builder withRequestAsync(boolean requestAsync) {
            this.mxBuilder.withRequestAsync(requestAsync);
            return this;
        }

        /**
         * CSV Construction parallel level. By default it == Runtime.availableProcessors.
         * @param parallel
         * @return
         */
        public Builder withCSVConstructionParallel(int parallel) {
            this.mxBuilder.withCSVConstructionParallel(parallel);
            return this;
        }

    }
}
