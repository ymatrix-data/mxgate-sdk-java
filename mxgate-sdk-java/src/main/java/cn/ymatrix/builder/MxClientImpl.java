package cn.ymatrix.builder;

import cn.ymatrix.api.ColumnMetaWrapper;
import cn.ymatrix.api.JobMetadataWrapper;
import cn.ymatrix.api.StatusCode;
import cn.ymatrix.apiclient.DataPostListener;
import cn.ymatrix.apiclient.MxClient;
import cn.ymatrix.apiclient.Result;
import cn.ymatrix.apiclient.ResultStatus;
import cn.ymatrix.apiserver.MxServer;
import cn.ymatrix.apiserver.SendDataResult;
import cn.ymatrix.cache.Cache;
import cn.ymatrix.concurrencycontrol.WorkerPool;
import cn.ymatrix.concurrencycontrol.WorkerPoolFactory;
import cn.ymatrix.data.*;
import cn.ymatrix.exception.*;
import cn.ymatrix.faulttolerance.CircuitBreakerFactory;
import cn.ymatrix.logger.MxLogger;
import cn.ymatrix.messagecenter.ResultMessageCenter;
import cn.ymatrix.messagecenter.ResultMessageQueue;
import cn.ymatrix.objectpool.TupleCreator;
import cn.ymatrix.objectpool.TuplePool;
import cn.ymatrix.objectpool.TuplesCreator;
import cn.ymatrix.objectpool.TuplesPool;
import cn.ymatrix.utils.StrUtil;
import org.slf4j.Logger;

import javax.management.InvalidAttributeValueException;
import java.security.InvalidParameterException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The implementation of MXClient
 * This client instance is not thread safe.
 */
class MxClientImpl implements MxClient {
    private static final String TAG = StrUtil.logTagWrap(MxClientImpl.class.getName());
    private static final long DEFAULT_FLUSH_BYTES = 4194304; // 4MB
    private static final int DEFAULT_FLUSH_INTERVAL_MILLIS = 2000; // 2seconds
    private static final int DEFAULT_CLOSE_WAIT_TIME_MILLIS = 2000;
    private static final int DEFAULT_SCHEDULED_FLUSH_TASK_DELAY = 2000;
    private static final int DEFAULT_CSV_CONSTRUCTION_BATCH_SIZE = 2000;
    private static final Logger l = MxLogger.init(MxClientImpl.class);

    /**
     * Cache which is used to save tuples.
     */
    private final Cache cache;
    private final MxServer server;
    private final ClientConfig clientConfig;
    private final HTTPConfig httpConfig;
    private JobMetadataWrapper metadataWrapper;
    // Column attribute name -> IndexJobMetadata
    private final Map<String, ColumnMetadata> columnMetaMap;
    // The delimiter to separate the columns in CSV file.
    private String delimiter;
    private DataPostListener dataPostListener;

    /**
     * The bytes size to accumulate for flush into the cache.
     */
    private long bytesToFlushLimitation;

    /**
     * Flush interval.
     */
    private long flushIntervalMillis;

    /**
     * Temp tuples buffer.
     */
    private final List<Tuple> tmpTuplesList;
    private final List<Tuple> tmpTuplesListBlocking;
    private Timer timer;
    private long accumulatedTuplesBytes;
    private final AtomicBoolean close;
    private final int serialNumber;
    private ResultMessageQueue<Result> queue;
    private ResultMessageRegister messageRegister;
    private final String clientName;
    private int enoughLinesToFlush;
    private TupleCreator tupleCreator;
    private TuplesCreator tuplesCreator;
    private TuplePool tuplePool;
    private TuplesPool tuplesPool;
    private boolean compressTuples;

    // Whether to encode the compressed bytes with base64
    // for the correction of conversion from compressed bytes to string.
    private boolean withBase64CompressEncode;

    private int objPoolSize;

    // Split one Tuples into some batch of sub Tuples and do CSV construction
    // concurrently, to increase the performance.
    private int csvConstructionBatchSize = DEFAULT_CSV_CONSTRUCTION_BATCH_SIZE;


    /**
     * @param serialNumber Unique serial number.
     * @param clientConfig
     * @param httpConfig
     * @param cache
     * @param server
     * @throws NullPointerException
     * @throws InvalidParameterException
     */
    MxClientImpl(int serialNumber, ClientConfig clientConfig, HTTPConfig httpConfig, Cache cache, MxServer server)
            throws NullPointerException, InvalidParameterException {
        if (serialNumber <= 0) {
            throw new InvalidParameterException("invalid serial number " + serialNumber);
        }

        if (clientConfig == null) {
            throw new NullPointerException("create MxClient on a null ClientConfig");
        }

        if (StrUtil.isNullOrEmpty(clientConfig.getSchema())) {
            throw new InvalidParameterException("create MxClient on an empty schema");
        }

        if (StrUtil.isNullOrEmpty(clientConfig.getTable())) {
            throw new InvalidParameterException("create MxClient on an empty table");
        }

        if (httpConfig == null) {
            throw new NullPointerException("create MxClient on an empty HTTPConfig");
        }

        if (StrUtil.isNullOrEmpty(httpConfig.getServerURLGRPC())) {
            throw new NullPointerException("create MxClient on an empty server url gRPC");
        }

        if (StrUtil.isNullOrEmpty(httpConfig.getServerURLDataSending())) {
            throw new NullPointerException("create MxClient on an empty server url for data sending");
        }

        if (cache == null) {
            throw new NullPointerException("create MxClient on an null cache");
        }

        if (server == null) {
            throw new NullPointerException("create MxClient on an null server");
        }
        this.serialNumber = serialNumber;

        this.clientConfig = clientConfig;

        this.httpConfig = httpConfig;

        this.cache = cache;

        this.server = server;

        // Renew tmp tuples.
        this.tmpTuplesList = new ArrayList<>();

        this.tmpTuplesListBlocking = new ArrayList<>();

        // This map will be read by multiple threads.
        // However, it is only read, so for the performance, we will not use ConcurrentHashMap.
        this.columnMetaMap = new HashMap<>();

        this.bytesToFlushLimitation = DEFAULT_FLUSH_BYTES;

        this.flushIntervalMillis = DEFAULT_FLUSH_INTERVAL_MILLIS;

        this.accumulatedTuplesBytes = 0;

        this.close = new AtomicBoolean(false);

        this.clientName = generateClientName();
    }

    private String generateClientName() {
        return StrUtil.connect(this.clientConfig.getSchema(), ".", this.clientConfig.getTable(), "#", String.valueOf(this.serialNumber));
    }

    @Override
    public String getClientName() {
        return this.clientName;
    }

    Cache getCache() {
        return this.cache;
    }

    MxServer getServer() {
        return this.server;
    }

    int getSerialNumber() {
        return this.serialNumber;
    }

    /**
     * Before unregister from the result message queue,
     * we will wait (request_timeout + retry_wait_duration) * max_retry_attempts
     * to make sure the last flush batch's result can be received from the message queue.
     */
    private int getCloseWaitTimeMillis() {
        if (this.httpConfig != null) {
            int waitTimeMillis = (this.httpConfig.getRequestTimeoutMillis() + this.httpConfig.getWaitRetryDurationLimitation()) * this.httpConfig.getMaxRetryAttempts();
            return waitTimeMillis > 0 ? waitTimeMillis : DEFAULT_CLOSE_WAIT_TIME_MILLIS;
        }
        return DEFAULT_CLOSE_WAIT_TIME_MILLIS;
    }

    /**
     * Tuples will be flushed at regular interval.
     */
    private void runScheduledFlushTask() {
        // Cancel the old timer.
        if (timer != null) {
            timer.cancel();
        }
        timer = new Timer();
        // After 2000 milliseconds this schedule will begin.
        timer.schedule(new ScheduledFlushTask(this), DEFAULT_SCHEDULED_FLUSH_TASK_DELAY, this.flushIntervalMillis);
        l.info("{} Start scheduled flush task with interval(ms) = {} .", TAG, this.flushIntervalMillis);
    }

    private void stopScheduledFlushTask() {
        if (timer != null) {
            timer.cancel();
            l.info("{} Stop scheduled flush task with interval(ms) = {} .", TAG, this.flushIntervalMillis);
        }
    }

    void prepareMetadata(JobMetadataWrapper metadataWrapper) throws InvalidAttributeValueException {
        this.metadataWrapper = metadataWrapper;
        List<ColumnMetaWrapper> list = this.metadataWrapper.getColumnMetaList();
        if (list.size() == 0) {
            throw new InvalidAttributeValueException("metadata column list is empty");
        }
        // Construct a columns metadata map.
        int size = list.size();
        for (int i = 0; i < size; i++) {
            if (list.get(i) != null) {
                this.columnMetaMap.put(list.get(i).getName(), new ColumnMetadata(list.get(i), i));
                l.debug("{} ColumnMeta: index = {} -> column_name = {} column_type = {} .", TAG, i, list.get(i).getName(), list.get(i).getType());
            }
        }
        // Set delimiter.
        String delimiter = metadataWrapper.getDelimiter();
        if (delimiter.isEmpty()) {
            throw new InvalidAttributeValueException("CSV file delimiter is empty");
        }
        this.delimiter = metadataWrapper.getDelimiter();
        l.info("{} Load metadata for MxClient({})", TAG, this.clientName);
    }

    private void initTuplePool(int size) {
        this.tupleCreator = new TupleCreator() {
            @Override
            public Tuple createTuple() {
                return MxClientImpl.this.newTuple();
            }
        };
        this.tuplePool = new TuplePool(this.tupleCreator, size);
        l.info("{} Init tuple pool with size = {} in {}", TAG, size, this.generateClientName());
    }

    private Tuple newTuple() {
        return new TupleImpl(MxClientImpl.this.columnMetaMap, MxClientImpl.this.delimiter,
                MxClientImpl.this.clientConfig.getSchema(), MxClientImpl.this.clientConfig.getTable());
    }

    private void initTuplesPool(int size) {
        this.tuplesCreator = new TuplesCreator() {
            @Override
            public Tuples createTuples() {
                return MxClientImpl.this.generateTuples();
            }
        };
        this.tuplesPool = new TuplesPool(this.tuplesCreator, size);
    }

    private Tuples getTuples() {
        if (this.tuplesPool != null) {
            return this.tuplesPool.tuplesBorrow();
        }
        return generateTuples();
    }

    private void returnTuples(Tuples tuples) {
        if (this.tuplesPool != null) {
            this.tuplesPool.tuplesReturn(tuples);
        }
        // If the TuplesPool is null, then this Tuples will be GC.
    }

    private Tuple getTuple() {
        if (this.tuplePool != null) {
            return this.tuplePool.tupleBorrow();
        }
        return newTuple();
    }

    private void returnTuple(Tuple tuple) {
        if (this.tuplePool != null) {
            this.tuplePool.tupleReturn(tuple);
        }
        // If the TuplePool is null, then this Tuple will be GC.
    }

    /**
     * Generate tuples with max tuples sync.
     */
    private Tuples generateTuples() {
        TuplesImpl tuples = new TuplesImpl(this.clientConfig.getSchema(), this.clientConfig.getTable(), this.csvConstructionBatchSize);
        TuplesTarget target = new TuplesTarget();
        target.setURL(this.httpConfig.getServerURLDataSending());
        target.setTimeout(this.httpConfig.getRequestTimeoutMillis());
        tuples.setTarget(target);
        tuples.setSenderID(this.generateClientName());
        tuples.setDelimiter(this.delimiter);
        tuples.setCompress(this.compressTuples);
        tuples.setBase64Encoding4CompressedBytes(this.withBase64CompressEncode);
        return tuples;
    }

    @Override
    public void appendTuple(final Tuple tuple) throws NullPointerException, ClientClosedException, IllegalStateException {
        throwClientClosedExceptionIfRequest();
        if (tuple == null) {
            throw new NullPointerException("Tuple is null for append.");
        }
        checkCircuitBreaker();
        try {
            // Readiness check.
            tuple.readinessCheck();
            add(tuple);
        } catch (TupleNotReadyException e) {
            l.error("{} Tuple is not ready in client appendTuple of table: {}.{}", TAG, this.clientConfig.getSchema(), this.clientConfig.getTable(), e);
            if (this.queue != null) {
                String msg = "tuple is not ready in client appendTuple of table "
                        + this.clientConfig.getSchema()
                        + this.clientConfig.getTable();
                Result result = new Result();
                result.setStatus(ResultStatus.FAILURE);
                Map<Tuple, String> map = new HashMap<>();
                map.put(tuple, e.getMessage());
                result.setMsg(msg);
                result.setErrorTuplesMap(map);
                this.queue.add(result);
            }
        }
    }

    @Override
    public void appendTuples(final Tuple... tuples) throws NullPointerException, ClientClosedException, IllegalStateException {
        appendTuplesList(Arrays.asList(tuples));
    }

    @Override
    public void appendTuplesList(final List<Tuple> tuples) throws NullPointerException, TupleNotReadyException, ClientClosedException, IllegalStateException {
        throwClientClosedExceptionIfRequest();
        if (tuples == null) {
            throw new NullPointerException("Tuples list is null");
        }
        checkCircuitBreaker();

        // Declare broken tuples container and result.
        Map<Tuple, String> map = null;
        for (Tuple tuple : tuples) {
            if (tuple == null) {
                l.error("{} Skip an null tuple in client appendTuplesList of table: {}.{}", TAG, this.clientConfig.getSchema(), this.clientConfig.getTable());
                continue;
            }
            // Readiness check.
            try {
                tuple.readinessCheck();
                add(tuple);
            } catch (TupleNotReadyException e) {
                l.error("{} Tuple is not ready in client appendTuplesList of table: {}.{}", TAG, this.clientConfig.getSchema(), this.clientConfig.getTable(), e);
                // Add broken tuple into map.
                if (this.queue != null) {
                    if (map == null) {
                        map = new HashMap<>();
                    }
                    map.put(tuple, e.getMessage());
                }
            }
        }
        // Broken tuples callback.
        if (map != null && !map.isEmpty() && this.queue != null) {
            Result result = new Result();
            String msg = "Tuple is not ready in client appendTuplesList of table "
                    + this.clientConfig.getSchema()
                    + this.clientConfig.getTable();
            result.setErrorTuplesMap(map);
            result.setMsg(msg);
            result.setStatus(ResultStatus.FAILURE);
            this.queue.add(result);
        }
    }

    // For concurrency read and write of the tmpTuplesList,
    // this method is tagged with synchronized.
    private synchronized void add(Tuple tuple) throws NullPointerException, IllegalStateException {
        // We don't need to add nullable tuple.
        if (tuple == null) {
            return;
        }
        if (this.tmpTuplesList == null) {
            throw new NullPointerException("UNEXPECTED: MxClient tmp tuples list is null.");
        }
        // If set to with enough lines to flush,
        if (this.enoughLinesToFlush > 0) {
            addTupleWithEnoughLines(tuple);
            return;
        }
        if (this.bytesToFlushLimitation <= 0) {
            this.bytesToFlushLimitation = DEFAULT_FLUSH_BYTES;
        }
        String csvStr = tuple.toCSVLineStr();
        if (csvStr != null) {
            long tupleBytesSize = tuple.toCSVLineStr().getBytes().length;
            this.tmpTuplesList.add(tuple);
            this.accumulatedTuplesBytes += tupleBytesSize;
            l.debug("{} Accumulated bytes size {}", TAG, this.accumulatedTuplesBytes);
            if (this.accumulatedTuplesBytes >= this.bytesToFlushLimitation) {
                l.debug("{} Enough, flush {} bytes", TAG, this.accumulatedTuplesBytes);
                this.flush();
            }
        }
    }

    private void addTupleWithEnoughLines(Tuple tuple) {
        this.tmpTuplesList.add(tuple);
        if (this.tmpTuplesList.size() >= this.enoughLinesToFlush) {
            l.debug("{} Enough, flush {} lines", TAG, this.tmpTuplesList.size());
            this.flush();
        }
    }

    private boolean addTupleWithEnoughLinesBlocking(Tuple tuple) {
        this.tmpTuplesListBlocking.add(tuple);
        if (this.tmpTuplesListBlocking.size() >= this.enoughLinesToFlush) {
            l.debug("{}, Enough {} lines for flush blocking", TAG, this.tmpTuplesListBlocking.size());
            return true;
        }
        return false;
    }

    @Override
    public boolean appendTupleBlocking(Tuple tuple) throws NullPointerException, ClientClosedException, IllegalStateException {
        throwClientClosedExceptionIfRequest();
        if (tuple == null) {
            throw new NullPointerException("Tuple is null for append.");
        }
        checkCircuitBreaker();

        tuple.readinessCheck();
        return addBlocking(tuple);
    }

    @Override
    public boolean appendTuplesBlocking(Tuple... tuples) throws NullPointerException, ClientClosedException, IllegalStateException {
        return this.appendTuplesListBlocking(Arrays.asList(tuples));
    }

    @Override
    public boolean appendTuplesListBlocking(List<Tuple> tuples) throws NullPointerException, ClientClosedException, IllegalStateException {
        throwClientClosedExceptionIfRequest();
        if (tuples == null) {
            throw new NullPointerException("Tuples is null");
        }
        checkCircuitBreaker();

        // Declare broken tuples container and result.
        Map<Tuple, String> map = null;
        for (Tuple tuple : tuples) {
            if (tuple == null) {
                l.error("{} Skip an null tuple in client appendTuples of table: {}.{}", TAG, this.clientConfig.getSchema(), this.clientConfig.getTable());
                continue;
            }
            // Readiness check.
            try {
                tuple.readinessCheck();
            } catch (TupleNotReadyException e) {
                // Add broken tuple into map.
                if (map == null) {
                    map = new HashMap<>();
                }
                map.put(tuple, e.getMessage());
            }
        }

        if (map != null && !map.isEmpty()) {
            String msg = "There are some tuples not ready in client appendTuplesList of table "
                    + this.clientConfig.getSchema()
                    + this.clientConfig.getTable() + ":";
            for (Map.Entry<Tuple, String> entry : map.entrySet()) {
                msg = msg + entry.getKey().getRawInputString() + " -> " + entry.getValue();
            }
            l.error(msg);
            throw new TupleNotReadyException(msg);
        }

        boolean needFlush = false;
        for (Tuple tuple : tuples) {
            if (tuple == null) {
                l.error("{} Skip an null tuple in client appendTuples of table: {}.{}", TAG, this.clientConfig.getSchema(), this.clientConfig.getTable());
                continue;
            }
            needFlush = needFlush || addBlocking(tuple);
        }
        return needFlush;
    }

    private synchronized boolean addBlocking(Tuple tuple) throws NullPointerException, IllegalStateException {
        // We don't need to add nullable tuple.
        if (tuple == null) {
            return false;
        }

        if (this.tmpTuplesListBlocking == null) {
            throw new NullPointerException("UNEXPECTED: MxClient tmp tuples list is null.");
        }

        // If set to with enough lines to flush,
        if (this.enoughLinesToFlush > 0) {
            return addTupleWithEnoughLinesBlocking(tuple);
        }

        if (this.bytesToFlushLimitation <= 0) {
            this.bytesToFlushLimitation = DEFAULT_FLUSH_BYTES;
        }
        String csvStr = tuple.toCSVLineStr();
        if (csvStr != null) {
            long tupleBytesSize = csvStr.getBytes().length;
            this.tmpTuplesListBlocking.add(tuple);
            this.accumulatedTuplesBytes += tupleBytesSize;
            l.debug("{} Accumulated bytes size blocking {}", TAG, this.accumulatedTuplesBytes);
            if (this.accumulatedTuplesBytes >= this.bytesToFlushLimitation) {
                l.debug("{} Enough to flush {} bytes blocking mode", TAG, this.accumulatedTuplesBytes);
                return true;
            } else {
                return false;
            }
        }
        return false;
    }

    @Override
    public synchronized void flushBlocking() throws AllTuplesFailException, PartiallyTuplesFailException, ClientClosedException {
        throwClientClosedExceptionIfRequest();
        if (this.tmpTuplesListBlocking.isEmpty()) {
            l.debug("{} Temp tuples list is empty, no need to flush blocking mode.", TAG);
            return;
        }
        checkCircuitBreaker();
        Tuples tuples = this.getTuples();
        if (tuples == null) {
            l.error("{} Get null tuples container for flush blocking.", TAG);
            return;
        }
        tuples.appendTupleList(this.tmpTuplesListBlocking);
        SendDataResult result = null;
        try {
            result = this.server.sendDataBlocking(tuples);
        } catch (Exception e) {
            l.error("MxServer send data blocking NullPointerException ", e);
        }
        handleSendDataResult(result, tuples);

        if (this.objPoolSize > 0) {
            this.tuplesBack(tuples);
        }
        // Shrink tmp tuples.
        this.tmpTuplesListBlocking.clear();
        // Reset the accumulated tuples bytes size.
        this.accumulatedTuplesBytes = 0;
    }

    private void handleSendDataResult(SendDataResult result, Tuples tuples) throws AllTuplesFailException, PartiallyTuplesFailException {
        if (result == null) {
            AllTuplesFailException allTuplesFailException = new AllTuplesFailException("Get empty result from send data blocking.");
            Result failureResult = new Result();
            failureResult.setMsg("Get empty result from send data blocking.");
            failureResult.setErrorTuplesMap(TuplesConsumeResultConvertor.convertErrorTuples(null, tuples, "Get empty result from send data blocking."));
            failureResult.setSucceedLines(0); // All tuples are failed, no succeed lines.
            failureResult.setStatus(ResultStatus.FAILURE);
            allTuplesFailException.setResult(failureResult);
            // Shrink tmp tuples.
            this.tmpTuplesListBlocking.clear();
            // Reset the accumulated tuples bytes size.
            this.accumulatedTuplesBytes = 0;
            throw allTuplesFailException;
        }
        if (result.getCode() == StatusCode.ERROR || result.getCode() == StatusCode.ALL_TUPLES_FAIL) {
            AllTuplesFailException allTuplesFailException = new AllTuplesFailException(result.getMsg());
            Result failureResult = new Result();
            failureResult.setMsg(result.getMsg());
            failureResult.setErrorTuplesMap(TuplesConsumeResultConvertor.convertErrorTuples(null, tuples, result.getMsg()));
            failureResult.setSucceedLines(0); // All tuples are failed, no succeed lines.
            failureResult.setStatus(ResultStatus.FAILURE);
            allTuplesFailException.setResult(failureResult);
            // Shrink tmp tuples.
            this.tmpTuplesListBlocking.clear();
            // Reset the accumulated tuples bytes size.
            this.accumulatedTuplesBytes = 0;
            throw allTuplesFailException;
        } else if (result.getCode() == StatusCode.PARTIALLY_TUPLES_FAIL) {
            PartiallyTuplesFailException partiallyTuplesFailException = new PartiallyTuplesFailException(result.getMsg());
            Result failureResult = new Result();
            failureResult.setMsg(result.getMsg());
            failureResult.setErrorTuplesMap(TuplesConsumeResultConvertor.convertErrorTuples(result.getErrorLinesMap(), tuples, result.getMsg()));
            failureResult.setSucceedLines(TuplesConsumeResultConvertor.convertSucceedTuplesLines(result.getErrorLinesMap(), tuples));
            failureResult.setSuccessLinesSerialNumList(TuplesConsumeResultConvertor.convertSuccessfulSerialNums(result.getErrorLinesMap(), tuples));
            failureResult.setStatus(ResultStatus.FAILURE);
            partiallyTuplesFailException.setResult(failureResult);
            // Shrink tmp tuples.
            this.tmpTuplesListBlocking.clear();
            // Reset the accumulated tuples bytes size.
            this.accumulatedTuplesBytes = 0;
            throw partiallyTuplesFailException;
        }
    }

    private void tuplesBack(Tuples tuples) {
        // For blocking request,
        int tuplesSize = tuples.size();
        for (int i = 0; i < tuplesSize; i++) {
            Tuple tuple = tuples.getTupleByIndex(i);
            if (tuple != null) {
                tuple.reset();
                // Return the tuple back to the tuple object.
                this.returnTuple(tuple);
            }
        }
        tuples.reset();
        tuples.setCompress(this.compressTuples);
        tuples.setBase64Encoding4CompressedBytes(this.withBase64CompressEncode);
        // Return rawTuples back to the tuples object pool.
        this.returnTuples(tuples);
    }

    // Generate an empty tuple for client user.
    @Override
    public Tuple generateEmptyTuple() throws NullPointerException, IndexOutOfBoundsException, ClientClosedException {
        throwClientClosedExceptionIfRequest();
        return this.getTuple();
    }

    @Override
    public Tuple generateEmptyTupleLite() {
        return new TupleImplLite(this.delimiter, this.clientConfig.getSchema(), this.clientConfig.getTable());
    }

    private void throwClientClosedExceptionIfRequest() throws ClientClosedException {
        if (this.closed()) {
            throw new ClientClosedException("MxClient(" + generateClientName() + ") has been closed.");
        }
    }

    private void checkCircuitBreaker() throws ClientClosedException {
        CircuitBreakerConfig cbCfg = this.server.getCircuitBreakerConfig();
        if (cbCfg == null || !cbCfg.isEnable()) {
            return;
        }
        if (CircuitBreakerFactory.getInstance().isCircuitBreak(this.httpConfig.getServerURLDataSending(), this.clientConfig.getSchema(), this.clientConfig.getTable())) {
            throw new IllegalStateException(String.format("ATTENTION: Circuit has broken because too many failures(%.2f%%) or slow calls(%.2f%%) from MxGate." +
                            " Cannot add tuple now, and will recover atomically when response of MxGate back to normal.",
                    CircuitBreakerFactory.getInstance().getFailureRate(this.httpConfig.getServerURLDataSending(), this.clientConfig.getSchema(), this.clientConfig.getTable()),
                    CircuitBreakerFactory.getInstance().getSlowCallRate(this.httpConfig.getServerURLDataSending(), this.clientConfig.getSchema(), this.clientConfig.getTable())));
        }
    }

    /**
     * Flush will be invoked both in scheduler timer's thread pool and the SDK users manually.
     * So it is synchronized.
     */
    @Override
    public synchronized void flush() throws ClientClosedException {
        throwClientClosedExceptionIfRequest();
        if (this.tmpTuplesList.isEmpty()) {
            l.debug("{} Temp tuples list is empty, no need to flush.", TAG);
            return;
        }
        Tuples tuples = this.getTuples();
        tuples.appendTupleList(this.tmpTuplesList);
        try {
            if(!this.cache.offer(tuples)) {
                throw new EnqueueException("Enqueue timeout");
            }
        } catch (Exception e) {
            // Send the fail tuples with callback.
            String msg = tuples.size() + " Tuples are refused to enqueue and this cache size is " + cache.size();
            l.error("{} {}", TAG, msg);
            if (this.dataPostListener != null) {
                Result failureResult = new Result();
                failureResult.setMsg(msg);
                failureResult.setErrorTuplesMap(TuplesConsumeResultConvertor.convertErrorTuples(null, tuples, msg));
                failureResult.setSucceedLines(0); // All tuples are failed, no succeed lines.
                failureResult.setStatus(ResultStatus.FAILURE);
                this.dataPostListener.onFailure(failureResult);
            }
        }
        // Shrink tmp tuples.
        this.tmpTuplesList.clear();
        // Reset the accumulated tuples bytes size.
        this.accumulatedTuplesBytes = 0;
    }

    @Override
    public synchronized void registerDataPostListener(DataPostListener listener) throws NullPointerException, InvalidParameterException, ClientClosedException {
        throwClientClosedExceptionIfRequest();
        if (this.dataPostListener != null) {
            throw new InvalidParameterException("DataPostListener has already been registered.");
        }
        if (listener == null) {
            throw new NullPointerException("DataPostListener for register is null.");
        }
        this.dataPostListener = listener;
        l.info("{} Register DataPostListener for MxClient({})", TAG, this.clientName);
        registerMessageQueue();
    }

    private synchronized void registerMessageQueue() {
        // The result message already has been registered.
        if (messageRegister != null && queue != null) {
            return;
        }
        messageRegister = new ResultMessageRegister();
        queue = ResultMessageCenter.getSingleInstance().register(this.generateClientName());
        messageRegister.register(queue);
        l.info("{} Register MessageQueue from MessageCenter for MxClient({}).", TAG, this.clientName);
    }

    /**
     * Unregister the message queue from message center.
     */
    private synchronized void unRegisterMessageQueue() {
        if (messageRegister != null && queue != null) {
            ResultMessageCenter.getSingleInstance().unRegister(this.generateClientName());
            messageRegister.unRegister();
            l.info("{} Unregister MessageQueue from MessageCenter for MxClient({}).", TAG, this.clientName);
        }
    }

    @Override
    public synchronized void withEnoughBytesToFlush(long bytesLimitation) throws InvalidParameterException, ClientClosedException {
        throwClientClosedExceptionIfRequest();
        if (bytesLimitation <= 0) {
            throw new InvalidParameterException("invalid flush bytes limitation: " + bytesLimitation);
        }
        this.bytesToFlushLimitation = bytesLimitation;
    }

    @Override
    public synchronized void withIntervalToFlushMillis(long intervalMillis) throws InvalidParameterException, ClientClosedException {
        throwClientClosedExceptionIfRequest();
        if (intervalMillis <= 0) {
            throw new InvalidParameterException("invalid flush interval: " + intervalMillis);
        }
        this.flushIntervalMillis = intervalMillis;
        // Reschedule the timer.
        this.runScheduledFlushTask();
    }

    @Override
    public synchronized void withEnoughLinesToFlush(int lines) {
        if (lines <= 0) {
            throw new InvalidParameterException("invalid flush lines limitation: " + lines);
        }
        this.enoughLinesToFlush = lines;
    }

    @Override
    public void withCSVConstructionBatchSize(int size) {
        if (size <= 0) {
            throw new InvalidParameterException("invalid CSV construction batch size: " + size);
        }
        this.csvConstructionBatchSize = size;
    }

    @Override
    public synchronized void useTuplesPool(int size) {
        if (size <= 0) {
            throw new InvalidParameterException("invalid tuples pool size: " + size);
        }
        this.objPoolSize = size;
        initTuplePool(this.objPoolSize);
        initTuplesPool(this.objPoolSize);
    }

    @Override
    public void setDelimiter(String delimiter) {
        this.delimiter = delimiter;
    }

    @Override
    public String getDelimiter() {
        return this.delimiter;
    }

    @Override
    public void setSchema(String schema) {
        this.clientConfig.setSchema(schema);
    }

    @Override
    public String getSchema() {
        return this.clientConfig.getSchema();
    }

    @Override
    public void setTable(String table) {
        this.clientConfig.setTable(table);
    }

    @Override
    public String getTable() {
        return this.clientConfig.getTable();
    }

    @Override
    public synchronized void withCompress() {
        this.compressTuples = true;
    }

    @Override
    public synchronized void withoutCompress() {
        this.compressTuples = false;
    }

    @Override
    public void withBase64Encode4Compress() {
        this.withBase64CompressEncode = true;
    }

    @Override
    public void withoutBase64EncodeCompress() {
        this.withBase64CompressEncode = false;
    }

    /**
     * Before unregister from the result message queue,
     * we will wait (request_timeout + retry_wait_duration) * max_retry_attempts
     * to make sure the last flush batch's result can be received from the message queue.
     */
    protected synchronized void close() throws ClientClosedException, InterruptedException, IllegalStateException {
        l.info("{} Begin to close client({})", TAG, this.clientName);
        throwClientClosedExceptionIfRequest();
        this.flush();
        this.close.set(true);
        stopScheduledFlushTask();
        // Flush last bytes into the cache.
        threadWait(getCloseWaitTimeMillis());
        unRegisterMessageQueue();
        l.info("{} Client({}) is closed.", TAG, this.clientName);
    }

    private void threadWait(int waitTimeMillis) throws InterruptedException {
        Thread.sleep(waitTimeMillis);
    }

    /**
     * This method maybe is used by pause and resume of MxBuilder.
     *
     * @param closed
     */
    protected void setClosedState(boolean closed) {
        this.close.set(closed);
    }

    protected boolean closed() {
        return this.close.get();
    }

    /**
     * This method is used for test right now.
     *
     * @return
     */
    private boolean isFree() {
        return this.tmpTuplesList.isEmpty() && this.cache.isEmpty();
    }

    private static class ScheduledFlushTask extends TimerTask {
        private final MxClient client;

        ScheduledFlushTask(MxClient client) {
            this.client = client;
        }

        @Override
        public void run() {
            if (this.client != null) {
                l.debug("{} Flush tuples into cache by timer scheduler.", TAG);
                try {
                    this.client.flush();
                } catch (IllegalStateException iE) {
                    l.error("{} Schedule flush task illegal-state exception: sending tuples too fast", TAG, iE);
                } catch (Exception e) {
                    l.error("{} Schedule flush task exception", TAG, e);
                }
            }
        }
    }

    private class ResultMessageRegister {
        private final WorkerPool singleWorkerPool;
        private final AtomicBoolean stop;

        private ResultMessageRegister() {
            this.singleWorkerPool = WorkerPoolFactory.initFixedSizeWorkerPool(1);
            this.stop = new AtomicBoolean(false);
        }

        private void unRegister() {
            this.stop.set(true);
            this.singleWorkerPool.shutdownNow();
        }

        private void register(ResultMessageQueue<Result> messageQueue) throws NullPointerException {
            if (messageQueue == null) {
                throw new NullPointerException("MessageQueue<Result> is null for register.");
            }
            if (MxClientImpl.this.dataPostListener == null) {
                l.error("{} DataPostListener is null, will not register Result message from the MessageCenter.", TAG);
                return;
            }
            try {
                this.singleWorkerPool.join(new Runnable() {
                    @Override
                    public void run() {
                        while (!ResultMessageRegister.this.stop.get() && messageQueue != null) {
                            try {
                                Result result = messageQueue.get();
                                if (result == null) {
                                    // Skip null result.
                                    l.warn("{} Get null Result form messageQueue skip.", TAG);
                                    continue;
                                }
                                if (result.getStatus() == null) {
                                    l.error("{} Get Result without status, will not callback to DataPostListener.", TAG);
                                    continue;
                                }
                                if (MxClientImpl.this.dataPostListener != null) {
                                    if (result.getStatus() == ResultStatus.SUCCESS) {
                                        MxClientImpl.this.dataPostListener.onSuccess(result);
                                    } else {
                                        MxClientImpl.this.dataPostListener.onFailure(result);
                                    }
                                }
                                // For async request, the result will contain raw tuples.
                                Tuples rawTuples = result.getRawTuples();
                                if (rawTuples != null && MxClientImpl.this.objPoolSize > 0) {
                                    MxClientImpl.this.tuplesBack(rawTuples);
                                }
                            } catch (Exception e) {
                                e.printStackTrace();
                                l.error("{} Consume message from ResultMessageQueue exception: {}", TAG, e);
                            }
                        }
                    }
                });
            } catch (Exception e) {
                l.error("{} singleWorkerPool.join exception", TAG, e);
            }
        }
    }

    /**
     * The implementation of Tuples
     */
    private static class TuplesImpl implements Tuples {
        // Tuples container.
        private final List<Tuple> tuples;
        private String schema;
        private String table;
        private TuplesTarget target;
        private String senderID;
        private String delimiter;
        private boolean needCompress;
        private boolean needBase64Encoding4CompressedBytes;
        private final int csvBatchSize;

        TuplesImpl(String schema, String table, int csvBatchSize) throws InvalidParameterException {
            if (csvBatchSize <= 0) {
                throw new InvalidParameterException("CSV Batch Size is invalid (must be positive) " + csvBatchSize);
            }
            this.tuples = new ArrayList<>();
            this.schema = schema;
            this.table = table;
            this.csvBatchSize = csvBatchSize;
        }

        @Override
        public void append(Tuple tuple) {
            this.tuples.add(tuple);
        }

        @Override
        public void appendTuples(Tuple... tuples) {
            Collections.addAll(this.tuples, tuples);
        }

        @Override
        public void appendTupleList(List<Tuple> tupleList) {
            this.tuples.addAll(tupleList);
        }

        @Override
        public List<Tuple> getTuplesList() {
            return tuples;
        }

        @Override
        public int size() {
            return this.tuples.size();
        }

        @Override
        public void setSchema(String schema) {
            this.schema = schema;
        }

        @Override
        public void setTable(String table) {
            this.table = table;
        }

        @Override
        public String getSchema() {
            return this.schema;
        }

        @Override
        public String getTable() {
            return this.table;
        }

        @Override
        public void setTarget(TuplesTarget target) {
            this.target = target;
        }

        @Override
        public TuplesTarget getTarget() {
            return this.target;
        }

        @Override
        public Tuple getTupleByIndex(int index) {
            if (index >= this.size() || index < 0) {
                return null;
            }
            return this.tuples.get(index);
        }

        @Override
        public void setSenderID(String senderID) {
            this.senderID = senderID;
        }

        @Override
        public String getSenderID() {
            return this.senderID;
        }

        @Override
        public boolean isEmpty() {
            return this.tuples.isEmpty();
        }

        @Override
        public void reset() {
            this.tuples.clear();
        }

        @Override
        public void setDelimiter(String delimiter) {
            this.delimiter = delimiter;
        }

        @Override
        public String getDelimiter() {
            return this.delimiter;
        }

        @Override
        public boolean needCompress() {
            return this.needCompress;
        }

        @Override
        public synchronized void setCompress(boolean compress) {
            this.needCompress = compress;
        }

        @Override
        public boolean needBase64Encoding4CompressedBytes() {
            return this.needBase64Encoding4CompressedBytes;
        }

        @Override
        public void setBase64Encoding4CompressedBytes(boolean base64Encoding) {
            this.needBase64Encoding4CompressedBytes = base64Encoding;
        }

        @Override
        public int getCSVBatchSize() {
            return this.csvBatchSize;
        }
    }
}
