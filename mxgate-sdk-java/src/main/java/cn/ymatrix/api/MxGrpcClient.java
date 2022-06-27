package cn.ymatrix.api;

import cn.ymatrix.apiserver.GetJobMetadataListener;
import cn.ymatrix.apiserver.SendDataListener;
import cn.ymatrix.apiserver.SendDataResult;
import cn.ymatrix.builder.MxBuilder;
import cn.ymatrix.compress.CompressionFactory;
import cn.ymatrix.data.Tuples;
import cn.ymatrix.exception.CircuitBreakException;
import cn.ymatrix.exception.RetryException;
import cn.ymatrix.faulttolerance.CircuitBreakerFactory;
import cn.ymatrix.faulttolerance.RetryStatistic;
import cn.ymatrix.logger.MxLogger;
import cn.ymatrix.utils.CSVConstructor;
import cn.ymatrix.utils.StrUtil;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.grpc.*;
import io.grpc.stub.StreamObserver;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import org.slf4j.Logger;

import java.security.InvalidParameterException;
import java.util.concurrent.TimeUnit;

public class MxGrpcClient {
    private static final String TAG = StrUtil.logTagWrap(MxGrpcClient.class.getName()) + MxBuilder.SDK_VERSION;
    private static final Logger l = MxLogger.init(MxGrpcClient.class);
    public static final int GRPC_RESPONSE_CODE_OK = 0;
    public static final int GRPC_RESPONSE_CODE_ERROR = 1;
    public static final int GRPC_RESPONSE_INVALID_BROKERS = 2;
    public static final int GRPC_RESPONSE_TOPIC_NOT_FOUND = 3;
    public static final int GRPC_RESPONSE_CODE_INVALID_REQUEST = 4;
    public static final int GRPC_RESPONSE_CODE_INVALID_CONFIG = 5;
    public static final int GRPC_RESPONSE_CODE_INVALID_TABLE = 6;
    public static final int GRPC_RESPONSE_CODE_MXGATE_NOT_READY = 7;
    public static final int GRPC_RESPONSE_CODE_TIMEOUT = 8; // MxGate is timeout internally.
    public static final int GRPC_RESPONSE_CODE_ALL_TUPLES_FAILED = 9;
    public static final int GRPC_RESPONSE_CODE_ALL_UNDEFINED_ERROR = 10;


    /**
     * encoding_method         = "encoding-method"
     * compressed_bytes_encode = "bytes-encoding"
     */
    private static final String EncodingMethodKey = "encoding-method";
    private static final String EncodingMethodValue = "zstd";
    private static final String CompressedBytesEncodingKey = "bytes-encoding";
    private static final String CompressedBytesEncodingValue = "Base64";
    private final MXGateGrpc.MXGateBlockingStub blockingStub;
    private final MXGateGrpc.MXGateStub asyncStub;
    private final String schema;
    private final String table;
    private final ManagedChannel channel;
    private final int timeoutMillis;
    private final Context.CancellableContext cancellableContext;
    private CircuitBreaker circuitBreaker;
    private final SendDataInterceptor sendDataInterceptor;
    private cn.ymatrix.compress.Compressor compressor;
    private final CSVConstructor constructor;

    public static MxGrpcClient prepareMxGrpcClient(String target, String schema, String table, int timeoutMillis,
                                                   cn.ymatrix.builder.CircuitBreakerConfig circuitBreakerConfig, CSVConstructor constructor) throws NullPointerException {
        if (StrUtil.isNullOrEmpty(target)) {
            throw new NullPointerException("target is null when try to create MxGrpcClient");
        }
        if (StrUtil.isNullOrEmpty(schema)) {
            throw new NullPointerException("schema is null when try to create MxGrpcClient");
        }
        if (StrUtil.isNullOrEmpty(table)) {
            throw new NullPointerException("table is null when try to create MxGrpcClient");
        }
        return new MxGrpcClient(target, schema, table, timeoutMillis, circuitBreakerConfig, constructor);
    }

    MxGrpcClient(String targetURL, String schema, String table, int timeoutMillis,
                 cn.ymatrix.builder.CircuitBreakerConfig circuitBreakerConfig, CSVConstructor constructor) {
        this.sendDataInterceptor = new SendDataInterceptor();
        ManagedChannel channel = ManagedChannelBuilder.forTarget(targetURL).usePlaintext().intercept(sendDataInterceptor).build();
        l.info("{} Start MxGrpcClient on target = {} for table = {}.{} with timeout = {} ms.",
                TAG, targetURL, schema, table, timeoutMillis);
        this.blockingStub = MXGateGrpc.newBlockingStub(channel)
                .withMaxInboundMessageSize(Integer.MAX_VALUE)
                .withMaxOutboundMessageSize(Integer.MAX_VALUE);
        this.asyncStub = MXGateGrpc.newStub(channel)
                .withMaxInboundMessageSize(Integer.MAX_VALUE)
                .withMaxOutboundMessageSize(Integer.MAX_VALUE);
        this.channel = channel;
        this.schema = schema;
        this.table = table;
        this.cancellableContext = Context.current().withCancellation();
        this.timeoutMillis = timeoutMillis;
        this.constructor = constructor;
        if (circuitBreakerConfig != null) {
            // Ignore the retry exceptions.
            circuitBreakerConfig.setIgnoredExceptions(RetryException.class);
            this.circuitBreaker = CircuitBreakerFactory.getInstance()
                    .prepareCircuitBreaker(targetURL, schema, table, circuitBreakerConfig);
        }
    }

    /**
     * Get job metadata sync.
     */
    public void getJobMetadataBlocking(final GetJobMetadataListener listener, RetryStatistic rs) throws NullPointerException, RetryException {
        if (listener == null) {
            throw new NullPointerException("ConnectionListener callback is null while try to get job metadata.");
        }
        try {
            l.info("{} Get Metadata from mxgate backend service for table {}.{}", TAG, this.schema, this.table);
            // Prepare request.
            GetJobMetadata.Request request = GetJobMetadata.Request.newBuilder().setSchema(this.schema).setTable(this.table).build();
            GetJobMetadata.Response response;
            // Get job metadata sync.
            if (this.timeoutMillis > 0) {
                response = blockingStub.withDeadlineAfter(this.timeoutMillis, TimeUnit.MILLISECONDS).getJobMetadata(request);
            } else {
                response = blockingStub.getJobMetadata(request);
            }
            handleGetJobMetadataResponse(listener, response);
        } catch (Exception e) {
            rethrowExceptionGetJobMetadata(e, listener, rs);
        }
    }

    public void stop() {
        if (this.channel != null) {
            l.info("{} Stop MxGrpcClient for table {}.{}", TAG, this.schema, this.table);
            try {
                channel.shutdown().awaitTermination(this.timeoutMillis, TimeUnit.MILLISECONDS);
                l.debug("{} gRPC channel shutdown after get job metadata for table {}.{}, finally.",
                        TAG, MxGrpcClient.this.schema, MxGrpcClient.this.table);
            } catch (Exception e) {
                l.error("{} Channel shutdown error in get job metadata for table {}.{} : {}.",
                        TAG, e.getMessage(), MxGrpcClient.this.schema, MxGrpcClient.this.table);
                e.printStackTrace();
            }
        }
    }

    private void rethrowExceptionGetJobMetadata(Exception e, final GetJobMetadataListener listener, RetryStatistic rs)
            throws NullPointerException, RetryException {
        if (e == null) {
            throw new NullPointerException();
        }
        // No need to retry.
        if (e instanceof InvalidParameterException && e.getMessage().contains("could not be found in table")) {
            String errMsg = StrUtil.connect("get exception from get job metadata gRPC API for table ",
                    MxGrpcClient.this.schema, ".", MxGrpcClient.this.table, " ", e.getMessage());
            listener.onFailure(errMsg);
            if (cancellableContext != null) {
                cancellableContext.cancel(e);
            }
            return;
        }

        String errMsg = StrUtil.connect("Get exception from get job metadata gRPC API for table ",
                MxGrpcClient.this.schema, ".", MxGrpcClient.this.table, " (Retry) ", e.getMessage());
        l.error("{} {}", TAG, errMsg);

        // At each the retry exception throw, we will increase the retry statistic.
        if (rs != null && rs.increaseRetryTimes()) {
            l.debug("{} Actual retry time {} and reached", TAG, rs.actuallyRetryTimes());
            listener.onFailure(errMsg);
        }

        // For retry, without the retry statistic, we will call the listener callback at each time.
        if (rs == null) {
            listener.onFailure(errMsg);
        }

        throw new RetryException(e.getMessage());
    }

    public SendDataResult sendDataBlocking(final Tuples tuples, final RetryStatistic rs, boolean needCompress,
                                           boolean needBase64Encoding, final SendDataListener listener)
            throws NullPointerException, RetryException {
        if (tuples == null) {
            throw new NullPointerException("Tuples to be sent is nullable.");
        }

        if (this.constructor == null) {
            throw new NullPointerException("CSV Constructor is null for Data sending, please create it first.");
        }

        StringBuilder rawCSVData = constructor.constructCSVFromTuplesWithTasks(tuples.getTuplesList(), tuples.getCSVBatchSize(), tuples.getDelimiter());
        if (rawCSVData == null) {
            throw new NullPointerException("Get null CSV data from CSV constructor.");
        }

        String data = new String(rawCSVData.toString().getBytes(StandardCharsets.UTF_8), StandardCharsets.UTF_8);
        if (needCompress) {
            return this.sendData(tuples, compressData(data, needBase64Encoding), rs, listener);
        }
        this.sendDataInterceptor.removeMetadataKV(EncodingMethodKey);
        this.sendDataInterceptor.removeMetadataKV(CompressedBytesEncodingKey);
        return this.sendData(tuples, data, rs, listener);
    }

    private String compressData(String data, boolean needBase64Encoding) {
        this.sendDataInterceptor.setMetadataKV(EncodingMethodKey, EncodingMethodValue);
        this.sendDataInterceptor.setMetadataKV(CompressedBytesEncodingKey, CompressedBytesEncodingValue);
        if (this.compressor == null) {
            this.compressor = CompressionFactory.getCompressor();
        }
        byte[] compressedBytes = this.compressor.compress(data.getBytes(StandardCharsets.UTF_8));
        if (!needBase64Encoding) { // Send compressed bytes directly without base64 encoding.
            return new String(compressedBytes, StandardCharsets.UTF_8);
        }
        // We use base64 to encode the compressed bytes
        // to make sure the correctness of the conversion from bytes to string.
        return Base64.getEncoder().encodeToString(compressedBytes);
    }

    private SendDataResult sendData(final Tuples tuples, final String data, final RetryStatistic rs, final SendDataListener listener)
            throws NullPointerException, RetryException {
        if (this.circuitBreaker != null) {
            return this.wrapWithCircuitBreaker(tuples, data, rs, listener);
        }
        return this.sendDataBlockingCore(tuples, data, rs, listener);
    }

    private SendDataResult wrapWithCircuitBreaker(final Tuples tuples, final String data, final RetryStatistic rs, final SendDataListener listener)
            throws NullPointerException, RetryException {
        Supplier<SendDataResult> responseSupplier = () -> sendDataBlockingCore(tuples, data, rs, listener);
        return circuitBreaker.decorateSupplier(responseSupplier).get();
    }

    private SendDataResult sendDataBlockingCore(final Tuples tuples, String data, RetryStatistic rs, final SendDataListener listener)
            throws NullPointerException, RetryException {
        SendData.Response response;
        try {
            SendData.Request request = SendData.Request.newBuilder().setSchema(this.schema).setTable(this.table)
                    .setData(data).build();
            // With timeout or not.
            if (this.timeoutMillis > 0) {
                response = blockingStub.withDeadlineAfter(this.timeoutMillis, TimeUnit.MILLISECONDS).sendData(request);
            } else {
                response = blockingStub.sendData(request);
            }
        } catch (Exception e) {
            // Need retry.
            if (rs != null && !rs.increaseRetryTimes()) {
                l.error("{} Send data gRPC blocking exception retry", TAG, e);
                throw new RetryException(e.getMessage());
            }
            // Trigger Circuit Breaker.
            if (this.circuitBreaker != null) {
                l.error("{} Send data gRCP blocking exception trigger Circuit Breaker", TAG, e);
                throw new CircuitBreakException(e.getMessage());
            }
            // No retry, no Circuit Breaker, rethrow this exception to outside caller.
            throw new RuntimeException(e);
        }
        return handleSendDataResponse(listener, response, tuples);
    }

    /**
     * For some kind of the response, we need to retry, and the others, we don't need to.
     * For retry response, we will throw a retry exception.
     *
     * @throws RuntimeException exception that need to retry
     */
    private void handleGetJobMetadataResponse(final GetJobMetadataListener listener, GetJobMetadata.Response response) throws RuntimeException {
        if (response == null) {
            String errMsg = StrUtil.connect("Get null response from get job metadata gRPC API of table ",
                    MxGrpcClient.this.schema, ".", MxGrpcClient.this.table);
            l.error("{} {}", TAG, errMsg);
            listener.onFailure(errMsg);
            throw new RuntimeException(errMsg);
        }

        // Deal with different response code.
        switch (response.getCode()) {
            case GRPC_RESPONSE_CODE_OK: // Normal response.
                l.info("{} Get job metadata successfully of table {}.{}", TAG, MxGrpcClient.this.schema, MxGrpcClient.this.table);
                try {
                    JobMetadataWrapper wrapper = JobMetadataWrapper.wrapJobMetadata(response.getMetadata());
                    listener.onSuccess(wrapper);
                } catch (Exception e) {
                    String msg = StrUtil.connect("Get invalid job metadata of table ",
                            MxGrpcClient.this.schema, ".", MxGrpcClient.this.table, ": ", e.getMessage());
                    l.error("{} {}", TAG, msg, e);
                    listener.onFailure(msg);
                }
                break;
            case GRPC_RESPONSE_CODE_ERROR: // Error
            case GRPC_RESPONSE_INVALID_BROKERS: // Invalid brokers
            case GRPC_RESPONSE_TOPIC_NOT_FOUND: // Topic not found(reserved for Kafka)
            case GRPC_RESPONSE_CODE_ALL_TUPLES_FAILED: // All tuples are failed, for get job metadata, we should not get this kind of error.
            case GRPC_RESPONSE_CODE_ALL_UNDEFINED_ERROR: // Undefined error
                // For these errors, we may not need to retry.
                String msg = StrUtil.connect("Get error from get job metadata gRPC API for table ",
                        MxGrpcClient.this.schema, ".", MxGrpcClient.this.table, ": ",
                        response.getMsg(), " responseCode=", String.valueOf(response.getCode()));
                l.error("{} {}", TAG, msg);
                listener.onFailure(msg);
                break;
            case GRPC_RESPONSE_CODE_INVALID_REQUEST: // Invalid request, no need to retry.
            case GRPC_RESPONSE_CODE_INVALID_CONFIG: // Invalid config, no need to retry.
            case GRPC_RESPONSE_CODE_INVALID_TABLE: // Invalid table, no need to retry.
                String msgInvalidError = StrUtil.connect("Invalid request of get job metadata gRPC API ",
                        response.getMsg(), " of table ", MxGrpcClient.this.schema, ".",
                        MxGrpcClient.this.table, " responseCode=", String.valueOf(response.getCode()));
                l.error("{} {}", TAG, msgInvalidError);
                listener.onFailure(msgInvalidError);
                break;
            case GRPC_RESPONSE_CODE_MXGATE_NOT_READY: // MxGate is not ready, need to retry
            case GRPC_RESPONSE_CODE_TIMEOUT: // MxGate timeout (internally), need to retry
                String serverNotReady = StrUtil.connect("Server is not ready for get job metadata gRPC API ",
                        response.getMsg(), " of table ", MxGrpcClient.this.schema, ".",
                        MxGrpcClient.this.table, " responseCode=", String.valueOf(response.getCode()));
                l.error("{} {}", TAG, serverNotReady);
                listener.onFailure(serverNotReady);
                throw new RuntimeException(serverNotReady);
            default: // Unexpected response, maybe we need to retry, throw the exception.
                String unexpectedResp = StrUtil.connect("Get unexpected response from get job metadata gRPC API ",
                        response.getMsg(), " of table ", MxGrpcClient.this.schema, ".",
                        MxGrpcClient.this.table, " responseCode=", String.valueOf(response.getCode()));
                l.error("{} {}", TAG, unexpectedResp);
                listener.onFailure(unexpectedResp);
                throw new RuntimeException(unexpectedResp);
        }
    }

    /**
     * For some kind of the response, we need to retry, and the others, we don't need to.
     * For retry response, we will throw a retry exception.
     *
     * @throws RetryException exception that need to retry
     */
    private SendDataResult handleSendDataResponse(final SendDataListener listener, final SendData.Response response,
                                                  final Tuples tuples) throws RetryException {
        if (response == null) {
            String errMsg = makeSimpleErrorMsg("Get null response from send data gRPC api", response);
            l.error("{} Get null response from send data gRPC API of table {}.{}", TAG, this.schema, this.table);
            if (listener != null) {
                listener.onFailure(new SendDataResult(StatusCode.ALL_TUPLES_FAIL, null, errMsg), tuples);
            }
            throw new RetryException(errMsg);
        }

        String errMsg;
        SendDataResult result;
        // Deal with different response code.
        switch (response.getCode()) {
            case GRPC_RESPONSE_CODE_OK: // Normal response.
                // There are some tuples that could not be inserted into DB, but no need to retry.
                if (response.getErrorLinesMap().size() > 0) {
                    errMsg = StrUtil.connect("Send data to server with ", String.valueOf(response.getErrorLinesMap().size()),
                            " error lines from gRPC response of table", MxGrpcClient.this.schema, ".", MxGrpcClient.this.table);
                    l.error("{} {}", TAG, errMsg);
                    result = new SendDataResult(StatusCode.PARTIALLY_TUPLES_FAIL, response.getErrorLinesMap(), errMsg);
                    if (listener != null) {
                        listener.onFailure(result, tuples);
                    }
                    break;
                }

                String successMsg = StrUtil.connect("Send data successfully for table ",
                        MxGrpcClient.this.schema, ".", MxGrpcClient.this.table);
                l.debug("{} {}", TAG, successMsg);
                result = new SendDataResult(StatusCode.NORMAL, null, successMsg);
                if (listener != null) {
                    listener.onSuccess(result, tuples);
                }
                break;
            case GRPC_RESPONSE_CODE_ERROR: // Error
            case GRPC_RESPONSE_INVALID_BROKERS: // Invalid brokers
            case GRPC_RESPONSE_TOPIC_NOT_FOUND: // Topic not found(reserved for Kafka)
            case GRPC_RESPONSE_CODE_ALL_UNDEFINED_ERROR: // Undefined error
                // For these errors, we may not need to retry.
                errMsg = makeSimpleErrorMsg("Get error from send data gRPC API", response);
                l.error("{} {}", TAG, errMsg);
                result = new SendDataResult(StatusCode.ALL_TUPLES_FAIL, null, errMsg);
                if (listener != null) {
                    listener.onFailure(result, tuples);
                }
                break;
            case GRPC_RESPONSE_CODE_INVALID_REQUEST: // Invalid request, no need to retry.
            case GRPC_RESPONSE_CODE_INVALID_CONFIG: // Invalid config, no need to retry.
            case GRPC_RESPONSE_CODE_INVALID_TABLE: // Invalid table, no need to retry.
                errMsg = makeSimpleErrorMsg("Invalid request of send data gRPC API", response);
                l.error("{} {}", TAG, errMsg);
                result = new SendDataResult(StatusCode.ERROR, null, errMsg);
                if (listener != null) {
                    listener.onFailure(result, tuples);
                }
                break;
            case GRPC_RESPONSE_CODE_MXGATE_NOT_READY: // MxGate is not ready, need to retry.
                errMsg = makeSimpleErrorMsg("Server is not ready for send data gRPC API", response);
                l.error("{} {}", TAG, errMsg);
                result = new SendDataResult(StatusCode.ERROR, null, errMsg);
                if (listener != null) {
                    listener.onFailure(result, tuples);
                }
                throw new RuntimeException("Server is not ready for send data of table " +
                        MxGrpcClient.this.schema + "." + MxGrpcClient.this.table);
            case GRPC_RESPONSE_CODE_TIMEOUT: // MxGate timeout (internally), need to retry.
            case GRPC_RESPONSE_CODE_ALL_TUPLES_FAILED: // All tuples are failed,
                errMsg = makeSimpleErrorMsg("Send data to server with all lines failed from gRPC response", response);
                l.error("{} {}", TAG, errMsg);
                // For all tuples fail, we do not need to pass the error lines map.
                result = new SendDataResult(StatusCode.ALL_TUPLES_FAIL, null, errMsg);
                if (listener != null) {
                    listener.onFailure(result, tuples);
                }
                throw new RetryException(errMsg);
            default: // Unexpected response, maybe we need to retry, throw the exception.
                errMsg = makeSimpleErrorMsg("Get unexpected response from send data gRPC API", response);
                l.error("{} {}", TAG, errMsg);
                result = new SendDataResult(StatusCode.ALL_TUPLES_FAIL, null, errMsg);
                if (listener != null) {
                    listener.onFailure(result, tuples);
                }
                throw new RetryException(errMsg);
        }
        return result;
    }

    private String makeSimpleErrorMsg(String mHeader, SendData.Response response) {
        String errMsg = "";
        if (response != null) {
            errMsg = response.getMsg();
        }
        return StrUtil.connect(mHeader, " ", errMsg, " of table ", MxGrpcClient.this.schema, ".", MxGrpcClient.this.table);
    }

    /**
     * Until now, this async method has not been used.
     */
    public void getJobMetadataAsync(GetJobMetadataListener listener) throws NullPointerException, RetryException {
        if (listener == null) {
            throw new NullPointerException("ConnectionListener callback is null while try to get job metadata for table "
                    + MxGrpcClient.this.schema
                    + "."
                    + MxGrpcClient.this.table);
        }
        try {
            GetJobMetadata.Request request = GetJobMetadata.Request.newBuilder().setSchema(this.schema).setTable(this.table).build();
            if (this.timeoutMillis > 0) {
                asyncStub.withDeadlineAfter(this.timeoutMillis, TimeUnit.MILLISECONDS).getJobMetadata(request, getJobMetadataObserver(listener));
            } else {
                asyncStub.getJobMetadata(request, getJobMetadataObserver(listener));
            }
        } catch (Exception e) {
            l.error("{} Get job metadata(async) exception {} for table {}.{}",
                    TAG, e.getMessage(), MxGrpcClient.this.schema, MxGrpcClient.this.table, e);
            // Here, right now, we haven't distinguished which exception we should retry and which should not.
            throw new RetryException(e.getMessage());
        }
    }

    /**
     * Until now, this async method has not been used.
     */
    public void sendDataAsync(final Tuples tuples, final String data, final boolean needCompress, final boolean needBase64Encoding,
                              final SendDataListener listener, final RetryStatistic rs)
            throws NullPointerException, RetryException {
        if (listener == null) {
            throw new NullPointerException("SendDataListener callback is null while try to get job metadata for table "
                    + MxGrpcClient.this.schema + "." + MxGrpcClient.this.table);
        }
        // With Circuit Breaker.
        if (this.circuitBreaker != null) {
            this.circuitBreaker.decorateRunnable(new Runnable() {
                @Override
                public void run() throws NullPointerException, RetryException {
                    MxGrpcClient.this.sendDataAsyncCore(tuples, data, needCompress, needBase64Encoding, listener, rs);
                }
            }).run();
            return;
        }
        // Without Circuit Breaker.
        sendDataAsyncCore(tuples, data, needCompress, needBase64Encoding, listener, rs);
    }

    private void sendDataAsyncCore(final Tuples tuples, String data, final boolean needCompress, final boolean needBase64Encoding,
                                   final SendDataListener listener, final RetryStatistic rs)
            throws NullPointerException, RetryException {
        try {
            if (needCompress) {
                data = compressData(data, needBase64Encoding);
            }
            SendData.Request request = SendData.Request.newBuilder().setSchema(this.schema).setTable(this.table).setData(data).build();
            if (this.timeoutMillis > 0) {
                asyncStub.withDeadlineAfter(this.timeoutMillis, TimeUnit.MILLISECONDS).sendData(request, getSendDataObserver(listener, tuples));
            } else {
                asyncStub.sendData(request, getSendDataObserver(listener, tuples));
            }
        } catch (Exception e) {
            // Need retry.
            if (rs != null && !rs.increaseRetryTimes()) {
                l.error("{} Send data gRPC async exception retry", TAG, e);
                throw new RetryException(e.getMessage());
            }
            // Trigger Circuit Breaker.
            if (this.circuitBreaker != null) {
                l.error("{} Send data gRCP async exception trigger Circuit Breaker", TAG, e);
                throw new CircuitBreakException(e.getMessage());
            }
            // No retry, no Circuit Breaker, rethrow this exception to outside caller.
            throw new RuntimeException(e);
        }
    }

    private StreamObserver<GetJobMetadata.Response> getJobMetadataObserver(final GetJobMetadataListener listener) {
        return new StreamObserver<GetJobMetadata.Response>() {
            @Override
            public void onNext(GetJobMetadata.Response response) {
                try {
                    handleGetJobMetadataResponse(listener, response);
                } catch (Exception e) {
                    l.error("Handle GetJobMetadata Response async exception for table {}.{}", MxGrpcClient.this.schema, MxGrpcClient.this.table, e);
                }
            }

            @Override
            public void onError(Throwable t) {
                l.error("{} Get job metadata async error for table {}.{} : ", TAG, MxGrpcClient.this.schema, MxGrpcClient.this.table, t);
                if (t != null) {
                    t.printStackTrace();
                }
            }

            @Override
            public void onCompleted() {
                l.info("{} Get job metadata async complete for table {}.{}", TAG, MxGrpcClient.this.schema, MxGrpcClient.this.table);
            }
        };
    }

    private StreamObserver<SendData.Response> getSendDataObserver(final SendDataListener listener, final Tuples tuples) {
        // Return an anonymous inner class which acts as a closure.
        return new StreamObserver<SendData.Response>() {
            @Override
            public void onNext(SendData.Response response) {
                try {
                    handleSendDataResponse(listener, response, tuples);
                } catch (Exception e) {
                    l.error("{} Handle send data response async exception for table {}.{}:",
                            TAG, MxGrpcClient.this.schema, MxGrpcClient.this.table, e);
                }
            }

            @Override
            public void onError(Throwable t) {
                l.error("{} Send data async gRPC error for table {}.{} : ", TAG, MxGrpcClient.this.schema, MxGrpcClient.this.table, t);
                if (t != null) {
                    t.printStackTrace();
                }
            }

            @Override
            public void onCompleted() {
                l.info("{} Send data async gRPC complete for table {}.{}", TAG, MxGrpcClient.this.schema, MxGrpcClient.this.table);
            }
        };
    }

    static class SendDataInterceptor implements ClientInterceptor {
        private final Map<String, String> metadataMap;

        public SendDataInterceptor() {
            metadataMap = new HashMap<>();
        }

        public void setMetadataKV(String key, String value) {
            this.metadataMap.put(key, value);
        }

        public void removeMetadataKV(String key) {
            this.metadataMap.remove(key);
        }

        @Override
        public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
            return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(next.newCall(method, callOptions)) {

                @Override
                public void start(Listener<RespT> responseListener, Metadata headers) {
                    if (metadataMap.size() > 0) {
                        for (Map.Entry<String, String> entry : metadataMap.entrySet()) {
                            headers.put(Metadata.Key.of(entry.getKey(), Metadata.ASCII_STRING_MARSHALLER), entry.getValue());
                        }
                    }
                    super.start(responseListener, headers);
                }
            };
        }
    }

}
