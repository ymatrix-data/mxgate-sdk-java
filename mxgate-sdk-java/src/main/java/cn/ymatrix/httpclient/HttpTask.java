package cn.ymatrix.httpclient;

import cn.ymatrix.api.StatusCode;
import cn.ymatrix.apiserver.SendDataListener;
import cn.ymatrix.apiserver.SendDataResult;
import cn.ymatrix.builder.CircuitBreakerConfig;
import cn.ymatrix.builder.MxBuilder;
import cn.ymatrix.compress.CompressionFactory;
import cn.ymatrix.compress.Compressor;
import cn.ymatrix.data.Tuples;
import cn.ymatrix.data.TuplesTarget;
import cn.ymatrix.exception.BrokenTuplesException;
import cn.ymatrix.exception.CircuitBreakException;
import cn.ymatrix.exception.RetryException;
import cn.ymatrix.faulttolerance.CircuitBreakerFactory;
import cn.ymatrix.faulttolerance.RetryConfiguration;
import cn.ymatrix.faulttolerance.RetryControl;
import cn.ymatrix.faulttolerance.RetryStatistic;
import cn.ymatrix.logger.MxLogger;
import cn.ymatrix.utils.CSVConstructor;
import cn.ymatrix.utils.StrUtil;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.client.api.Response;
import org.eclipse.jetty.client.api.Result;
import org.eclipse.jetty.client.util.BufferingResponseListener;
import org.eclipse.jetty.client.util.BytesContentProvider;
import org.eclipse.jetty.client.util.StringContentProvider;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.util.Callback;
import org.slf4j.Logger;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public abstract class HttpTask implements Task, RetryControl {
    private static final String TAG = HttpTask.class.getName() + "[" + MxBuilder.SDK_VERSION + "]";
    private static final Logger l = MxLogger.init(HttpTask.class);
    public static final String lineNumPrefix = "At line:";
    private static final String CONTENT_ENCODING = "Content-Encoding";
    private static final String COMPRESS_METHOD = "zstd";
    private static final String BYTES_ENCODING = "bytes-encoding";
    private static final String BASE64 = "Base64";

    private static final int maxResponseBufferSize = 8 * 1024 * 1024;
    /**
     * Http Client of Jetty.
     */
    private final HttpClient client;
    private RetryConfiguration retryConfiguration;
    private CircuitBreakerConfig circuitBreakerConfig;
    private RetryStatistic rs;
    private SendDataListener listener;
    private Compressor compressor;
    private final CSVConstructor csvConstructor;

    public HttpTask(CircuitBreakerConfig cbConfig, final HttpClient client, final CSVConstructor constructor) {
        this.circuitBreakerConfig = cbConfig;
        this.client = client;
        this.csvConstructor = constructor;
    }

    public void registerListener(SendDataListener listener) {
        this.listener = listener;
    }

    public void withRetry(RetryConfiguration retryConfiguration) {
        this.retryConfiguration = retryConfiguration;
        this.rs = new RetryStatistic(retryConfiguration.getMaxAttempts());
    }

    public boolean canRetry() {
        return (this.rs != null && !this.rs.exceedMaxRetryTimes());
    }

    private void tuplesValidation(final Tuples tuples) throws NullPointerException {
        if (tuples == null) {
            throw new NullPointerException("send http request on a null Tuples.");
        }
        TuplesTarget target = tuples.getTarget();
        if (target == null) {
            throw new NullPointerException("send http request on a null TuplesTarget");
        }
        if (StrUtil.isNullOrEmpty(target.getURL())) {
            throw new NullPointerException("send http request on a null Target URL.");
        }
    }

    private Request prepareRequest(final Tuples tuples) {
        TuplesTarget target = tuples.getTarget();
        Request request = client.POST(target.getURL());
        request.timeout(tuples.getTarget().getTimeout(), TimeUnit.MILLISECONDS);
        request.header(HttpHeader.CONTENT_TYPE, "text/plain");
        return request;
    }

    private String prepareContent(final Tuples tuples) throws BrokenTuplesException, NullPointerException {
        if (this.csvConstructor == null) {
            throw new NullPointerException("CSV Constructor is null in prepareContent HttpTask.");
        }

        StringBuilder rawData = this.csvConstructor.constructCSVFromTuplesWithTasks(tuples.getTuplesList(), tuples.getCSVBatchSize(), tuples.getDelimiter());
        if (rawData == null) {
            throw new BrokenTuplesException("Get a nullable raw CSV data from CSV constructor.");
        }
        String tableName = new StringBuilder().append(tuples.getSchema()).append(".").append(tuples.getTable()).append("\n").toString();
        return rawData.insert(0, tableName).toString();
    }

    private void needCompress(final Tuples tuples, Request request, String content) {
        if (tuples.needCompress()) {
            request.header(CONTENT_ENCODING, COMPRESS_METHOD);
            if (compressor == null) {
                compressor = CompressionFactory.getCompressor();
            }
            byte[] compressedBytes = compressor.compress(content.getBytes(StandardCharsets.UTF_8));
            // Sometimes, we need to use base64 to encode the compressed bytes
            // to make sure the correctness of the conversion from bytes to string.
            if (tuples.needBase64Encoding4CompressedBytes()) {
                request.header(BYTES_ENCODING, BASE64);
                byte[] encodedBytes = Base64.getEncoder().encode(compressedBytes);
                request.content(new BytesContentProvider(encodedBytes), "utf-8");
            } else {
                request.content(new BytesContentProvider(compressedBytes), "utf-8");
            }
        } else {
            request.content(new StringContentProvider(content, "utf-8"));
        }
    }

    private CircuitBreaker getCircuitBreaker(final Tuples tuples) {
        if (this.circuitBreakerConfig == null) {
            return null;
        }
        // Ignore the RetryException.class
        this.circuitBreakerConfig.setIgnoredExceptions(RetryException.class);
        return CircuitBreakerFactory.getInstance().prepareCircuitBreaker(tuples.getTarget().getURL(),
                tuples.getSchema(), tuples.getTable(), this.circuitBreakerConfig);
    }

    public void requestAsync(final Tuples tuples) throws NullPointerException, RetryException, CircuitBreakException {
        tuplesValidation(tuples);
        // Decorate with circuit breaker test.
        CircuitBreaker breaker = getCircuitBreaker(tuples);
        if (breaker != null) {
            breaker.decorateRunnable(new Runnable() {
                @Override
                public void run() throws RetryException, CircuitBreakException {
                    requestAsyncCore(tuples);
                }
            }).run();
        }
        requestAsyncCore(tuples);
    }

    private void requestAsyncCore(final Tuples tuples) throws RetryException, CircuitBreakException {
        try {
            final long startTime = System.currentTimeMillis();
            final Request request = prepareRequest(tuples);
            final String requestContent = prepareContent(tuples);
            if (StrUtil.isNullOrEmpty(requestContent)) {
                l.error("{} CSV raw data is null in blocking HTTP request.", TAG);
                return;
            }
            needCompress(tuples, request, requestContent);

            // With this BufferingResponseListener we can get the response content to
            // analyze the error lines and reasons.
            request.send(new BufferingResponseListener(maxResponseBufferSize) {
                @Override
                public void onComplete(Result result) {
                    try {
                        final long endTime = System.currentTimeMillis();
                        final long timeCost = endTime - startTime;
                        HttpTask.this.handleResponse(tuples, result.getResponse().getStatus(),
                                getContentAsString(), requestContent, timeCost, "async");
                    } catch (Exception e) {
                        l.error("{} Send request onComplete callback response handling exception callback onFailure.", TAG, e);
                        HttpTask.this.allTuplesFailCallback(e.getMessage(), tuples);
                    }
                }
            });
        } catch (Exception e) {
            // For BrokenTuplesException, we call the onFailure callback of the listener directly.
            // No need to retry or trigger the Circuit Breaker.
            if (e instanceof BrokenTuplesException) {
                l.error("{} Send request async mode broken tuples exception with callback onFailure", TAG, e);
                allTuplesFailCallback(e.getMessage(), tuples);
                return;
            }

            // Need to retry, will not call the onFailure callback.
            if (this.rs != null && !this.rs.increaseRetryTimes()) {
                l.error("{} Send request async mode request exception retry", TAG, e);
                throw new RetryException("send request async mode request exception retry " + e.getMessage());
            }

            // The Circuit Breaker has been configured. call the onFailure callback and throw the exception.
            if (getCircuitBreaker(tuples) != null) {
                l.error("{} Send request async mode trigger the CircuitBreaker", TAG, e);
                allTuplesFailCallback(e.getMessage(), tuples);
                throw new CircuitBreakException(e.getMessage());
            }

            // No retry, no Circuit Breaker,
            l.error("{} Send request asnyc mode exception with callback onFailure", TAG, e);
            allTuplesFailCallback(e.getMessage(), tuples);
        }
    }

    private void allTuplesFailCallback(String errMsg, final Tuples tuples) {
        if (this.listener != null) {
            SendDataResult result = new SendDataResult(StatusCode.ALL_TUPLES_FAIL, null, errMsg);
            this.listener.onFailure(result, tuples);
        }
    }

    public SendDataResult requestBlocking(final Tuples tuples) throws NullPointerException, RetryException, CircuitBreakException {
        tuplesValidation(tuples);
        CircuitBreaker breaker = getCircuitBreaker(tuples);
        if (breaker != null) {
            Supplier<SendDataResult> resultSupplier = () -> requestBlockingCore(tuples);
            return breaker.decorateSupplier(resultSupplier).get();
        }
        return requestBlockingCore(tuples);
    }

    private SendDataResult requestBlockingCore(final Tuples tuples) throws RetryException, CircuitBreakException {
        try {
            long startTimeMillis = System.currentTimeMillis();
            Request request = prepareRequest(tuples);
            String requestContent = prepareContent(tuples);
            if (requestContent == null) {
                l.error("{} CSV raw data is null in blocking HTTP request.", TAG);
                return null;
            }
            needCompress(tuples, request, requestContent);
            ContentResponse response;
            response = request.send();
            long endTimeMillis = System.currentTimeMillis();
            return handleResponse(tuples, response.getStatus(), response.getContentAsString(),
                    requestContent, endTimeMillis - startTimeMillis, "blocking");
        } catch (Exception e) {
            // For BrokenTuplesException, we call the onFailure callback of the listener directly.
            // No need to retry or trigger the Circuit Breaker.
            if (e instanceof BrokenTuplesException) {
                l.error("{} Send request blocking mode broken tuples exception with callback onFailure", TAG, e);
                allTuplesFailCallback(e.getMessage(), tuples);
                return null;
            }

            // Need to retry, will not call the onFailure callback.
            if (this.rs != null && !this.rs.increaseRetryTimes()) {
                l.error("{} Send request blocking mode request exception retry", TAG, e);
                throw new RetryException("send request blocking mode request exception retry " + e.getMessage());
            }

            // The Circuit Breaker has been configured. call the onFailure callback and throw the exception.
            if (getCircuitBreaker(tuples) != null) {
                l.error("{} Send request blocking mode trigger the CircuitBreaker", TAG, e);
                allTuplesFailCallback(e.getMessage(), tuples);
                throw new CircuitBreakException(e.getMessage());
            }

            // No retry, no Circuit Breaker,
            l.error("{} Send request blocking mode exception with callback onFailure", TAG, e);
            allTuplesFailCallback(e.getMessage(), tuples);
            return null;
        }
    }

    private SendDataResult handleResponse(final Tuples tuples, int status, String respContent, String requestContent, long timeCostMillis, String mode) {
        if (status == 204) { // No need to parse body.
            l.info("{} Send request success all {} tuples HTTP {} mode (response code = {} time_cost_millis = {})",
                    TAG, tuples.size(), mode, status, timeCostMillis);
            SendDataResult result = new SendDataResult(StatusCode.NORMAL, null, "Send " + tuples.size() + " tuples succeed.");
            if (this.listener != null) {
                this.listener.onSuccess(result, tuples);
            }
            return result;
        } else if (status == 200) { // Need to parse body, partially tuples failure.
            l.error("{} Send request success partially HTTP {} mode (response code = {} time_cost_millis = {}) {}",
                    TAG, mode, status, timeCostMillis, respContent);
            if (StrUtil.isNullOrEmpty(respContent)) { // No response body, we treat all the tuples has been sent successfully.
                SendDataResult result = new SendDataResult(StatusCode.NORMAL, null, "Send " + tuples.size() + " tuples succeed.");
                if (this.listener != null) {
                    this.listener.onSuccess(result, tuples);
                }
                return result;
            }
            Map<Long, String> errorLineCouples = parsePartiallyErrorResponse(respContent);
            for (Map.Entry<Long, String> entry : errorLineCouples.entrySet()) {
                l.error("{} Send request HTTP {} mode Total tuples size = {}: error line number = {}, error line = {}, error reason = {}",
                        TAG,
                        mode,
                        tuples.size(),
                        entry.getKey(),
                        tuples.getTupleByIndex((int) (entry.getKey() - 1)),
                        entry.getValue());
            }
            SendDataResult result = new SendDataResult(StatusCode.PARTIALLY_TUPLES_FAIL, errorLineCouples, respContent);
            if (this.listener != null) {
                this.listener.onFailure(result, tuples);
            }
            return result;
        } else { // Right now, for 2xx, mxgate only response 200 or 204, for other response code, it will be treated as a failure.
            l.error("{} Send request HTTP {} mode request error all tuples {} time_cost_millis = {}, status = {} response = {} \n{} ",
                    TAG, mode, tuples.size(), timeCostMillis, status, respContent, requestContent);
            SendDataResult result = new SendDataResult(StatusCode.ALL_TUPLES_FAIL, null, respContent);
            if (this.listener != null) {
                this.listener.onFailure(result, tuples);
            }
            return result;
        }
    }

    private Map<Long, String> parsePartiallyErrorResponse(String response) {
        if (StrUtil.isNullOrEmpty(response)) {
            return null;
        }
        // Split the result by "\n"
        String[] lines = response.split("\r?\n");
        if (lines.length == 0) {
            return null;
        }
        Map<Long, String> errorCouple = new HashMap<>();
        for (int i = 0; i < lines.length; i++) {
            // Start with lineNumPrefix and not the last line.
            if (lines[i].startsWith(lineNumPrefix) // find the line number prefix.
                    && i < lines.length - 1 // not the last line.
                    && !lines[i + 1].startsWith(lineNumPrefix)) { // the next line will be the error line reason, not the error line number.
                String numStr = lines[i].substring(lineNumPrefix.length()).trim();
                // Convert the line number to long.
                try {
                    long errorNumInt = Long.parseLong(numStr) - 1; // For http the first line is schema.table.
                    String errorReasonStr = findMultipleErrorReasonLines(lines, i + 1);
                    errorCouple.put(errorNumInt, errorReasonStr);
                    l.debug("error line num = {} error reason = {}", errorNumInt, errorReasonStr);
                } catch (Exception e) {
                    l.error("Convert error line number {} error: ", numStr, e);
                }
            }
        }
        return errorCouple;
    }

    /**
     * Find all the lines that consistent and not start with lineNumPrefix
     * They may be one error reason with multiple lines.
     *
     * @param responseLines
     * @param index
     * @return Multiple error reason lines.
     */
    private String findMultipleErrorReasonLines(String[] responseLines, int index) {
        if (index >= responseLines.length) {
            return "";
        }
        StringBuilder wholeErrorLines = new StringBuilder();
        for (int i = index; i < responseLines.length; i++) {
            if (responseLines[i].startsWith(lineNumPrefix)) {
                break;
            }
            wholeErrorLines.append(responseLines[i]).append('\n');
        }
        return wholeErrorLines.toString();
    }

}
