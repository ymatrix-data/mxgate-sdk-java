package cn.ymatrix.worker;

import cn.ymatrix.api.StatusCode;
import cn.ymatrix.apiclient.Result;
import cn.ymatrix.apiclient.ResultStatus;
import cn.ymatrix.apiserver.SendDataListener;
import cn.ymatrix.apiserver.SendDataResult;
import cn.ymatrix.builder.MxBuilder;
import cn.ymatrix.builder.RequestType;
import cn.ymatrix.cache.Cache;
import cn.ymatrix.concurrencycontrol.WorkerPool;
import cn.ymatrix.concurrencycontrol.WorkerPoolFactory;
import cn.ymatrix.data.Tuples;
import cn.ymatrix.data.TuplesConsumeResultConvertor;
import cn.ymatrix.exception.RetryException;
import cn.ymatrix.faulttolerance.CircuitBreakerFactory;
import cn.ymatrix.httpclient.DataSendingGRPCTask;
import cn.ymatrix.httpclient.HttpTask;
import cn.ymatrix.httpclient.SingletonHTTPClient;
import cn.ymatrix.logger.MxLogger;
import cn.ymatrix.messagecenter.ResultMessageCenter;
import cn.ymatrix.messagecenter.ResultMessageQueue;
import cn.ymatrix.utils.CSVConstructor;
import cn.ymatrix.utils.StrUtil;
import cn.ymatrix.utils.ThroughoutCalculator;
import org.eclipse.jetty.client.HttpClient;
import org.slf4j.Logger;

import java.security.InvalidParameterException;
import java.util.concurrent.atomic.AtomicBoolean;

public class TuplesConsumer extends TaskWorker {
    private static final String TAG = StrUtil.logTagWrap(TuplesConsumer.class.getName());
    private WorkerPool tasksPool;

    // If dropAll == true, we will drop all the tuples and not send them to the backend server.
    // By default, it is gRPC.
    private final AtomicBoolean dropAll;

    private RequestType requestType;

    private boolean useAsyncRequest;

    private final SendDataListener listener;

    private final HttpClient client;

    private final CSVConstructor csvConstructor;

    public TuplesConsumer(final HttpClient client, final CSVConstructor constructor)
            throws NullPointerException, InvalidParameterException {
        super();
        if (client == null) {
            throw new NullPointerException("HttpClient is null in TuplesConsumer construction");
        }
        if (constructor == null) {
            throw new NullPointerException("CSV constructor is null during TuplesConsumer construction.");
        }
        this.dropAll = new AtomicBoolean(false);
        this.requestType = RequestType.WithGRPC;
        this.stop.set(false);
        this.listener = new SendDataListenerImpl();
        this.client = client;
        this.csvConstructor = constructor;
    }

    public RequestType getRequestType() {
        return requestType;
    }

    public void setRequestType(RequestType requestType) {
        if (requestType == null) {
            return;
        }
        this.requestType = requestType;
    }

    public void dropAll(boolean drop) {
        this.dropAll.set(drop);
    }

    public void useAsyncRequest(boolean asyncRequest) {
        this.useAsyncRequest = asyncRequest;
    }

    /**
     * Consume tuples from cache and
     *
     * @param tasksPool to dispatch task
     */
    public void poolToDispatchTask(WorkerPool tasksPool) {
        this.tasksPool = tasksPool;
    }

    // Consume tuples from cache.
    public void consume(final Cache cache) throws NullPointerException {
        if (cache == null) {
            throw new NullPointerException("register on a null cache");
        }
        this.workerPool.join(new Runnable() {
            @Override
            public void run() {
                while (!TuplesConsumer.this.stop.get()) {
                    try {
                        l.info("{} Before consume cache size {}", TAG, cache.size());
                        Tuples tuples = cache.get();
                        if (TuplesConsumer.this.dropAll.get()) {
                            l.info("{} Drop tuples in consumer mode.", TAG);
                            // Comment out this through output calculation to avoid the impact of data loading performance.
                            // ThroughoutCalculator.getInstance().add(tuples);
                            continue;
                        }
                        if (TuplesConsumer.this.tuplesValidation(tuples)) {
                            while (CircuitBreakerFactory.getInstance().isCircuitBreak(tuples.getTarget().getURL(), tuples.getSchema(), tuples.getTable())) {
                                try {
                                    l.warn("{} Circuit breaker open, stop sending data to {}.{}", TAG, tuples.getSchema(), tuples.getTable());
                                    Thread.sleep(100);
                                } catch (Exception e) {
                                    // do nothing
                                    l.warn("{} failed to make thread sleep during circuit-breaking: {}", TAG, e.getMessage());
                                }
                            }
                            TuplesConsumer.this.sendTuples(tuples);
                        }
                    } catch (Exception e) {
                        l.error("{} Consume tuples from cache exception: {}.", TAG, e);
                    }
                }
            }
        });
    }

    private boolean tuplesValidation(final Tuples tuples) {
        try {
            tuplesNullableCheck(tuples);
            return true;
        } catch (NullPointerException e) {
            handleInvalidTuples(tuples);
        }
        return false;
    }

    private void handleInvalidTuples(final Tuples tuples) {
        if (tuples == null) {
            // For nullable tuples, we only print a line of error log.
            l.error("{} Invalid nullable tuples", TAG);
            return;
        }
        this.listener.onFailure(handleAllTuplesFailure(tuples.getSchema(), tuples.getTable()), tuples);
    }

    private void sendTuples(final Tuples tuples) {
        RequestType consumeType = this.getRequestType();
        try {
            if (consumeType == RequestType.WithHTTP) {
                sendTuplesHttp(tuples);
            } else {
                sendTuplesGRPC(tuples);
            }
        } catch (Exception e) {
            if (tuples != null) {
                l.error("{} send tuples in consumer with exception", TAG, e);
                handleInvalidTuples(tuples);
            }
        }
    }

    private SendDataResult handleAllTuplesFailure(String schema, String table) {
        String errMsg = "All tuples fail for table " + schema  + "." + table;
        return new SendDataResult(StatusCode.ALL_TUPLES_FAIL, null, errMsg);
    }

    private void tuplesNullableCheck(Tuples tuples) throws NullPointerException {
        if (tuples == null) {
            throw new NullPointerException("create DataSendingGRPCTask on a null tuples.");
        }
        if (tuples.getTarget() == null || tuples.getTarget().getURL() == null) {
            throw new NullPointerException("create DataSendingGRPCTask on a null tuples target.");
        }
        if (tuples.getSchema() == null) {
            throw new NullPointerException("create DataSendingGRPCTask on a null tuples schema.");
        }
        if (tuples.getTable() == null) {
            throw new NullPointerException("create DataSendingGRPCTask on a null tuples table.");
        }
    }

    private void sendTuplesHttp(final Tuples tuples) {
        HttpTask httpTask = generateHTTPTask(tuples, (this.retryConfig != null));
        httpTask.registerListener(this.listener);
        if (this.retryConfig != null) {
            httpTask.withRetry(this.retryConfig);
            Runnable retryTask = decorateTaskWithRetry("SendTuplesHTTP", httpTask);
            if (retryTask != null) {
                this.tasksPool.join(retryTask);
            } else {
                l.error("{} Get an empty retry task to join for send tuples with HTTP {}.", TAG, tuples);
            }
        } else {
            this.tasksPool.join(httpTask);
        }
    }

    private HttpTask generateHTTPTask(final Tuples tuples, final boolean retry) {
        return new HttpTask(this.circuitBreakerConfig, this.client, this.csvConstructor) {
            @Override
            public void run() throws RetryException {
                try {
                    if (TuplesConsumer.this.useAsyncRequest) {
                        this.requestAsync(tuples);
                    } else {
                        this.requestBlocking(tuples);
                    }
                } catch (RetryException e) {
                    if (retry) {
                        throw e;
                    }
                    l.error("{} Send tuples raw HTTP with Exception for table {}.{}", TAG, tuples.getSchema(), tuples.getTable(), e);
                } catch (NullPointerException e) {
                    // There maybe something wrong with the Tuples.
                    TuplesConsumer.this.handleInvalidTuples(tuples);
                } catch (Exception e) { // With no retry exception, we will catch all the unexpected exceptions.
                    // Here, we only print an error log here, and leave all the
                    // error handling to the DataSendGRPCTask.
                    l.error("{} Send tuples HTTP with unexpected Exception for table {}.{}", TAG, tuples.getSchema(), tuples.getTable(), e);
                }
            }
        };
    }

    private void sendTuplesGRPC(final Tuples tuples) throws NullPointerException {
        DataSendingGRPCTask task = generateTaskGRPC(tuples, (this.retryConfig != null));
        task.registerListener(listener);
        // Retry task.
        if (this.retryConfig != null) {
            task.withRetry(this.retryConfig);
            Runnable retryTask = decorateTaskWithRetry("SendTuplesGRPC", task);
            if (retryTask == null) {
                l.error("{} Get an empty retry task to join for send tuples with gRPC {}.", TAG, tuples);
            }
            this.tasksPool.join(retryTask);
            return;
        }
        // Raw task.
        this.tasksPool.join(task);
    }

    private DataSendingGRPCTask generateTaskGRPC(final Tuples tuples, final boolean retry) throws NullPointerException {
        return new DataSendingGRPCTask(tuples, circuitBreakerConfig, this.csvConstructor) {
            @Override
            public void run() throws RetryException {
                try {
                    if (TuplesConsumer.this.useAsyncRequest) {
                        this.sendTuplesAsync(tuples);
                    } else {
                        this.sendTuplesBlocking(tuples);
                    }
                } catch (RetryException e) {
                    if (retry) {
                        throw e;
                    }
                    l.error("{} Send tuples raw gRCP with Exception for table {}.{}", TAG, tuples.getSchema(), tuples.getTable(), e);
                } catch (NullPointerException e) {
                    // There maybe something wrong with the Tuples.
                    TuplesConsumer.this.handleInvalidTuples(tuples);
                } catch (Exception e) { // With no retry exception, we will catch all the unexpected exceptions.
                    // We only print an error log here, and leave all the
                    // error handling to the DataSendGRPCTask.
                    l.error("{} Send tuples gRPC raw with unexpected Exception for table {}.{}", TAG, tuples.getSchema(), tuples.getTable(), e);
                }
            }
        };
    }

    private static class SendDataListenerImpl implements SendDataListener {
        private static final Logger l = MxLogger.init(SendDataListenerImpl.class);

        public SendDataListenerImpl() {
        }

        @Override
        public void onSuccess(SendDataResult result, Tuples tuples) {
            try {
                if (tuples != null) {
                    ResultMessageQueue<Result> queue = ResultMessageCenter.getSingleInstance().fetch(tuples.getSenderID());
                    if (queue != null && result != null) {
                        int successTuples = tuples.size();
                        String successMsg = StrUtil.connect(result.getMsg(), " with ", String.valueOf(successTuples), " lines.");
                        Result successResult = new Result();
                        successResult.setMsg(successMsg);
                        successResult.setSucceedLines(TuplesConsumeResultConvertor.convertSucceedTuplesLines(null, tuples));
                        successResult.setSuccessLinesSerialNumList(TuplesConsumeResultConvertor.convertSuccessfulSerialNums(result.getErrorLinesMap(), tuples));
                        successResult.setStatus(ResultStatus.SUCCESS);
                        successResult.setRawTuples(tuples);
                        queue.add(successResult);
                        // After add the result into message queue, shrink the Tuples list.
                        // tuples.shrink();
                    }
                }
            } catch (Exception e) {
                l.error("{} Send data blocking onSuccess callback exception ", TAG, e);
            }
        }

        @Override
        public void onFailure(SendDataResult result, Tuples tuples) {
            try {
                if (tuples != null) {
                    ResultMessageQueue<Result> queue = ResultMessageCenter.getSingleInstance().fetch(tuples.getSenderID());
                    if (queue != null && result != null) {
                        Result failureResult = new Result();
                        failureResult.setMsg(result.getMsg());
                        if (result.getCode() == StatusCode.ALL_TUPLES_FAIL) {
                            failureResult.setErrorTuplesMap(TuplesConsumeResultConvertor.convertErrorTuples(null, tuples, result.getMsg()));
                            failureResult.setSucceedLines(0); // All tuples are failed, no succeed lines.
                        } else {
                            failureResult.setErrorTuplesMap(TuplesConsumeResultConvertor.convertErrorTuples(result.getErrorLinesMap(), tuples, result.getMsg()));
                            failureResult.setSucceedLines(TuplesConsumeResultConvertor.convertSucceedTuplesLines(result.getErrorLinesMap(), tuples));
                            failureResult.setSuccessLinesSerialNumList(TuplesConsumeResultConvertor.convertSuccessfulSerialNums(result.getErrorLinesMap(), tuples));
                        }
                        failureResult.setStatus(ResultStatus.FAILURE);
                        failureResult.setRawTuples(tuples);
                        queue.add(failureResult);
                        // After add the result into message queue, shrink the Tuples list.
                        // tuples.shrink();
                    }
                }
            } catch (Exception e) {
                l.error("{} Send data blocking onFailure callback exception ", TAG, e);
            }
        }
    }


}
