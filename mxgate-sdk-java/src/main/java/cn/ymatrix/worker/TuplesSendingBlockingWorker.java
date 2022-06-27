package cn.ymatrix.worker;

import cn.ymatrix.apiserver.SendDataResult;
import cn.ymatrix.builder.CircuitBreakerConfig;
import cn.ymatrix.builder.RequestType;
import cn.ymatrix.data.Tuples;
import cn.ymatrix.exception.RetryException;
import cn.ymatrix.faulttolerance.RetryConfiguration;
import cn.ymatrix.faulttolerance.RetryControl;
import cn.ymatrix.httpclient.DataSendingGRPCTask;
import cn.ymatrix.httpclient.HttpTask;
import cn.ymatrix.logger.MxLogger;
import cn.ymatrix.utils.CSVConstructor;
import cn.ymatrix.utils.StrUtil;
import org.eclipse.jetty.client.HttpClient;
import org.slf4j.Logger;

public class TuplesSendingBlockingWorker {
    private static final String TAG = StrUtil.logTagWrap(TuplesSendingBlockingWorker.class.getName());

    private static final Logger l = MxLogger.init(TuplesSendingBlockingWorker.class);

    private RetryConfiguration retryConfiguration;

    private CircuitBreakerConfig circuitBreakerConfig;

    private RequestType requestType;

    private final HttpClient httpClient;

    private final CSVConstructor constructor;

    public TuplesSendingBlockingWorker(RequestType requestType, HttpClient client, CSVConstructor constructor) {
        this.requestType = requestType;
        this.httpClient = client;
        this.constructor = constructor;
    }

    public void withRetry(RetryConfiguration retryConfiguration) {
        this.retryConfiguration = retryConfiguration;
    }

    public void withCircuitBreaker(CircuitBreakerConfig c) {
        this.circuitBreakerConfig = c;
    }

    public SendDataResult sendTuplesBlocking(final Tuples tuples) throws NullPointerException {
        SendDataResult result = null;
        RetryControl retryCtrl = null;
        boolean success = false;
        while (!success) {
            try {
                switch (this.requestType) {
                    case WithHTTP:
                        // Enter the retry branch, it is not the first time.
                        if (retryCtrl instanceof HttpTask) {
                            HttpTask task = (HttpTask) retryCtrl;
                            task.requestBlocking(tuples);
                            success = true;
                            break;
                        }
                        HttpTask httpTask = new HttpTask(this.circuitBreakerConfig, this.httpClient, this.constructor) {
                            @Override
                            public void run() {
                                // For blocking call with result return, nothing to do here.
                            }
                        };
                        if (retryCtrl == null) {
                            retryCtrl = httpTask;
                        }
                        if (this.retryConfiguration != null) {
                            httpTask.withRetry(this.retryConfiguration);
                        }
                        result = httpTask.requestBlocking(tuples);
                        success = true;
                        break;
                    case WithGRPC:
                        // Enter the retry branch, it is not the first time.
                        if (retryCtrl instanceof DataSendingGRPCTask) {
                            result = ((DataSendingGRPCTask)retryCtrl).sendTuplesBlocking(tuples);
                            success = true;
                            break;
                        }
                        DataSendingGRPCTask grpcTask = new DataSendingGRPCTask(tuples, this.circuitBreakerConfig, this.constructor) {
                            @Override
                            public void run() {
                                // For blocking call with result return, nothing to do here.
                            }
                        };
                        if (this.retryConfiguration != null) {
                            grpcTask.withRetry(this.retryConfiguration);
                        }
                        if (retryCtrl == null) {
                            retryCtrl = grpcTask;
                        }
                        result = grpcTask.sendTuplesBlocking(tuples);
                        success = true;
                        break;
                    default:
                        throw new IllegalArgumentException(String.format("un-support request type for sending data: %s", this.requestType));
                }
            } catch (RetryException e) {
                if (retryCtrl != null && retryCtrl.canRetry()) { // Not reach the max retry times
                    this.sleepThenRetry(retryConfiguration.getWaitDurationMillis());
                    continue;
                }
                l.error("{} sendTuplesBlocking with exception ", TAG, e);
                throw e;
            }
        }
        return result;
    }

    private void sleepThenRetry(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            l.error("{} TuplesSenderBlocking retry sleep interval exception", TAG, e);
        }
    }

}
