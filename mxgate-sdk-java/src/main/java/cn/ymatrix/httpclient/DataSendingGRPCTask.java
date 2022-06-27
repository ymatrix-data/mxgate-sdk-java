package cn.ymatrix.httpclient;

import cn.ymatrix.api.MxGrpcClient;
import cn.ymatrix.api.MxGrpcClientManager;
import cn.ymatrix.api.StatusCode;
import cn.ymatrix.apiserver.SendDataListener;
import cn.ymatrix.apiserver.SendDataResult;
import cn.ymatrix.builder.CircuitBreakerConfig;
import cn.ymatrix.builder.MxBuilder;
import cn.ymatrix.data.Tuples;
import cn.ymatrix.exception.RetryException;
import cn.ymatrix.faulttolerance.RetryConfiguration;
import cn.ymatrix.faulttolerance.RetryControl;
import cn.ymatrix.faulttolerance.RetryStatistic;
import cn.ymatrix.logger.MxLogger;
import cn.ymatrix.utils.CSVConstructor;
import cn.ymatrix.utils.StrUtil;
import org.slf4j.Logger;

import java.nio.charset.StandardCharsets;

public abstract class DataSendingGRPCTask implements Task, RetryControl {
    private static final String TAG = StrUtil.logTagWrap(DataSendingGRPCTask.class.getName()) + MxBuilder.SDK_VERSION;
    static final Logger l = MxLogger.init(DataSendingGRPCTask.class);
    private final MxGrpcClient client;
    private RetryConfiguration retryConfiguration;
    private RetryStatistic rs;
    private SendDataListener listener;
    private final CSVConstructor constructor;

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

    public DataSendingGRPCTask(Tuples tuples, CircuitBreakerConfig circuitBreakerConfig, CSVConstructor constructor) throws NullPointerException {
        if (tuples == null) {
            throw new NullPointerException("Create DataSendingGRPCTask on a null tuples.");
        }
        if (tuples.getTarget() == null || tuples.getTarget().getURL() == null) {
            throw new NullPointerException("Create DataSendingGRPCTask on a null tuples target.");
        }
        if (tuples.getSchema() == null) {
            throw new NullPointerException("Create DataSendingGRPCTask on a null tuples schema.");
        }
        if (tuples.getTable() == null) {
            throw new NullPointerException("Create DataSendingGRPCTask on a null tuples table.");
        }
        if (constructor == null) {
            throw new NullPointerException("CSVConstructor is null during the creation of DataSendingGRPCTask.");
        }

        this.constructor = constructor;
        client = MxGrpcClientManager.getInstance().prepareClient(tuples.getTarget().getURL(), tuples.getSchema(),
                tuples.getTable(), tuples.getTarget().getTimeout(), circuitBreakerConfig, this.constructor);
        l.debug("{} init DataSendingGRPCTask for table {}.{}.", TAG, tuples.getSchema(), tuples.getTable());
    }

    /**
     * This method maybe called in multiple threads, so, we use synchronized to protect it.
     * NullPointerException, maybe the caller only need to print a log of the nullable Tuples.
     * RetryException: need to retry;
     * Another unexpected Exception: the caller need to try catch and this exception means that the sending of all the tuples
     * are fail, and the MxClient need to know this kind of failure.
     */
    public synchronized SendDataResult sendTuplesBlocking(final Tuples tuples) throws NullPointerException, RetryException {
        if (tuples == null) {
            throw new NullPointerException("send request with a null Tuples in gRPC blocking mode.");
        }
        try {
            long startTimeMillis = System.currentTimeMillis();
            SendDataResult result = client.sendDataBlocking(tuples, rs, tuples.needCompress(), tuples.needBase64Encoding4CompressedBytes(), listener);
            long endTimeMillis = System.currentTimeMillis();
            l.info("{} Send {} tuples in gRPC blocking mode (status = {}, time_cost_millis = {})",
                    TAG, tuples.size(), result.getCode(), endTimeMillis - startTimeMillis);
            return result;
        } catch (RetryException e) {
            l.error("{} Send data blocking with retry exception", TAG, e);
            throw e;
        } catch (Exception e) {
            l.error("{} Send data in gRPC blocking mode with exception", TAG, e);
            if (this.listener != null) {
                this.listener.onFailure(handleAllTuplesFailure(tuples.getSchema(), tuples.getTable()), tuples);
            }
        }
        return null;
    }

    public synchronized void sendTuplesAsync(final Tuples tuples) throws NullPointerException, RetryException {
        if (tuples == null) {
            throw new NullPointerException("send request with a null Tuples in gRPC async mode.");
        }
        try {
            long startTimeMillis = System.currentTimeMillis();

            if (constructor == null) {
                l.error("{} CSV constructor is null in sendTuplesAsync.", TAG);
                throw new NullPointerException("CSV constructor is null in sendTuplesAsync.");
            }

            StringBuilder rawCSVData = constructor.constructCSVFromTuplesWithTasks(tuples.getTuplesList(), tuples.getCSVBatchSize(), tuples.getDelimiter());
            if (rawCSVData == null) {
                l.error("{} Get null CSV Data from CSV constructor", TAG);
                throw new NullPointerException("Get null CSV Data from CSV constructor");
            }

            String tupleData = new String(rawCSVData.toString().getBytes(StandardCharsets.UTF_8), StandardCharsets.UTF_8);
            client.sendDataAsync(tuples, tupleData, tuples.needCompress(), tuples.needBase64Encoding4CompressedBytes(), this.listener, this.rs);
            long endTimeMillis = System.currentTimeMillis();
            l.info("{} Send {} tuples in gRPC async mode (time_cost_millis = {})",
                    TAG, tuples.size(), endTimeMillis - startTimeMillis);
        } catch (RetryException e) {
            l.error("{} Send data in gRPC async mode with exception(retry)", TAG, e);
            throw e;
        } catch (Exception e) {
            l.error("{} Send data in gRPC async mode with unexpected exception", TAG, e);
            if (this.listener != null) {
                this.listener.onFailure(handleAllTuplesFailure(tuples.getSchema(), tuples.getTable()), tuples);
            }
        }
    }

    private SendDataResult handleAllTuplesFailure(String schema, String table) {
        String errMsg = "All tuples fail for table " + schema + "." + table;
        return new SendDataResult(StatusCode.ALL_TUPLES_FAIL, null, errMsg);
    }
}



















