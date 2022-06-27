package cn.ymatrix.worker;

import cn.ymatrix.apiserver.GetJobMetadataListener;
import cn.ymatrix.exception.RetryException;
import cn.ymatrix.faulttolerance.RetryStatistic;
import cn.ymatrix.httpclient.GetJobMetadataGRPCTask;
import cn.ymatrix.utils.StrUtil;

public class JobMetadataManager extends TaskWorker {
    private static final String TAG = StrUtil.logTagWrap(JobMetadataManager.class.getName());

    public JobMetadataManager() {
        super();
    }

    public void getJobMetadata(String gRPCHost, String schema, String table, GetJobMetadataListener listener) {
        // Retry task.
        if (this.retryConfig != null) {
            RetryStatistic rs = new RetryStatistic(this.retryConfig.getMaxAttempts());
            Runnable retryTask = getJobMetadataTaskRetry(gRPCHost, schema, table, rs, listener);
            if (retryTask != null) {
                this.workerPool.join(retryTask);
            } else {
                l.error("{} Get an empty retry task to join for get metadata of table {}.{}", TAG, schema, table);
            }
            return;
        }
        // Raw task.
        this.workerPool.join(getJobMetadataTaskRaw(gRPCHost, schema, table, listener));
    }

    private Runnable getJobMetadataTaskRaw(final String gRPCHost, final String schema, final String table, final GetJobMetadataListener listener) {
        return new GetJobMetadataGRPCTask() {
            @Override
            public void run() {
                try {
                    // For raw task, no retry statistic.
                    this.getJobMetadata(gRPCHost, schema, table, JobMetadataManager.this.getTimeoutMillis(), null, listener);
                } catch (Exception e) { // For raw task, we will not perform retry and try catch all the exceptions.
                    l.error("{} Get job meta data exception with raw task ", TAG, e);
                }
            }
        };
    }

    private Runnable getJobMetadataTaskRetry(final String gRPCHost, final String schema, final String table, final RetryStatistic rs,
                                             final GetJobMetadataListener listener) {
        Runnable taskRaw = new GetJobMetadataGRPCTask() {
            @Override
            public void run() throws RetryException { // For retry task, we will throw RetryException.
                try {
                    this.getJobMetadata(gRPCHost, schema, table, JobMetadataManager.this.getTimeoutMillis(), rs, listener);
                } catch (Exception e) {
                    if (e instanceof RetryException) {
                        l.error("{} Get job meta data exception with retry {}", TAG, e.getMessage());
                        throw e;
                    }
                    l.error("{} Get job meta data exception with retry", TAG, e);
                }
            }
        };
        return decorateTaskWithRetry("GetJobMetadata", taskRaw);
    }

}
