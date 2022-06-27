package cn.ymatrix.httpclient;

import cn.ymatrix.api.MxGrpcClient;
import cn.ymatrix.apiserver.GetJobMetadataListener;
import cn.ymatrix.exception.RetryException;
import cn.ymatrix.faulttolerance.RetryStatistic;

public abstract class GetJobMetadataGRPCTask implements Task {
    public GetJobMetadataGRPCTask() {

    }

    public void getJobMetadata(String gRPCHost, String schema, String table, int timeoutMillis, RetryStatistic retryStatistic,
                               GetJobMetadataListener listener) throws NullPointerException, RetryException {
        // No need to create the CSVConstructor for Metadata fetching.
        MxGrpcClient client = MxGrpcClient.prepareMxGrpcClient(gRPCHost, schema, table, timeoutMillis, null, null);
        // Here, we pass a RetryStatistic to the getJobMetadataBlocking method,
        // we want to distinguish under which exception we need to retry and which not.
        client.getJobMetadataBlocking(listener, retryStatistic);
        // This client will be used only once, so, after the getJobMetadataBlocking, we close its channel.
        client.stop();
    }

}
