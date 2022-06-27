package runner;

import cases.grpc.*;
import cn.ymatrix.builder.MxBuilder;
import cn.ymatrix.builder.RequestType;

public class E2ECaseRunnerGRPC extends E2ECaseRunner {

    private E2ECaseRunnerGRPC() {
        super();
    }

    public static void main(String[] args) {
        MxBuilder builder = MxBuilder.newBuilder()
                .withConcurrency(1)
                .withRequestTimeoutMillis(2000)
                .withMaxRetryAttempts(5)
                .withRetryWaitDurationMillis(1500)
                .withRequestType(RequestType.WithGRPC)
                .build();
        E2ECaseRunner r = getInstance();

        r.addTestCase(new SimpleConcurrencyGRPC(builder, r.mxgateHelper));
        r.addTestCase(new SimpleSyncGRPC(builder, r.mxgateHelper));
        r.addTestCase(new MultipleColumnTypeGRPC(builder, r.mxgateHelper));
        r.addTestCase(new VariousEncodedStringGRPC(builder, r.mxgateHelper));
        r.addTestCase(new WithCompressGRPC(builder, r.mxgateHelper));
        r.addTestCase(new WriterIsNilGRPC(builder, r.mxgateHelper));
        r.addTestCase(new ExceptedFailureGRPC(builder, r.mxgateHelper));
        r.execute();
    }
}
