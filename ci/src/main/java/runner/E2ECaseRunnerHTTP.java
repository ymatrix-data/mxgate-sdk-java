package runner;

import cases.http.*;
import cn.ymatrix.builder.MxBuilder;
import cn.ymatrix.builder.RequestType;

public class E2ECaseRunnerHTTP extends E2ECaseRunner {

    private E2ECaseRunnerHTTP() {
        super();
    }

    public static void main(String[] args) {
        MxBuilder builder = MxBuilder.newBuilder()
                .withDropAll(false)
                .withConcurrency(20)
                .withRequestTimeoutMillis(2000)
                .withMaxRetryAttempts(5)
                .withRetryWaitDurationMillis(1500)
                .withRequestType(RequestType.WithHTTP)
                .build();
        E2ECaseRunner r = getInstance();

        r.addTestCase(new SimpleConcurrencyHTTP(builder, r.mxgateHelper));
        r.addTestCase(new SimpleSyncHTTP(builder, r.mxgateHelper));
        r.addTestCase(new MultipleColumnTypeHTTP(builder, r.mxgateHelper));
        r.addTestCase(new VariousEncodedStringHTTP(builder, r.mxgateHelper));
        r.addTestCase(new ExceptedFailureHTTP(builder, r.mxgateHelper));
        r.addTestCase(new WithCompressHTTP(builder, r.mxgateHelper));
        r.addTestCase(new WriterIsNilHTTP(builder, r.mxgateHelper));
        r.execute();
    }
}