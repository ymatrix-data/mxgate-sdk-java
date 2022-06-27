package runner;

import cases.objectpool.ObjectPoolTestCase;
import cn.ymatrix.builder.MxBuilder;
import cn.ymatrix.builder.RequestType;

public class E2ECaseRunnerObjectPool extends E2ECaseRunner {

    private E2ECaseRunnerObjectPool() {
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
        r.addTestCase(new ObjectPoolTestCase(builder, r.mxgateHelper));
        r.execute();
    }

}
