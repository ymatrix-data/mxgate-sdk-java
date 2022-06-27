package runner;

import cases.http.*;
import cn.ymatrix.builder.MxBuilder;
import cn.ymatrix.builder.RequestType;

public class E2ECaseRunnerAsyncRequestsHTTP extends E2ECaseRunner {
    private E2ECaseRunnerAsyncRequestsHTTP() {
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
                .withRequestAsync(true)
                .withCSVConstructionParallel(10)
                .build();
        E2ECaseRunner r = getInstance();
        r.addTestCase(new AsyncRequestsHTTP(builder, r.mxgateHelper));
        r.addTestCase(new SkipConnectHTTP(builder, r.mxgateHelper));
        r.addTestCase(new SkipConnectWithConfigHTTP(builder, r.mxgateHelper));
        r.addTestCase(new SkipConnectWithGroupHTTP(builder, r.mxgateHelper));
        r.addTestCase(new SkipConnectWithGroupConfigHTTP(builder, r.mxgateHelper));
        r.addTestCase(new ConnectWithGroupHTTP(builder, r.mxgateHelper));
        r.addTestCase(new ConnectWithGroupConfigHTTP(builder, r.mxgateHelper));
        r.addTestCase(new ConnectWithGroupCallbackHTTP(builder, r.mxgateHelper));
        r.addTestCase(new ConnectWithGroupConfigCallbackHTTP(builder, r.mxgateHelper));
        r.addTestCase(new ConnectWithConfigCallbackHTTP(builder, r.mxgateHelper));
        r.execute();
    }

}
