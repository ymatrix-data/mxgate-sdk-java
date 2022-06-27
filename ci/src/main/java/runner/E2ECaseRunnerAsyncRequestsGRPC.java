package runner;

import cases.grpc.*;
import cn.ymatrix.builder.MxBuilder;
import cn.ymatrix.builder.RequestType;

public class E2ECaseRunnerAsyncRequestsGRPC extends E2ECaseRunner {

    private E2ECaseRunnerAsyncRequestsGRPC() {
        super();
    }

    public static void main(String[] args) {
        MxBuilder builder = MxBuilder.newBuilder()
                .withDropAll(false)
                .withConcurrency(20)
                .withRequestTimeoutMillis(2000)
                .withMaxRetryAttempts(5)
                .withRetryWaitDurationMillis(1500)
                .withRequestType(RequestType.WithGRPC)
                .withRequestAsync(true)
                .withCSVConstructionParallel(10)
                .build();
        E2ECaseRunner r = getInstance();
        r.addTestCase(new AsyncRequestsGRPC(builder, r.mxgateHelper));
        r.addTestCase(new ConnectWithConfigCallbackGRPC(builder, r.mxgateHelper));
        r.addTestCase(new ConnectWithGroupCallbackGRPC(builder, r.mxgateHelper));
        r.addTestCase(new ConnectWithGroupConfigCallbackGRPC(builder, r.mxgateHelper));
        r.addTestCase(new ConnectWithGroupGRPC(builder, r.mxgateHelper));
        r.addTestCase(new ConnectWithGroupConfigGRPC(builder, r.mxgateHelper));
        r.addTestCase(new SkipConnectGRPC(builder, r.mxgateHelper));
        r.addTestCase(new SkipConnectWithConfigGRPC(builder, r.mxgateHelper));
        r.addTestCase(new SkipConnectWithGroupGRPC(builder, r.mxgateHelper));
        r.addTestCase(new SkipConnectWithGroupConfigGRPC(builder, r.mxgateHelper));
        r.execute();
    }

}
