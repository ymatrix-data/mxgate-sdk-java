package runner;

import cases.grpc.CircuitBreakerOpenByFailureGRPC;
import cases.grpc.CircuitBreakerOpenBySlowCallGRPC;
import cn.ymatrix.builder.MxBuilder;

public class E2ECaseRunnerCircuitBreakerGRPC extends E2ECaseRunner {
    private E2ECaseRunnerCircuitBreakerGRPC() {
        super();
    }

    public static void main(String[] args) {
        MxBuilder builder = MxBuilder.newBuilder()
                .withConcurrency(1)
                .withRequestTimeoutMillis(2000)
                .withMaxRetryAttempts(2)
                .withRetryWaitDurationMillis(100)
                .withCircuitBreaker()
                .withMinimumNumberOfCalls(1)
                .withSlidingWindowSize(10)
                .withFailureRateThreshold(60.0f)
                .withSlowCallDurationThresholdMillis(1000)
                .withSlowCallRateThreshold(80.0f)
                .build();
        E2ECaseRunner r = getInstance();

        r.addTestCase(new CircuitBreakerOpenByFailureGRPC(builder, r.mxgateHelper));
        r.addTestCase(new CircuitBreakerOpenBySlowCallGRPC(builder, r.mxgateHelper));
        r.execute();
    }
}
