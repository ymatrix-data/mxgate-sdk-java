package runner;

import cases.http.CircuitBreakerOpenByFailureHTTP;
import cases.http.CircuitBreakerOpenBySlowCallHTTP;
import cn.ymatrix.builder.MxBuilder;
import cn.ymatrix.builder.RequestType;

public class E2ECaseRunnerCircuitBreakerHTTP extends E2ECaseRunner {

    private E2ECaseRunnerCircuitBreakerHTTP() {
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
                .withCircuitBreaker()
                .withMinimumNumberOfCalls(1)
                .withSlidingWindowSize(10)
                .withFailureRateThreshold(60.0f)
                .withSlowCallDurationThresholdMillis(1000)
                .withSlowCallRateThreshold(80.0f)
                .build();
        E2ECaseRunner r = getInstance();

        r.addTestCase(new CircuitBreakerOpenByFailureHTTP(builder, r.mxgateHelper));
        r.addTestCase(new CircuitBreakerOpenBySlowCallHTTP(builder, r.mxgateHelper));
        r.execute();
    }
}
