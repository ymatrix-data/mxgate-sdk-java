package cn.ymatrix.apiserver;

import cn.ymatrix.builder.CircuitBreakerConfig;
import cn.ymatrix.builder.RequestType;
import cn.ymatrix.builder.ServerConfig;
import org.junit.Assert;
import org.junit.Test;

public class TestServerConfig {

    @Test
    public void TestServerConfigSetAndGetParameters() {
        ServerConfig config = new ServerConfig();
        // By default, the RequestType is GRPC.
        Assert.assertEquals(config.getRequestType(), RequestType.WithGRPC);
        config.setRequestType(RequestType.WithHTTP);
        Assert.assertEquals(config.getRequestType(), RequestType.WithHTTP);
        config.setRequestType(RequestType.WithGRPC);
        Assert.assertEquals(config.getRequestType(), RequestType.WithGRPC);

        config.setConcurrency(100);
        Assert.assertEquals(config.getConcurrency(), 100);

        config.setMaxRetryAttempts(3);
        Assert.assertEquals(config.getMaxRetryAttempts(), 3);

        config.setWaitRetryDurationMillis(200);
        Assert.assertEquals(config.getWaitRetryDurationMillis(), 200);

        config.setCSVConstructionParallel(100);
        Assert.assertEquals(config.getCSVConstructionParallel(), 100);

        config.setDropAll(true);
        Assert.assertTrue(config.isDropAll());
        config.setDropAll(false);
        Assert.assertFalse(config.isDropAll());

        config.setTimeoutMillis(3000);
        Assert.assertEquals(config.getTimeoutMillis(), 3000);

        CircuitBreakerConfig circuitBreakerConfig = new CircuitBreakerConfig();
        circuitBreakerConfig.setEnable();
        circuitBreakerConfig.setMinimumNumberOfCalls(20);
        config.setCircuitBreakerConfig(circuitBreakerConfig);
        Assert.assertEquals(config.getCircuitBreakerConfig(), circuitBreakerConfig);
        Assert.assertEquals(config.getCircuitBreakerConfig().getMinimumNumberOfCalls(), 20);
    }

}
