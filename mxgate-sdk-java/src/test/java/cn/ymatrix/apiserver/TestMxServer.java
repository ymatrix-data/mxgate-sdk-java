package cn.ymatrix.apiserver;

import cn.ymatrix.builder.RequestType;
import cn.ymatrix.builder.ServerConfig;
import cn.ymatrix.faulttolerance.RetryConfiguration;
import cn.ymatrix.logger.MxLogger;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.InvalidParameterException;

public class TestMxServer {
    @Test
    public void TestMxServerShutdownNow() {
        MxServer mxServer = getMxServerInstance();
        Assert.assertFalse(mxServer.isShutdown());
        Assert.assertFalse(mxServer.isTerminated());
        mxServer.shutdownNow();
        Assert.assertTrue(mxServer.isShutdown());
        Assert.assertTrue(mxServer.isTerminated());
    }

    @Test
    public void TestMxServerFactory() {
        MxServer server = MxServerFactory.getMxServerInstance(getServerConfig());
        Assert.assertNotNull(server);
    }

    @Test
    public void TestMxServerInstance() {
        MxServer mxServer = getMxServerInstance();
        Assert.assertNotNull(mxServer);
    }

    @Test(expected = NullPointerException.class)
    public void TestServerCreationWithNullableConfig() {
        MxServerInstance.getInstance(null);
    }

    @Test(expected = InvalidParameterException.class)
    public void TestServerConfigWithInvalidConcurrency() {
        ServerConfig config = new ServerConfig();
        config.setConcurrency(-1);
        MxServerInstance.getInstance(config);
    }

    @Test
    public void TestNeedRetry() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        MxServer server = getMxServerInstance();
        Assert.assertNotNull(server);

        Method needRetryMethod = server.getClass().getDeclaredMethod("needRetry", ServerConfig.class);
        Assert.assertNotNull(needRetryMethod);
        needRetryMethod.setAccessible(true);

        boolean expectedException = false;
        try {
            ServerConfig config = null;
            // Nullable config will invoke an Exception.
            needRetryMethod.invoke(server, config);
        } catch (Exception e) {
            expectedException = true;
        }
        Assert.assertTrue(expectedException);

        try {
            // Reset expected exception flag.
            expectedException = false;
            // Add invalid Server config, an Exception will be thrown.
            ServerConfig config = getServerConfig();
            config.setMaxRetryAttempts(3);
            config.setWaitRetryDurationMillis(-200);
            needRetryMethod.invoke(server, config);
        } catch (Exception e) {
            expectedException = true;
        }
        Assert.assertTrue(expectedException);

        try {
            // Reset expected exception flag.
            expectedException = false;

            // Add invalid Server config, an Exception will be thrown.
            ServerConfig config = getServerConfig();
            config.setMaxRetryAttempts(-3);
            config.setWaitRetryDurationMillis(200);
            needRetryMethod.invoke(server, config);
        } catch (Exception e) {
            expectedException = true;
        }
        Assert.assertTrue(expectedException);

        ServerConfig config = getServerConfig();
        // Set both negative parameters, will get a nullable return value.
        config.setWaitRetryDurationMillis(-200);
        config.setMaxRetryAttempts(-3);
        RetryConfiguration retryConfiguration = (RetryConfiguration) needRetryMethod.invoke(server, config);
        Assert.assertNull(retryConfiguration);

        ServerConfig validConfig = getServerConfig();
        config.setWaitRetryDurationMillis(200);
        config.setMaxRetryAttempts(3);
        RetryConfiguration validRetryConfiguration = (RetryConfiguration) needRetryMethod.invoke(server, config);
        Assert.assertNotNull(validRetryConfiguration);
        Assert.assertEquals(validRetryConfiguration.getWaitDurationMillis(), 200);
        Assert.assertEquals(validRetryConfiguration.getMaxAttempts(), 3);
    }

    private MxServer getMxServerInstance() {
        ServerConfig serverConfig = new ServerConfig();
        serverConfig.setConcurrency(5);
        serverConfig.setMaxRetryAttempts(3);
        serverConfig.setTimeoutMillis(300);
        serverConfig.setDropAll(false);
        serverConfig.setRequestType(RequestType.WithHTTP);
        serverConfig.setWaitRetryDurationMillis(200);
        serverConfig.setCSVConstructionParallel(10);
        return MxServerInstance.getInstance(serverConfig);
    }

    private ServerConfig getServerConfig() {
        ServerConfig serverConfig = new ServerConfig();
        serverConfig.setConcurrency(5);
        serverConfig.setMaxRetryAttempts(3);
        serverConfig.setTimeoutMillis(300);
        serverConfig.setDropAll(false);
        serverConfig.setRequestType(RequestType.WithHTTP);
        serverConfig.setWaitRetryDurationMillis(200);
        serverConfig.setCSVConstructionParallel(10);
        return serverConfig;
    }

}
