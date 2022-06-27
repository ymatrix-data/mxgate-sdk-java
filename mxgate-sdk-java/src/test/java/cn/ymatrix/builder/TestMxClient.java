package cn.ymatrix.builder;

import cn.ymatrix.api.JobMetadataWrapper;
import cn.ymatrix.api.StatusCode;
import cn.ymatrix.apiclient.DataPostListener;
import cn.ymatrix.apiclient.Result;
import cn.ymatrix.apiserver.MxServer;
import cn.ymatrix.apiserver.MxServerFactory;
import cn.ymatrix.apiserver.SendDataResult;
import cn.ymatrix.cache.Cache;
import cn.ymatrix.cache.CacheFactory;
import cn.ymatrix.data.Column;
import cn.ymatrix.data.Tuple;
import cn.ymatrix.data.Tuples;
import cn.ymatrix.data.TuplesTarget;
import cn.ymatrix.exception.AllTuplesFailException;
import cn.ymatrix.exception.ClientClosedException;
import cn.ymatrix.exception.TupleNotReadyException;
import cn.ymatrix.faulttolerance.CircuitBreakerFactory;
import cn.ymatrix.httpclient.MockHttpServer;
import cn.ymatrix.logger.MxLogger;
import cn.ymatrix.messagecenter.ResultMessageCenter;
import cn.ymatrix.messagecenter.ResultMessageQueue;
import cn.ymatrix.utils.Utils;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import org.checkerframework.checker.units.qual.C;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class TestMxClient {
    private static final String schema = "public";
    private static final String table = "test_table";
    private static final int SERVER_PORT = 8087;

    private static final String SERVER_URL_GRPC = String.format("localhost:%d/", SERVER_PORT);
    private static final String SERVER_URL_HTTP = String.format("http://localhost:%d/", SERVER_PORT);
    static Logger l = MxLogger.init(TestMxClient.class);

    @Test(expected = InvalidParameterException.class)
    public void TestMxClientCreationInvalidSerialNumber() {
        ClientConfig clientConfig = prepareClientConfig("TestMxClientCreationInvalidSerialNumber");
        HTTPConfig httpConfig = prepareHTTPConfig();
        ServerConfig serverConfig = prepareServerConfig();
        // Serial number is invalid and will throw InvalidParameterException.
        new MxClientImpl(-1, clientConfig, httpConfig,
                CacheFactory.getCacheInstance(), MxServerFactory.getMxServerInstance(serverConfig));
    }

    @Test(expected = NullPointerException.class)
    public void TestMxClientCreationNullClientConfig() {
        // ClientConfig is null, throw NullPointerException.
        new MxClientImpl(1, null, prepareHTTPConfig(),
                CacheFactory.getCacheInstance(), MxServerFactory.getMxServerInstance(prepareServerConfig()));
    }

    @Test(expected = NullPointerException.class)
    public void TestMxClientCreationNullHttpConfig() {
        new MxClientImpl(1, prepareClientConfig("TestMxClientCreationNullHttpConfig"), null,
                CacheFactory.getCacheInstance(), MxServerFactory.getMxServerInstance(prepareServerConfig()));
    }

    @Test(expected = NullPointerException.class)
    public void TestMxClientCreationNullCache() {
        new MxClientImpl(1, prepareClientConfig("TestMxClientCreationNullCache"), prepareHTTPConfig(),
                null, MxServerFactory.getMxServerInstance(prepareServerConfig()));
    }

    @Test(expected = NullPointerException.class)
    public void TestMxClientCreationNullMxServer() {
        new MxClientImpl(1, prepareClientConfig("TestMxClientCreationNullMxServer"), prepareHTTPConfig(),
                CacheFactory.getCacheInstance(), null);
    }

    @Test
    public void TestMxClientCreationNullConnectionListener() {
        new MxClientImpl(1, prepareClientConfig("TestMxClientCreationNullConnectionListener"), prepareHTTPConfig(),
                CacheFactory.getCacheInstance(), MxServerFactory.getMxServerInstance(prepareServerConfig()));
    }

    @Test
    public void TestMxClientGetClientName() {
        try {
            MxClientImpl client = prepareMxClientWithServerShutdown(CacheFactory.getCacheInstance(),
                    MxServerFactory.getMxServerInstance(prepareServerConfig()), "TestMxClientGetClientName");
            Assert.assertNotNull(client);
            Method method = MxClientImpl.class.getDeclaredMethod("getClientName");
            method.setAccessible(true);
            String clientName = (String) method.invoke(client);
            Assert.assertEquals("public.TestMxClientGetClientName#1", clientName);
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            l.error("TestMxClientGetClientName exception ", e);
            throw new RuntimeException(e);
        }
    }

    @Test
    public void TestGetCloseWaitTimeMillis() {
        try {
            MxClientImpl client = prepareMxClientWithServerShutdown(CacheFactory.getCacheInstance(),
                    MxServerFactory.getMxServerInstance(prepareServerConfig()), "TestGetCloseWaitTimeMillis");
            Assert.assertNotNull(client);
            Method method = MxClientImpl.class.getDeclaredMethod("getCloseWaitTimeMillis");
            method.setAccessible(true);
            int waitTimeMillis = (int) method.invoke(client);
            Assert.assertEquals((300 + 500) * 3, waitTimeMillis);
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            l.error("TestGetCloseWaitTimeMillis exception ", e);
            throw new RuntimeException(e);
        }
    }

    @Test
    public void TestHandleSendDataResultWithNullResult() {
        boolean invocationTargetException = false;
        try {
            MxClientImpl client = prepareMxClientWithServerShutdown(CacheFactory.getCacheInstance(),
                    MxServerFactory.getMxServerInstance(prepareServerConfig()), "TestHandleSendDataResult");
            Assert.assertNotNull(client);
            Method method = MxClientImpl.class.getDeclaredMethod("handleSendDataResult", SendDataResult.class, Tuples.class);
            Assert.assertNotNull(method);
            method.setAccessible(true);
            method.invoke(client, null, getTuples());
        } catch (Exception e) {
            if (e instanceof InvocationTargetException) {
                invocationTargetException = true;
            } else {
                throw new RuntimeException(e);
            }
        }
        Assert.assertTrue(invocationTargetException);
    }

    @Test
    public void TestHandleSendDataResultWithNormalStatus() {
        try {
            MxClientImpl client = prepareMxClientWithServerShutdown(CacheFactory.getCacheInstance(),
                    MxServerFactory.getMxServerInstance(prepareServerConfig()), "TestHandleSendDataResult");
            Assert.assertNotNull(client);
            Method method = MxClientImpl.class.getDeclaredMethod("handleSendDataResult", SendDataResult.class, Tuples.class);
            Assert.assertNotNull(method);
            method.setAccessible(true);
            SendDataResult result = new SendDataResult(StatusCode.NORMAL, null, null);
            method.invoke(client, result, getTuples());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void TestHandleSendDataResultWithStatusError() {
        boolean invocationTargetException = false;
        try {
            MxClientImpl client = prepareMxClientWithServerShutdown(CacheFactory.getCacheInstance(),
                    MxServerFactory.getMxServerInstance(prepareServerConfig()), "TestHandleSendDataResult");
            Assert.assertNotNull(client);
            Method method = MxClientImpl.class.getDeclaredMethod("handleSendDataResult", SendDataResult.class, Tuples.class);
            Assert.assertNotNull(method);
            method.setAccessible(true);
            SendDataResult result = new SendDataResult(StatusCode.ERROR, null, null);
            method.invoke(client, result, getTuples());
        } catch (Exception e) {
            if (e instanceof InvocationTargetException) {
                invocationTargetException = true;
            } else {
                throw new RuntimeException(e);
            }
        }
        Assert.assertTrue(invocationTargetException);
    }

    @Test
    public void TestHandleSendDataResultWithStatusAllTuplesFailed() {
        boolean invocationTargetException = false;
        try {
            MxClientImpl client = prepareMxClientWithServerShutdown(CacheFactory.getCacheInstance(),
                    MxServerFactory.getMxServerInstance(prepareServerConfig()), "TestHandleSendDataResult");
            Assert.assertNotNull(client);
            Method method = MxClientImpl.class.getDeclaredMethod("handleSendDataResult", SendDataResult.class, Tuples.class);
            Assert.assertNotNull(method);
            method.setAccessible(true);
            SendDataResult result = new SendDataResult(StatusCode.ALL_TUPLES_FAIL, null, null);
            method.invoke(client, result, getTuples());
        } catch (Exception e) {
            if (e instanceof InvocationTargetException) {
                invocationTargetException = true;
            } else {
                throw new RuntimeException(e);
            }
        }
        Assert.assertTrue(invocationTargetException);
    }

    @Test
    public void TestHandleSendDataResultWithStatusPartiallyTuplesFailed() {
        boolean invocationTargetException = false;
        try {
            MxClientImpl client = prepareMxClientWithServerShutdown(CacheFactory.getCacheInstance(),
                    MxServerFactory.getMxServerInstance(prepareServerConfig()), "TestHandleSendDataResult");
            Assert.assertNotNull(client);
            Method method = MxClientImpl.class.getDeclaredMethod("handleSendDataResult", SendDataResult.class, Tuples.class);
            Assert.assertNotNull(method);
            method.setAccessible(true);
            SendDataResult result = new SendDataResult(StatusCode.PARTIALLY_TUPLES_FAIL, null, null);
            method.invoke(client, result, getTuples());
        } catch (Exception e) {
            if (e instanceof InvocationTargetException) {
                invocationTargetException = true;
            } else {
                throw new RuntimeException(e);
            }
        }
        Assert.assertTrue(invocationTargetException);
    }

    @Test
    public void TestMxClientAppendTupleWithException() {
        Cache cache = CacheFactory.getCacheInstance(10, 1000);
        MxServer server = MxServerFactory.getMxServerInstance(prepareServerConfig());
        MxClientImpl client = new MxClientImpl(1, prepareClientConfig("TestMxClientFlushWithException"), prepareHTTPConfig(), cache, server);
        client.setDelimiter("|");
        client.withEnoughLinesToFlush(1); // Each Tuple to flush.
        for (int i = 0; i < 10; i++) {
            Tuple tuple = client.generateEmptyTupleLite();
            client.appendTuple(tuple);
        }
        Assert.assertEquals(10, cache.size());
        Tuple tuple = client.generateEmptyTupleLite();
        long testSerialNumFlag = 11199;
        tuple.setSerialNum(testSerialNumFlag);
        final long[] failureSerialNum = {-1};
        client.registerDataPostListener(new DataPostListener() {
            @Override
            public void onSuccess(Result result) {

            }

            @Override
            public void onFailure(Result result) {
                Assert.assertEquals(result.getErrorTuplesMap().size(), 1);
                for (Map.Entry<Tuple, String> entry : result.getErrorTuplesMap().entrySet()) {
                    Tuple tuple = entry.getKey();
                    Assert.assertNotNull(tuple);
                    failureSerialNum[0] = tuple.getSerialNum();
                }
            }
        });
        client.appendTuple(tuple);
        Assert.assertEquals(testSerialNumFlag, failureSerialNum[0]);
    }

    @Test
    public void TestMxClientAppendTupleListWithException() {
        Cache cache = CacheFactory.getCacheInstance(10, 1000);
        MxServer server = MxServerFactory.getMxServerInstance(prepareServerConfig());
        MxClientImpl client = new MxClientImpl(1, prepareClientConfig("TestMxClientFlushWithException"), prepareHTTPConfig(), cache, server);
        client.setDelimiter("|");
        client.withEnoughLinesToFlush(2); // Each Tuple to flush.
        for (int i = 0; i < 10; i++) {
            Tuple tuple = client.generateEmptyTupleLite();
            client.appendTuple(tuple);
        }
        Assert.assertEquals(5, cache.size());

        Tuple tuple1 = client.generateEmptyTupleLite();
        long testSerialNumFlag = 11199;
        tuple1.setSerialNum(testSerialNumFlag);

        Tuple tuple2 = client.generateEmptyTupleLite();
        tuple2.setSerialNum(testSerialNumFlag);

        List<Tuple> tupleList = new ArrayList<>();
        tupleList.add(tuple1);
        tupleList.add(tuple2);

        client.registerDataPostListener(new DataPostListener() {
            @Override
            public void onSuccess(Result result) {

            }

            @Override
            public void onFailure(Result result) {
                Assert.assertEquals(result.getErrorTuplesMap().size(), 2);
                for (Map.Entry<Tuple, String> entry : result.getErrorTuplesMap().entrySet()) {
                    Tuple tuple = entry.getKey();
                    Assert.assertNotNull(tuple);
                    Assert.assertEquals(tuple.getSerialNum(), testSerialNumFlag);
                }
            }
        });
        client.appendTuplesList(tupleList);
    }

    @Test
    public void TestMxClientAppendTuplesWithException() {
        Cache cache = CacheFactory.getCacheInstance(10, 1000);
        MxServer server = MxServerFactory.getMxServerInstance(prepareServerConfig());
        MxClientImpl client = new MxClientImpl(1, prepareClientConfig("TestMxClientFlushWithException"), prepareHTTPConfig(), cache, server);
        client.setDelimiter("|");
        client.withEnoughLinesToFlush(2); // Each Tuple to flush.
        for (int i = 0; i < 10; i++) {
            Tuple tuple = client.generateEmptyTupleLite();
            client.appendTuple(tuple);
        }
        Assert.assertEquals(5, cache.size());

        Tuple tuple1 = client.generateEmptyTupleLite();
        long testSerialNumFlag = 11199;
        tuple1.setSerialNum(testSerialNumFlag);

        Tuple tuple2 = client.generateEmptyTupleLite();
        tuple2.setSerialNum(testSerialNumFlag);

        client.registerDataPostListener(new DataPostListener() {
            @Override
            public void onSuccess(Result result) {

            }

            @Override
            public void onFailure(Result result) {
                Assert.assertEquals(result.getErrorTuplesMap().size(), 2);
                for (Map.Entry<Tuple, String> entry : result.getErrorTuplesMap().entrySet()) {
                    Tuple tuple = entry.getKey();
                    Assert.assertNotNull(tuple);
                    Assert.assertEquals(tuple.getSerialNum(), testSerialNumFlag);
                }
            }
        });
        client.appendTuples(tuple1, tuple2);
    }

    @Test
    public void TestMxClientFlushBlockingWithExceptionHTTP() {
        Cache cache = CacheFactory.getCacheInstance(10, 1000);
        ServerConfig sc = new ServerConfig();
        sc.setRequestType(RequestType.WithHTTP);
        sc.setConcurrency(10);
        sc.setTimeoutMillis(500);
        sc.setMaxRetryAttempts(3);
        sc.setWaitRetryDurationMillis(300);
        sc.setCSVConstructionParallel(10);
        MxServer server = MxServerFactory.getMxServerInstance(sc);

        HTTPConfig hc = new HTTPConfig();
        hc.setWaitRetryDurationLimitation(300);
        hc.setMaxRetryAttempts(3);
        hc.setServerURLGRPC("fake_grpc_url");
        hc.setServerURLDataSending("fake_grpc_http");
        hc.setRequestTimeoutMillis(500);
        MxClientImpl client = new MxClientImpl(1, prepareClientConfig("TestMxClientFlushWithException"), hc, cache, server);
        client.setDelimiter("|");
        client.withEnoughLinesToFlush(1); // Each Tuple to flush.
        for (int i = 0; i < 10; i++) {
            Tuple tuple = client.generateEmptyTupleLite();
            client.appendTupleBlocking(tuple);
        }
        boolean exceptionHappened = false;
        try {
            client.flushBlocking();
        } catch (AllTuplesFailException e) {
            exceptionHappened = true;
            Assert.assertEquals(e.getResult().getErrorTuplesMap().size(), 10);
        }
        Assert.assertTrue(exceptionHappened);
    }

    @Test
    public void TestMxClientFlushBlockingWithExceptionGRPC() {
        Cache cache = CacheFactory.getCacheInstance(10, 1000);
        ServerConfig sc = new ServerConfig();
        sc.setRequestType(RequestType.WithGRPC);
        sc.setConcurrency(10);
        sc.setTimeoutMillis(500);
        sc.setMaxRetryAttempts(3);
        sc.setWaitRetryDurationMillis(300);
        sc.setCSVConstructionParallel(10);
        MxServer server = MxServerFactory.getMxServerInstance(sc);

        HTTPConfig hc = new HTTPConfig();
        hc.setWaitRetryDurationLimitation(300);
        hc.setMaxRetryAttempts(3);
        hc.setServerURLGRPC("fake_grpc_url");
        hc.setServerURLDataSending("fake_grpc_http");
        hc.setRequestTimeoutMillis(500);
        MxClientImpl client = new MxClientImpl(1, prepareClientConfig("TestMxClientFlushWithException"), hc, cache, server);
        client.setDelimiter("|");
        client.withEnoughLinesToFlush(1); // Each Tuple to flush.

        for (int i = 0; i < 10; i++) {
            Tuple tuple = client.generateEmptyTupleLite();
            client.appendTupleBlocking(tuple);
        }
        boolean exceptionHappened = false;
        try {
            client.flushBlocking();
        } catch (AllTuplesFailException e) {
            exceptionHappened = true;
            Assert.assertEquals(e.getResult().getErrorTuplesMap().size(), 10);
        }
        Assert.assertTrue(exceptionHappened);
    }


    @Test
    public void TestRunScheduledFlushTask() {
        try {
            Cache cache = CacheFactory.getCacheInstance();
            MxServer server = MxServerFactory.getMxServerInstance(prepareServerConfig());
            MxClientImpl client = prepareMxClientWithServerShutdown(cache, server, "TestRunScheduledFlushTask");
            client.setDelimiter("|");
            Assert.assertNotNull(client);

            // Add tuple to the list.
            Field field = MxClientImpl.class.getDeclaredField("tmpTuplesList");
            field.setAccessible(true);
            List<Tuple> tmpTuplesList = (List<Tuple>) field.get(client);
            Assert.assertNotNull(tmpTuplesList);
            Tuple emptyTuple = prepareEmptyTuple(1);
            tmpTuplesList.add(emptyTuple);

            // The schedule will flush the Tuple into the cache.
            Method method = MxClientImpl.class.getDeclaredMethod("runScheduledFlushTask");
            Assert.assertNotNull(method);
            method.setAccessible(true);
            method.invoke(client);

            // Consume the tuple from the cache.
            Tuples tuples = cache.get();
            Assert.assertNotNull(tuples);
            l.info("Get tuples from cache size = {}", tuples.size());
            Tuple tuple = tuples.getTupleByIndex(0);
            Assert.assertNotNull(tuple);
            l.info("Get tuple from cache size = {}", tuple.size());
            Assert.assertEquals(emptyTuple, tuple);
            Assert.assertEquals(1, tuple.size());

            // Shutdown the schedule.
            Method shutdownScheduleTask = MxClientImpl.class.getDeclaredMethod("stopScheduledFlushTask");
            Assert.assertNotNull(shutdownScheduleTask);
            shutdownScheduleTask.setAccessible(true);
            shutdownScheduleTask.invoke(client);
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException | NoSuchFieldException |
                 InterruptedException e) {
            l.error("TestRunScheduledFlushTask exception ", e);
            throw new RuntimeException(e);
        }
    }

    @Test
    public void TestPrepareMetadata() {
        try {
            Cache cache = CacheFactory.getCacheInstance();
            MxServer server = MxServerFactory.getMxServerInstance(prepareServerConfig());
            MxClientImpl client = prepareMxClientWithServerShutdown(cache, server, "TestPrepareMetadata");
            Assert.assertNotNull(client);

            Method method = MxClientImpl.class.getDeclaredMethod("prepareMetadata", JobMetadataWrapper.class);
            Assert.assertNotNull(method);
            method.setAccessible(true);
            method.invoke(client, JobMetadataWrapper.wrapJobMetadata(Utils.prepareJobMetadata(schema, table, "|", 5, "c")));

            Field delimiterField = MxClientImpl.class.getDeclaredField("delimiter");
            Assert.assertNotNull(delimiterField);
            delimiterField.setAccessible(true);
            String delimiter = (String) delimiterField.get(client);
            Assert.assertEquals("|", delimiter);

            Field field = MxClientImpl.class.getDeclaredField("columnMetaMap");
            Assert.assertNotNull(field);
            field.setAccessible(true);
            Map<String, ColumnMetadata> columnMetadataMap = (Map<String, ColumnMetadata>) field.get(client);
            Assert.assertNotNull(columnMetadataMap);
            Assert.assertEquals(5, columnMetadataMap.size());

            boolean c0Found = false;
            boolean c1Found = false;
            boolean c2Found = false;
            boolean c3Found = false;
            boolean c4Found = false;
            for (Map.Entry<String, ColumnMetadata> entry : columnMetadataMap.entrySet()) {
                Assert.assertNotNull(entry);
                l.info(entry.getKey());
                if (entry.getKey().equals("c0")) {
                    c0Found = true;
                }
                if (entry.getKey().equals("c1")) {
                    c1Found = true;
                }
                if (entry.getKey().equals("c2")) {
                    c2Found = true;
                }
                if (entry.getKey().equals("c3")) {
                    c3Found = true;
                }
                if (entry.getKey().equals("c4")) {
                    c4Found = true;
                }
            }
            Assert.assertTrue(c0Found);
            Assert.assertTrue(c1Found);
            Assert.assertTrue(c2Found);
            Assert.assertTrue(c3Found);
            Assert.assertTrue(c4Found);
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | NoSuchFieldException e) {
            l.error("TestPrepareMetadata exception", e);
            throw new RuntimeException(e);
        }
    }

    @Test
    public void TestThrowExceptionIfRequestNoThrow() {
        try {
            Cache cache = CacheFactory.getCacheInstance();
            MxServer server = MxServerFactory.getMxServerInstance(prepareServerConfig());
            MxClientImpl client = prepareMxClientWithServerShutdown(cache, server, "TestThrowExceptionIfRequestNoThrow");
            Assert.assertNotNull(client);

            Method method = MxClientImpl.class.getDeclaredMethod("throwClientClosedExceptionIfRequest");
            Assert.assertNotNull(method);

            method.setAccessible(true);
            method.invoke(client);
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            l.error("TestThrowExceptionIfRequest exception ", e);
            throw new RuntimeException(e);
        }
    }

    @Test(expected = InvocationTargetException.class)
    public void TestThrowExceptionIfRequestThrow() throws InvocationTargetException {
        try {
            Cache cache = CacheFactory.getCacheInstance();
            MxServer server = MxServerFactory.getMxServerInstance(prepareServerConfig());
            MxClientImpl client = prepareMxClientWithServerShutdown(cache, server, "TestThrowExceptionIfRequestThrow");
            Assert.assertNotNull(client);

            Field field = MxClientImpl.class.getDeclaredField("close");
            Assert.assertNotNull(field);
            field.setAccessible(true);

            AtomicBoolean close = (AtomicBoolean) field.get(client);
            Assert.assertNotNull(close);

            // Set the close state to true.
            close.set(true);

            Method method = MxClientImpl.class.getDeclaredMethod("throwClientClosedExceptionIfRequest");
            Assert.assertNotNull(method);
            method.setAccessible(true);

            // An InvocationTargetException will be thrown, for the reason that close state is true.
            method.invoke(client);
        } catch (NoSuchMethodException | IllegalAccessException | NoSuchFieldException e) {
            l.error("TestThrowExceptionIfRequest exception ", e);
            throw new RuntimeException(e);
        }
    }

    @Test
    public void TestGenerateTuples() {
        try {
            // Prepare MxClient.
            Cache cache = CacheFactory.getCacheInstance();
            MxServer server = MxServerFactory.getMxServerInstance(prepareServerConfig());
            MxClientImpl client = prepareMxClientWithServerShutdown(cache, server, "TestGenerateTuples");
            client.setDelimiter("|");
            Assert.assertNotNull(client);

            // Generate Tuples.
            Method generateTuples = MxClientImpl.class.getDeclaredMethod("generateTuples");
            Assert.assertNotNull(generateTuples);
            generateTuples.setAccessible(true);

            Tuples tuples = (Tuples) generateTuples.invoke(client);
            Assert.assertNotNull(tuples);
            Assert.assertEquals(schema, tuples.getSchema());
            Assert.assertEquals("TestGenerateTuples", tuples.getTable());
            Assert.assertEquals(0, tuples.size());
            Assert.assertNotNull(tuples.getTarget());
            Assert.assertEquals(SERVER_URL_HTTP, tuples.getTarget().getURL());
            Assert.assertEquals(schema + "." + "TestGenerateTuples" + "#1", tuples.getSenderID());
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    @Test(expected = NullPointerException.class)
    public void TestAppendNullTuple() {
        // Prepare MxClient.
        Cache cache = CacheFactory.getCacheInstance();
        MxServer server = MxServerFactory.getMxServerInstance(prepareServerConfig());
        MxClientImpl client = prepareMxClientWithServerShutdown(cache, server, "TestAppendNullTuple");
        Assert.assertNotNull(client);
        client.appendTuple(null);
    }

    @Test
    public void TestGenerateEmptyTuple() {
        try {
            // Prepare MxClient.
            Cache cache = CacheFactory.getCacheInstance();
            MxServer server = MxServerFactory.getMxServerInstance(prepareServerConfig());
            MxClientImpl client = prepareMxClientWithServerShutdown(cache, server, "TestGenerateEmptyTuple");
            client.setDelimiter("|");
            Assert.assertNotNull(client);

            Method method = MxClientImpl.class.getDeclaredMethod("prepareMetadata", JobMetadataWrapper.class);
            Assert.assertNotNull(method);
            method.setAccessible(true);
            int columnLength = 5;
            method.invoke(client, JobMetadataWrapper.wrapJobMetadata(Utils.prepareJobMetadata(schema, table, "|", columnLength, "c")));

            Tuple tuple = client.generateEmptyTuple();
            Assert.assertNotNull(tuple);
            Assert.assertNotNull(tuple.getColumns());
            Assert.assertEquals(columnLength, tuple.getColumns().length);
            Assert.assertEquals(schema + "." + "TestGenerateEmptyTuple", tuple.getTableName());

            for (int i = 0; i < tuple.getColumns().length; i++) {
                Assert.assertNotNull(tuple.getColumns()[i]);
                Assert.assertEquals("c" + i, tuple.getColumns()[i].getColumnName());
            }
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            l.error("TestGenerateEmptyTuple exception", e);
            throw new RuntimeException(e);
        }
    }

    @Test
    public void TestAppendTupleNotNull() {
        try {
            // Prepare MxClient.
            Cache cache = CacheFactory.getCacheInstance();
            MxServer server = MxServerFactory.getMxServerInstance(prepareServerConfig());
            MxClientImpl client = prepareMxClientWithServerShutdown(cache, server, "TestAppendTupleNotNull");
            Assert.assertNotNull(client);

            Method method = MxClientImpl.class.getDeclaredMethod("prepareMetadata", JobMetadataWrapper.class);
            Assert.assertNotNull(method);
            method.setAccessible(true);
            int columnLength = 5;
            method.invoke(client, JobMetadataWrapper.wrapJobMetadata(Utils.prepareJobMetadata(schema, table, "|", columnLength, "c")));

            CountDownLatch latch = new CountDownLatch(1);
            final boolean[] expectedFailure = {false};
            client.registerDataPostListener(new DataPostListener() {
                @Override
                public void onSuccess(Result result) {
                    expectedFailure[0] = false;
                    latch.countDown();
                }

                @Override
                public void onFailure(Result result) {
                    expectedFailure[0] = true;
                    latch.countDown();
                }
            });

            // Append an empty tuple which is not ready.
            Tuple tuple = client.generateEmptyTuple();
            client.appendTuple(tuple);

            // We will receive Result from onFailure of the DataPostListener.
            Assert.assertTrue(latch.await(2, TimeUnit.SECONDS));
            Assert.assertTrue(expectedFailure[0]);

            // Check tmp Tuples list.
            Field tmpTuplesList = MxClientImpl.class.getDeclaredField("tmpTuplesList");
            Assert.assertNotNull(tmpTuplesList);
            tmpTuplesList.setAccessible(true);
            List<Tuple> tupleList = (List<Tuple>) tmpTuplesList.get(client);
            Assert.assertNotNull(tupleList);
            Assert.assertEquals(0, tupleList.size());

            // Add a ready tuple.
            Tuple readyTuple = client.generateEmptyTuple();
            readyTuple.addColumn("c2", 12345);
            client.appendTuple(readyTuple);
            Assert.assertEquals(1, tupleList.size());

        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | InterruptedException |
                 NoSuchFieldException e) {
            l.error("TestAppendTupleNotNull", e);
            throw new RuntimeException(e);
        }
    }

    @Test
    public void TestAppendTuples() {
        try {
            // Prepare MxClient.
            Cache cache = CacheFactory.getCacheInstance();
            MxServer server = MxServerFactory.getMxServerInstance(prepareServerConfig());
            MxClientImpl client = prepareMxClientWithServerShutdown(cache, server, "TestAppendTuples");
            Assert.assertNotNull(client);

            Method method = MxClientImpl.class.getDeclaredMethod("prepareMetadata", JobMetadataWrapper.class);
            Assert.assertNotNull(method);
            method.setAccessible(true);
            int columnLength = 5;
            method.invoke(client, JobMetadataWrapper.wrapJobMetadata(Utils.prepareJobMetadata(schema, table, "|", columnLength, "c")));

            CountDownLatch latch = new CountDownLatch(1);
            final boolean[] expectedFailure = {false};
            client.registerDataPostListener(new DataPostListener() {
                @Override
                public void onSuccess(Result result) {
                    expectedFailure[0] = false;
                    latch.countDown();
                }

                @Override
                public void onFailure(Result result) {
                    expectedFailure[0] = true;
                    latch.countDown();
                }
            });

            // Append an empty tuple which is not ready.
            Tuple tuple1 = client.generateEmptyTuple();
            Tuple tuple2 = client.generateEmptyTuple();
            // Add a ready tuple.
            Tuple readyTuple1 = client.generateEmptyTuple();
            Tuple readyTuple2 = client.generateEmptyTuple();
            readyTuple1.addColumn("c2", 12345);
            readyTuple2.addColumn("c2", 12345);
            client.appendTuples(tuple1, tuple2, readyTuple1, readyTuple2);

            // We will receive Result from onFailure of the DataPostListener.
            Assert.assertTrue(latch.await(2, TimeUnit.SECONDS));
            Assert.assertTrue(expectedFailure[0]);

            // Check tmp Tuples list.
            Field tmpTuplesList = MxClientImpl.class.getDeclaredField("tmpTuplesList");
            Assert.assertNotNull(tmpTuplesList);
            tmpTuplesList.setAccessible(true);
            List<Tuple> tupleList = (List<Tuple>) tmpTuplesList.get(client);
            Assert.assertNotNull(tupleList);
            Assert.assertEquals(2, tupleList.size());

        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | InterruptedException |
                 NoSuchFieldException e) {
            l.error("TestAppendTupleNotNull", e);
            throw new RuntimeException(e);
        }
    }

    @Test
    public void TestAppendTuplesList() {
        try {
            // Prepare MxClient.
            Cache cache = CacheFactory.getCacheInstance();
            MxServer server = MxServerFactory.getMxServerInstance(prepareServerConfig());
            MxClientImpl client = prepareMxClientWithServerShutdown(cache, server, "TestAppendTuplesList");
            Assert.assertNotNull(client);

            Method method = MxClientImpl.class.getDeclaredMethod("prepareMetadata", JobMetadataWrapper.class);
            Assert.assertNotNull(method);
            method.setAccessible(true);
            int columnLength = 5;
            method.invoke(client, JobMetadataWrapper.wrapJobMetadata(Utils.prepareJobMetadata(schema, table, "|", columnLength, "c")));

            CountDownLatch latch = new CountDownLatch(1);
            final boolean[] expectedFailure = {false};
            client.registerDataPostListener(new DataPostListener() {
                @Override
                public void onSuccess(Result result) {
                    expectedFailure[0] = false;
                    latch.countDown();
                }

                @Override
                public void onFailure(Result result) {
                    expectedFailure[0] = true;
                    latch.countDown();
                }
            });

            // Append an empty tuple which is not ready.
            Tuple tuple1 = client.generateEmptyTuple();
            Tuple tuple2 = client.generateEmptyTuple();
            // Add a ready tuple.
            Tuple readyTuple1 = client.generateEmptyTuple();
            Tuple readyTuple2 = client.generateEmptyTuple();
            readyTuple1.addColumn("c2", 12345);
            readyTuple2.addColumn("c2", 12345);
            List<Tuple> list = new ArrayList<>();
            list.add(tuple1);
            list.add(tuple2);
            list.add(readyTuple1);
            list.add(readyTuple2);
            client.appendTuplesList(list);

            // We will receive Result from onFailure of the DataPostListener.
            Assert.assertTrue(latch.await(2, TimeUnit.SECONDS));
            Assert.assertTrue(expectedFailure[0]);

            // Check tmp Tuples list.
            Field tmpTuplesList = MxClientImpl.class.getDeclaredField("tmpTuplesList");
            Assert.assertNotNull(tmpTuplesList);
            tmpTuplesList.setAccessible(true);
            List<Tuple> tupleList = (List<Tuple>) tmpTuplesList.get(client);
            Assert.assertNotNull(tupleList);
            Assert.assertEquals(2, tupleList.size());

        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | InterruptedException |
                 NoSuchFieldException e) {
            l.error("TestAppendTupleNotNull", e);
            throw new RuntimeException(e);
        }
    }

    @Test
    public void TestAdd() {
        try {
            // Prepare MxClient.
            Cache cache = CacheFactory.getCacheInstance();
            MxServer server = MxServerFactory.getMxServerInstance(prepareServerConfig());
            MxClientImpl client = prepareMxClientWithServerShutdown(cache, server, "TestAdd");
            Assert.assertNotNull(client);

            Method method = MxClientImpl.class.getDeclaredMethod("prepareMetadata", JobMetadataWrapper.class);
            Assert.assertNotNull(method);
            method.setAccessible(true);
            int columnLength = 5;
            method.invoke(client, JobMetadataWrapper.wrapJobMetadata(Utils.prepareJobMetadata(schema, table, "|", columnLength, "c")));

            Method add = MxClientImpl.class.getDeclaredMethod("add", Tuple.class);
            Assert.assertNotNull(add);
            add.setAccessible(true);

            // Try to add a null Tuple into MxClient.
            Tuple tuple = null;
            add.invoke(client, tuple);
            // Check the tmp Tuple list, null Tuple could not be added.
            Field tmpTuplesList = MxClientImpl.class.getDeclaredField("tmpTuplesList");
            Assert.assertNotNull(tmpTuplesList);
            tmpTuplesList.setAccessible(true);
            List<Tuple> tupleList = (List<Tuple>) tmpTuplesList.get(client);
            Assert.assertNotNull(tupleList);
            Assert.assertEquals(0, tupleList.size());

            // Try to add an empty Tuple, the tmp Tuple list is still 0
            add.invoke(client, prepareEmptyTuple(1));
            Assert.assertEquals(0, tupleList.size());

            // The ready tuple will be added successfully.
            Tuple tupleReady = client.generateEmptyTuple();
            tupleReady.addColumn("c2", 12345);
            add.invoke(client, tupleReady);
            Assert.assertEquals(1, tupleList.size());

            Assert.assertNotNull(tupleReady.toCSVLineStr());
            Field accumulatedBytes = MxClientImpl.class.getDeclaredField("accumulatedTuplesBytes");
            Assert.assertNotNull(accumulatedBytes);
            accumulatedBytes.setAccessible(true);
            Assert.assertEquals((long) accumulatedBytes.get(client), tupleReady.toCSVLineStr().getBytes().length);
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | NoSuchFieldException e) {
            l.error("TestAppendTupleNotNull", e);
            throw new RuntimeException(e);
        }
    }

    @Test
    public void TestFlush() {
        Cache cache = CacheFactory.getCacheInstance();
        MxServer server = MxServerFactory.getMxServerInstance(prepareServerConfig());
        MxClientImpl client = prepareMxClientWithServerShutdown(cache, server, "TestFlush");
        Assert.assertNotNull(client);

        try {
            Method method = MxClientImpl.class.getDeclaredMethod("prepareMetadata", JobMetadataWrapper.class);
            Assert.assertNotNull(method);
            method.setAccessible(true);
            int columnLength = 5;
            method.invoke(client, JobMetadataWrapper.wrapJobMetadata(Utils.prepareJobMetadata(schema, table, "|", columnLength, "c")));
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            l.error("TestFlush prepareMetadata exception ", e);
            throw new RuntimeException(e);
        }

        // Give the flush interval a long time to wait.
        client.withIntervalToFlushMillis(40000);
        // Give the flush bytes a big enough size to wait.
        client.withEnoughBytesToFlush(50000000);

        Tuple tuple1 = client.generateEmptyTuple();
        Tuple tuple2 = client.generateEmptyTuple();
        tuple1.addColumn("c2", 12345);
        tuple2.addColumn("c2", 12345);
        client.appendTuples(tuple1, tuple2);
        client.flush();

        CountDownLatch latch = new CountDownLatch(1);
        final boolean[] getSuccess = {false};
        new Thread() {
            @Override
            public void run() {
                try {
                    Tuples tuples = cache.get();
                    Assert.assertNotNull(tuples);
                    if (tuples.size() == 2) {
                        getSuccess[0] = true;
                    }
                    latch.countDown();
                } catch (InterruptedException e) {
                    l.error("TestFlush consume tuples from cache exception ", e);
                    throw new RuntimeException(e);
                }
            }
        }.start();

        try {
            Assert.assertTrue(latch.await(1, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            l.error("TestFlush ");
            throw new RuntimeException(e);
        }

        Assert.assertTrue(getSuccess[0]);
    }

    @Test
    public void TestAddBlocking() {
        try {
            // Prepare MxClient.
            Cache cache = CacheFactory.getCacheInstance();
            MxServer server = MxServerFactory.getMxServerInstance(prepareServerConfig());
            MxClientImpl client = prepareMxClientWithServerShutdown(cache, server, "TestAddBlocking");
            Assert.assertNotNull(client);

            Method method = MxClientImpl.class.getDeclaredMethod("prepareMetadata", JobMetadataWrapper.class);
            Assert.assertNotNull(method);
            method.setAccessible(true);
            int columnLength = 5;
            method.invoke(client, JobMetadataWrapper.wrapJobMetadata(Utils.prepareJobMetadata(schema, table, "|", columnLength, "c")));

            Method add = MxClientImpl.class.getDeclaredMethod("addBlocking", Tuple.class);
            Assert.assertNotNull(add);
            add.setAccessible(true);

            // Try to add a null Tuple into MxClient.
            Tuple tuple = null;
            add.invoke(client, tuple);
            // Check the tmp Tuple list, null Tuple could not be added.
            Field tmpTuplesList = MxClientImpl.class.getDeclaredField("tmpTuplesListBlocking");
            Assert.assertNotNull(tmpTuplesList);
            tmpTuplesList.setAccessible(true);
            List<Tuple> tupleList = (List<Tuple>) tmpTuplesList.get(client);
            Assert.assertNotNull(tupleList);
            Assert.assertEquals(0, tupleList.size());

            // Try to add an empty Tuple, the tmp Tuple list is still 0
            add.invoke(client, prepareEmptyTuple(1));
            Assert.assertEquals(0, tupleList.size());

            // The ready tuple will be added successfully.
            Tuple tupleReady = client.generateEmptyTuple();
            tupleReady.addColumn("c2", 1);
            add.invoke(client, tupleReady);
            Assert.assertEquals(1, tupleList.size());

            Assert.assertNotNull(tupleReady.toCSVLineStr());
            Field accumulatedBytes = MxClientImpl.class.getDeclaredField("accumulatedTuplesBytes");
            Assert.assertNotNull(accumulatedBytes);
            accumulatedBytes.setAccessible(true);
            Assert.assertEquals((long) accumulatedBytes.get(client), tupleReady.toCSVLineStr().getBytes().length);
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | NoSuchFieldException e) {
            l.error("TestAppendTupleNotNull", e);
            throw new RuntimeException(e);
        }
    }

    @Test
    public void TestAppendTupleBlocking() {
        try {
            // Prepare MxClient.
            Cache cache = CacheFactory.getCacheInstance();
            MxServer server = MxServerFactory.getMxServerInstance(prepareServerConfig());
            MxClientImpl client = prepareMxClientWithServerShutdown(cache, server, "TestAppendTupleBlocking");
            Assert.assertNotNull(client);
            client.withEnoughBytesToFlush(30);

            Method method = MxClientImpl.class.getDeclaredMethod("prepareMetadata", JobMetadataWrapper.class);
            Assert.assertNotNull(method);
            method.setAccessible(true);
            int columnLength = 5;
            method.invoke(client, JobMetadataWrapper.wrapJobMetadata(Utils.prepareJobMetadata(schema, table, "|", columnLength, "c")));

            // Append an empty tuple which is not ready.
            boolean tupleNotReady = false;
            Tuple tuple = client.generateEmptyTuple();
            try {
                client.appendTupleBlocking(tuple);
            } catch (TupleNotReadyException e) {
                tupleNotReady = true;
            }
            Assert.assertTrue(tupleNotReady);

            // Check tmp Tuples list.
            Field tmpTuplesList = MxClientImpl.class.getDeclaredField("tmpTuplesListBlocking");
            Assert.assertNotNull(tmpTuplesList);
            tmpTuplesList.setAccessible(true);
            List<Tuple> tupleList = (List<Tuple>) tmpTuplesList.get(client);
            Assert.assertNotNull(tupleList);
            Assert.assertEquals(0, tupleList.size());

            // Add a ready tuple.
            Tuple readyTuple1 = client.generateEmptyTuple();
            readyTuple1.addColumn("c2", 1);
            Assert.assertFalse(client.appendTupleBlocking(readyTuple1));
            Assert.assertEquals(1, tupleList.size());

            // Add a ready tuple again.
            Tuple readyTuple2 = client.generateEmptyTuple();
            readyTuple2.addColumn("c2", 2);
            Assert.assertTrue(client.appendTupleBlocking(readyTuple2));
            Assert.assertEquals(2, tupleList.size());
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException |
                 NoSuchFieldException e) {
            l.error("TestAppendTupleNotNull", e);
            throw new RuntimeException(e);
        }
    }

    @Test(expected = TupleNotReadyException.class)
    public void TestAppendTuplesBlockingTupleNotReady() {
        try {
            // Prepare MxClient.
            Cache cache = CacheFactory.getCacheInstance();
            MxServer server = MxServerFactory.getMxServerInstance(prepareServerConfig());
            MxClientImpl client = prepareMxClientWithServerShutdown(cache, server, "TestAppendTuplesBlocking");
            Assert.assertNotNull(client);
            client.withEnoughBytesToFlush(60);

            Method method = MxClientImpl.class.getDeclaredMethod("prepareMetadata", JobMetadataWrapper.class);
            Assert.assertNotNull(method);
            method.setAccessible(true);
            int columnLength = 5;
            method.invoke(client, JobMetadataWrapper.wrapJobMetadata(Utils.prepareJobMetadata(schema, table, "|", columnLength, "c")));

            // Append an empty tuple which is not ready.
            Tuple tuple1 = client.generateEmptyTuple();
            Tuple tuple2 = client.generateEmptyTuple();
            // Add a ready tuple.
            Tuple readyTuple1 = client.generateEmptyTuple();
            Tuple readyTuple2 = client.generateEmptyTuple();
            readyTuple1.addColumn("c2", 1);
            readyTuple2.addColumn("c2", 2);
            client.appendTuplesBlocking(tuple1, tuple2, readyTuple1, readyTuple2);
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            l.error("TestAppendTupleNotNull", e);
            throw new RuntimeException(e);
        }
    }

    @Test
    public void TestAppendTuplesBlocking() {
        try {
            // Prepare MxClient.
            Cache cache = CacheFactory.getCacheInstance();
            MxServer server = MxServerFactory.getMxServerInstance(prepareServerConfig());
            MxClientImpl client = prepareMxClientWithServerShutdown(cache, server, "TestAppendTuplesBlocking");
            Assert.assertNotNull(client);
            client.withEnoughBytesToFlush(60);

            Method method = MxClientImpl.class.getDeclaredMethod("prepareMetadata", JobMetadataWrapper.class);
            Assert.assertNotNull(method);
            method.setAccessible(true);
            int columnLength = 5;
            method.invoke(client, JobMetadataWrapper.wrapJobMetadata(Utils.prepareJobMetadata(schema, table, "|", columnLength, "c")));

            // Check tmp Tuples list.
            Field tmpTuplesList = MxClientImpl.class.getDeclaredField("tmpTuplesListBlocking");
            Assert.assertNotNull(tmpTuplesList);
            tmpTuplesList.setAccessible(true);
            List<Tuple> tupleList = (List<Tuple>) tmpTuplesList.get(client);
            Assert.assertNotNull(tupleList);
            Assert.assertEquals(0, tupleList.size());

            Tuple readyTuple3 = client.generateEmptyTuple();
            Tuple readyTuple4 = client.generateEmptyTuple();
            readyTuple3.addColumn("c2", 3);
            readyTuple4.addColumn("c2", 4);
            Tuple readyTuple5 = client.generateEmptyTuple();
            Tuple readyTuple6 = client.generateEmptyTuple();
            readyTuple5.addColumn("c2", 5);
            readyTuple6.addColumn("c2", 6);

            Assert.assertFalse(client.appendTuplesBlocking(readyTuple3, readyTuple4));
            Assert.assertTrue(client.appendTuplesBlocking(readyTuple5, readyTuple6));
            Assert.assertEquals(4, tupleList.size());
            // No response body.
            MockHttpServer.startServerBlocking(SERVER_PORT, new MockHttpServer.RootHandler(204, ""));

            client.flushBlocking();
            Assert.assertEquals(0, tupleList.size());
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException |
                 NoSuchFieldException e) {
            l.error("TestAppendTupleNotNull", e);
            throw new RuntimeException(e);
        }
    }

    @Test
    public void TestRegisterMessageQueue() {
        try {
            Cache cache = CacheFactory.getCacheInstance();
            MxServer server = MxServerFactory.getMxServerInstance(prepareServerConfig());
            MxClientImpl client = prepareMxClientWithServerShutdown(cache, server, "TestRegisterMessageQueue");
            Assert.assertNotNull(client);

            Method method = MxClientImpl.class.getDeclaredMethod("prepareMetadata", JobMetadataWrapper.class);
            Assert.assertNotNull(method);
            method.setAccessible(true);
            int columnLength = 5;
            method.invoke(client, JobMetadataWrapper.wrapJobMetadata(Utils.prepareJobMetadata(schema, table, "|", columnLength, "c")));

            Method getClientName = MxClientImpl.class.getDeclaredMethod("getClientName");
            Assert.assertNotNull(getClientName);
            getClientName.setAccessible(true);
            String clientName = (String) getClientName.invoke(client);
            Assert.assertNotNull(clientName);

            Method registerMessageQueue = MxClientImpl.class.getDeclaredMethod("registerMessageQueue");
            Assert.assertNotNull(registerMessageQueue);
            registerMessageQueue.setAccessible(true);
            registerMessageQueue.invoke(client);

            ResultMessageQueue<Result> queue = ResultMessageCenter.getSingleInstance().fetch(clientName);
            Assert.assertNotNull(queue);

            Method unRegisterMessageQueue = MxClientImpl.class.getDeclaredMethod("unRegisterMessageQueue");
            Assert.assertNotNull(unRegisterMessageQueue);
            unRegisterMessageQueue.setAccessible(true);
            unRegisterMessageQueue.invoke(client);

            ResultMessageQueue<Result> queueNull = ResultMessageCenter.getSingleInstance().fetch(clientName);
            Assert.assertNull(queueNull);

        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            l.error("TestRegisterMessageQueue exception ", e);
            throw new RuntimeException(e);
        }
    }

    @Test(expected = ClientClosedException.class)
    public void TestWithEnoughBytesToFlushClose() {
        Cache cache = CacheFactory.getCacheInstance();
        MxServer server = MxServerFactory.getMxServerInstance(prepareServerConfig());
        MxClientImpl client = prepareMxClientWithServerShutdown(cache, server, "TestWithEnoughBytesToFlushClose");
        Assert.assertNotNull(client);

        try {
            Method method = MxClientImpl.class.getDeclaredMethod("prepareMetadata", JobMetadataWrapper.class);
            Assert.assertNotNull(method);
            method.setAccessible(true);
            int columnLength = 5;
            method.invoke(client, JobMetadataWrapper.wrapJobMetadata(Utils.prepareJobMetadata(schema, table, "|", columnLength, "c")));
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            l.error("TestWithEnoughBytesToFlushClose exception ", e);
            throw new RuntimeException(e);
        }

        try {
            client.close();
        } catch (InterruptedException e) {
            l.error("TestWithEnoughBytesToFlushClose close ", e);
            throw new RuntimeException(e);
        }

        // Before invoke this method, the client has been closed, so, this
        // withEnoughBytesToFlush will throw an exception.
        client.withEnoughBytesToFlush(200);
    }

    @Test
    public void TestWithEnoughBytesToFlush() {
        Cache cache = CacheFactory.getCacheInstance();
        MxServer server = MxServerFactory.getMxServerInstance(prepareServerConfig());
        MxClientImpl client = prepareMxClientWithServerShutdown(cache, server, "TestWithEnoughBytesToFlush");
        Assert.assertNotNull(client);

        try {
            Method method = MxClientImpl.class.getDeclaredMethod("prepareMetadata", JobMetadataWrapper.class);
            Assert.assertNotNull(method);
            method.setAccessible(true);
            int columnLength = 5;
            method.invoke(client, JobMetadataWrapper.wrapJobMetadata(Utils.prepareJobMetadata(schema, table, "|", columnLength, "c")));
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            l.error("TestWithEnoughBytesToFlush exception ", e);
            throw new RuntimeException(e);
        }

        // Give the flush interval a long time to wait.
        client.withIntervalToFlushMillis(40000);
        long expectedBytes = 355;
        client.withEnoughBytesToFlush(expectedBytes);
        // Check the bytes size.
        try {
            Field bytesToFlushLimitation = MxClientImpl.class.getDeclaredField("bytesToFlushLimitation");
            bytesToFlushLimitation.setAccessible(true);
            Assert.assertNotNull(bytesToFlushLimitation);
            Assert.assertEquals(expectedBytes, bytesToFlushLimitation.get(client));
        } catch (NoSuchFieldException | IllegalAccessException e) {
            l.error("TestWithEnoughBytesToFlush get fields exception ", e);
            throw new RuntimeException(e);
        }


        CountDownLatch latch = new CountDownLatch(1);
        final int[] expectedTupleSize = {0};
        // Consume the tuples from the cache.
        new Thread() {
            @Override
            public void run() {
                try {
                    Tuples tuples = cache.get();
                    Assert.assertNotNull(tuples);
                    expectedTupleSize[0] = tuples.size();
                    latch.countDown();
                } catch (InterruptedException e) {
                    l.error("TestWithEnoughBytesToFlush cache.get exception ", e);
                    throw new RuntimeException(e);
                }
            }
        }.start();

        int tupleSize = 20; // 20 * tuples(/19 bytes) == 380 bytes. (c2 == 1)
        // The expectedByes is 378, for the reason that to
        // increase the performance of Tuple.toCSVLineStr,
        // the Tuples bytes calculation is not very accurate.

        // Append Tuples into MxClient to flush by bytes size;
        for (int i = 0; i < tupleSize; i++) {
            Tuple tuple = client.generateEmptyTuple();
            tuple.addColumn("c2", 1);
            client.appendTuples(tuple);
        }

        try {
            Assert.assertTrue(latch.await(5, TimeUnit.SECONDS));
            l.info("expected tuples size {}", expectedTupleSize[0]);
            Assert.assertEquals(expectedTupleSize[0], tupleSize);
        } catch (InterruptedException e) {
            l.error("TestWithEnoughBytesToFlush await exception ", e);
            throw new RuntimeException(e);
        }
    }

    @Test
    public void TestWithEnoughLinesToFlush() {
        Cache cache = CacheFactory.getCacheInstance();
        MxServer server = MxServerFactory.getMxServerInstance(prepareServerConfig());
        MxClientImpl client = prepareMxClientWithServerShutdown(cache, server, "TestWithEnoughLinesToFlush");
        Assert.assertNotNull(client);

        try {
            Method method = MxClientImpl.class.getDeclaredMethod("prepareMetadata", JobMetadataWrapper.class);
            Assert.assertNotNull(method);
            method.setAccessible(true);
            int columnLength = 5;
            method.invoke(client, JobMetadataWrapper.wrapJobMetadata(Utils.prepareJobMetadata(schema, table, "|", columnLength, "c")));
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            l.error("TestWithEnoughLinesToFlush exception ", e);
            throw new RuntimeException(e);
        }

        // Give the flush interval a long time to wait.
        client.withIntervalToFlushMillis(40000);

        int expectedLines = 100;
        client.withEnoughLinesToFlush(expectedLines);

        // Check the enoughLinesToFlush
        try {
            Field linesToFlushLimitation = MxClientImpl.class.getDeclaredField("enoughLinesToFlush");
            linesToFlushLimitation.setAccessible(true);
            Assert.assertNotNull(linesToFlushLimitation);
            Assert.assertEquals(expectedLines, linesToFlushLimitation.get(client));
        } catch (NoSuchFieldException | IllegalAccessException e) {
            l.error("TestWithEnoughLinesToFlush get fields exception ", e);
            throw new RuntimeException(e);
        }

        CountDownLatch latch = new CountDownLatch(1);
        final int[] expectedTupleSize = {0};
        // Consume the tuples from the cache.
        new Thread() {
            @Override
            public void run() {
                try {
                    Tuples tuples = cache.get();
                    Assert.assertNotNull(tuples);
                    expectedTupleSize[0] = tuples.size();
                    latch.countDown();
                } catch (InterruptedException e) {
                    l.error("TestWithEnoughLinesToFlush cache.get exception ", e);
                    throw new RuntimeException(e);
                }
            }
        }.start();

        // Append Tuples into MxClient to flush by bytes size;
        for (int i = 0; i < expectedLines; i++) {
            Tuple tuple = client.generateEmptyTuple();
            tuple.addColumn("c2", 1);
            client.appendTuples(tuple);
        }

        try {
            Assert.assertTrue(latch.await(5, TimeUnit.SECONDS));
            l.info("expected tuples size {}", expectedTupleSize[0]);
            Assert.assertEquals(expectedTupleSize[0], expectedLines);
        } catch (InterruptedException e) {
            l.error("TestWithEnoughLinesToFlush await exception", e);
            throw new RuntimeException(e);
        }
    }

    @Test
    public void TestWithEnoughLinesToFlushBlocking() {
        Cache cache = CacheFactory.getCacheInstance();
        MxServer server = MxServerFactory.getMxServerInstance(prepareServerConfig());
        MxClientImpl client = prepareMxClientWithServerShutdown(cache, server, "TestWithEnoughLinesToFlushBlocking");
        Assert.assertNotNull(client);

        try {
            Method method = MxClientImpl.class.getDeclaredMethod("prepareMetadata", JobMetadataWrapper.class);
            Assert.assertNotNull(method);
            method.setAccessible(true);
            int columnLength = 5;
            method.invoke(client, JobMetadataWrapper.wrapJobMetadata(Utils.prepareJobMetadata(schema, table, "|", columnLength, "c")));
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            l.error("TestWithEnoughLinesToFlushBlocking exception ", e);
            throw new RuntimeException(e);
        }

        int expectedLines = 100;
        client.withEnoughLinesToFlush(expectedLines);

        // Check the enoughLinesToFlush
        try {
            Field linesToFlushLimitation = MxClientImpl.class.getDeclaredField("enoughLinesToFlush");
            linesToFlushLimitation.setAccessible(true);
            Assert.assertNotNull(linesToFlushLimitation);
            Assert.assertEquals(expectedLines, linesToFlushLimitation.get(client));
        } catch (NoSuchFieldException | IllegalAccessException e) {
            l.error("TestWithEnoughLinesToFlush get fields exception ", e);
            throw new RuntimeException(e);
        }

        // Append Tuples into MxClient to flush by bytes size;
        for (int i = 0; i < expectedLines; i++) {
            Tuple tuple = client.generateEmptyTuple();
            tuple.addColumn("c2", 1);

            if (i < expectedLines - 1) {
                Assert.assertFalse(client.appendTupleBlocking(tuple));
            }

            if (i == expectedLines - 1) {
                // Enough lines and can be flushed.
                Assert.assertTrue(client.appendTupleBlocking(tuple));
            }
        }
    }

    @Test
    public void TestWithEnoughLinesToFlushAppendTuplesListBlocking() {
        Cache cache = CacheFactory.getCacheInstance();
        MxServer server = MxServerFactory.getMxServerInstance(prepareServerConfig());
        MxClientImpl client = prepareMxClientWithServerShutdown(cache, server, "TestWithEnoughLinesToFlushBlocking");
        Assert.assertNotNull(client);

        try {
            Method method = MxClientImpl.class.getDeclaredMethod("prepareMetadata", JobMetadataWrapper.class);
            Assert.assertNotNull(method);
            method.setAccessible(true);
            int columnLength = 5;
            method.invoke(client, JobMetadataWrapper.wrapJobMetadata(Utils.prepareJobMetadata(schema, table, "|", columnLength, "c")));
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            l.error("TestWithEnoughLinesToFlushBlocking exception ", e);
            throw new RuntimeException(e);
        }

        int expectedLines = 100;
        client.withEnoughLinesToFlush(expectedLines);

        // Check the enoughLinesToFlush
        try {
            Field linesToFlushLimitation = MxClientImpl.class.getDeclaredField("enoughLinesToFlush");
            linesToFlushLimitation.setAccessible(true);
            Assert.assertNotNull(linesToFlushLimitation);
            Assert.assertEquals(expectedLines, linesToFlushLimitation.get(client));
        } catch (NoSuchFieldException | IllegalAccessException e) {
            l.error("TestWithEnoughLinesToFlush get fields exception ", e);
            throw new RuntimeException(e);
        }

        // Append Tuples into MxClient to flush by bytes size;
        List<Tuple> list = new ArrayList<>();
        for (int i = 0; i < expectedLines; i++) {
            Tuple tuple = client.generateEmptyTuple();
            tuple.addColumn("c2", 1);
            list.add(tuple);
        }

        Assert.assertTrue(client.appendTuplesListBlocking(list));
    }

    @Test
    public void TestWithEnoughLinesToFlushAppendTuplesBlocking() {
        Cache cache = CacheFactory.getCacheInstance();
        MxServer server = MxServerFactory.getMxServerInstance(prepareServerConfig());
        MxClientImpl client = prepareMxClientWithServerShutdown(cache, server, "TestWithEnoughLinesToFlushBlocking");
        Assert.assertNotNull(client);

        try {
            Method method = MxClientImpl.class.getDeclaredMethod("prepareMetadata", JobMetadataWrapper.class);
            Assert.assertNotNull(method);
            method.setAccessible(true);
            int columnLength = 5;
            method.invoke(client, JobMetadataWrapper.wrapJobMetadata(Utils.prepareJobMetadata(schema, table, "|", columnLength, "c")));
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            l.error("TestWithEnoughLinesToFlushBlocking exception ", e);
            throw new RuntimeException(e);
        }

        int expectedLines = 5;
        client.withEnoughLinesToFlush(expectedLines);

        // Check the enoughLinesToFlush
        try {
            Field linesToFlushLimitation = MxClientImpl.class.getDeclaredField("enoughLinesToFlush");
            linesToFlushLimitation.setAccessible(true);
            Assert.assertNotNull(linesToFlushLimitation);
            Assert.assertEquals(expectedLines, linesToFlushLimitation.get(client));
        } catch (NoSuchFieldException | IllegalAccessException e) {
            l.error("TestWithEnoughLinesToFlush get fields exception ", e);
            throw new RuntimeException(e);
        }

        Tuple tuple1 = client.generateEmptyTuple();
        tuple1.addColumn("c2", 1);

        Tuple tuple2 = client.generateEmptyTuple();
        tuple2.addColumn("c2", 1);

        Tuple tuple3 = client.generateEmptyTuple();
        tuple3.addColumn("c2", 1);

        Assert.assertFalse(client.appendTuplesBlocking(tuple1, tuple2, tuple3));

        Tuple tuple4 = client.generateEmptyTuple();
        tuple4.addColumn("c2", 1);

        Tuple tuple5 = client.generateEmptyTuple();
        tuple5.addColumn("c2", 1);

        Assert.assertTrue(client.appendTuplesBlocking(tuple4, tuple5));
    }

    @Test
    public void TestWithIntervalToFlushMillis() {
        Cache cache = CacheFactory.getCacheInstance();
        MxServer server = MxServerFactory.getMxServerInstance(prepareServerConfig());
        MxClientImpl client = prepareMxClientWithServerShutdown(cache, server, "TestWithIntervalToFlushMillis");
        Assert.assertNotNull(client);

        try {
            Method method = MxClientImpl.class.getDeclaredMethod("prepareMetadata", JobMetadataWrapper.class);
            Assert.assertNotNull(method);
            method.setAccessible(true);
            int columnLength = 5;
            method.invoke(client, JobMetadataWrapper.wrapJobMetadata(Utils.prepareJobMetadata(schema, table, "|", columnLength, "c")));
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            l.error("TestWithIntervalToFlushMillis exception ", e);
            throw new RuntimeException(e);
        }

        // Make the bytes big enough not to flush by bytes.
        client.withEnoughBytesToFlush(5000000);

        client.withIntervalToFlushMillis(2000);

        // Check the flush interval.
        try {
            Field flushIntervalMillis = MxClientImpl.class.getDeclaredField("flushIntervalMillis");
            flushIntervalMillis.setAccessible(true);
            Assert.assertNotNull(flushIntervalMillis);
            long expectedMillis = 2000;
            Assert.assertEquals(flushIntervalMillis.get(client), expectedMillis);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            l.error("TestWithIntervalToFlushMillis get fields exception ", e);
            throw new RuntimeException(e);
        }

        int tupleSize = 20; // 20 * tuples(/25 bytes) == 500 bytes. (c2 == 1)
        CountDownLatch latch = new CountDownLatch(1);

        final int[] expectedTupleSize = {0};
        // Consume the tuples from the cache.
        new Thread() {
            @Override
            public void run() {
                try {
                    Tuples tuples = cache.get();
                    Assert.assertNotNull(tuples);
                    expectedTupleSize[0] = tuples.size();
                    latch.countDown();
                } catch (InterruptedException e) {
                    l.error("TestWithIntervalToFlushMillis cache.get exception ", e);
                    throw new RuntimeException(e);
                }
            }
        }.start();

        // Append Tuples into MxClient to flush by bytes size;
        for (int i = 0; i < tupleSize; i++) {
            Tuple tuple = client.generateEmptyTuple();
            tuple.addColumn("c2", 1);
            client.appendTuples(tuple);
        }

        try {
            Assert.assertTrue(latch.await(5, TimeUnit.SECONDS));
            l.info("expected tuples size {}", expectedTupleSize[0]);
            Assert.assertEquals(expectedTupleSize[0], tupleSize);
        } catch (InterruptedException e) {
            l.error("TestWithIntervalToFlushMillis await exception ", e);
            throw new RuntimeException(e);
        }
    }

    @Test
    public void TestClose() {
        Cache cache = CacheFactory.getCacheInstance();
        MxServer server = MxServerFactory.getMxServerInstance(prepareServerConfig());
        MxClientImpl client = prepareMxClientWithServerShutdown(cache, server, "TestClose");
        Assert.assertNotNull(client);

        try {
            Method method = MxClientImpl.class.getDeclaredMethod("prepareMetadata", JobMetadataWrapper.class);
            Assert.assertNotNull(method);
            method.setAccessible(true);
            int columnLength = 5;
            method.invoke(client, JobMetadataWrapper.wrapJobMetadata(Utils.prepareJobMetadata(schema, table, "|", columnLength, "c")));

            // The MessageQueue will be registered after the DataPostListener has been registered.
            client.registerDataPostListener(new DataPostListener() {
                @Override
                public void onSuccess(Result result) {

                }

                @Override
                public void onFailure(Result result) {

                }
            });

            Method clientName = MxClientImpl.class.getDeclaredMethod("getClientName");
            Assert.assertNotNull(clientName);
            clientName.setAccessible(true);
            String clientNameStr = (String) clientName.invoke(client);
            Assert.assertNotNull(ResultMessageCenter.getSingleInstance().fetch(clientNameStr));

            // Append tuples.
            Tuple tuple1 = client.generateEmptyTuple();
            Tuple tuple2 = client.generateEmptyTuple();
            tuple1.addColumn("c2", 1);
            tuple2.addColumn("c2", 1);
            client.appendTuples(tuple1, tuple2);

            client.close();

            // Check the close state
            Field close = MxClientImpl.class.getDeclaredField("close");
            Assert.assertNotNull(close);
            close.setAccessible(true);
            AtomicBoolean closeBool = (AtomicBoolean) close.get(client);
            Assert.assertNotNull(closeBool);
            Assert.assertTrue(closeBool.get());

            // Consume the tuples, make sure the last tuples int the tmpTuplesList can be flushed into the cache.
            CountDownLatch latch = new CountDownLatch(1);
            final int[] expectedTuplesSize = {0};
            new Thread() {
                @Override
                public void run() {
                    try {
                        Tuples tuples = cache.get();
                        Assert.assertNotNull(tuples);
                        expectedTuplesSize[0] = tuples.size();
                        latch.countDown();
                    } catch (InterruptedException e) {
                        l.error("TestClose cache.get() exception ", e);
                        throw new RuntimeException(e);
                    }
                }
            }.start();
            Assert.assertTrue(latch.await(2, TimeUnit.SECONDS));
            Assert.assertEquals(expectedTuplesSize[0], 2);

            // Make sure the MxClient's MessageQueue has been unregistered.
            Assert.assertNull(ResultMessageCenter.getSingleInstance().fetch(clientNameStr));

        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | InterruptedException |
                 NoSuchFieldException e) {
            l.error("TestWithIntervalToFlushMillis exception ", e);
            throw new RuntimeException(e);
        }
    }

    @Test(expected = IllegalStateException.class)
    public void TestAppendTupleCircuitBreak() {
        try {
            String table = "TestAppendTupleCircuitBreak";
            // Prepare MxClient.
            Cache cache = CacheFactory.getCacheInstance();
            CircuitBreakerConfig cbc = new CircuitBreakerConfig()
                    .setEnable()
                    .setMinimumNumberOfCalls(1)
                    .setSlidingWindowSize(10)
                    .setFailureRateThreshold(60.0f);
            ServerConfig sc = prepareServerConfig();
            sc.setCircuitBreakerConfig(cbc);
            MxServer server = MxServerFactory.getMxServerInstance(sc);
            MxClientImpl client = prepareMxClientWithServerShutdown(cache, server, table);
            Assert.assertNotNull(client);

            Method method = MxClientImpl.class.getDeclaredMethod("prepareMetadata", JobMetadataWrapper.class);
            Assert.assertNotNull(method);
            method.setAccessible(true);
            int columnLength = 5;
            method.invoke(client, JobMetadataWrapper.wrapJobMetadata(Utils.prepareJobMetadata(schema, table, "|", columnLength, "c")));

            CircuitBreaker breaker = CircuitBreakerFactory.getInstance().prepareCircuitBreaker(SERVER_URL_HTTP, schema, table, cbc);

            // Check tmp Tuples list.
            Field tmpTuplesList = MxClientImpl.class.getDeclaredField("tmpTuplesList");
            Assert.assertNotNull(tmpTuplesList);
            tmpTuplesList.setAccessible(true);
            List<Tuple> tupleList = (List<Tuple>) tmpTuplesList.get(client);
            Assert.assertNotNull(tupleList);
            Assert.assertEquals(0, tupleList.size());

            // Add a ready tuple.
            Tuple t1 = client.generateEmptyTuple();
            t1.addColumn("c2", 1);
            client.appendTuple(t1);
            Assert.assertEquals(1, tupleList.size());

            breaker.onSuccess(1, TimeUnit.MILLISECONDS);
            breaker.onSuccess(1, TimeUnit.MILLISECONDS);
            breaker.onSuccess(1, TimeUnit.MILLISECONDS);

            // Add a tuple.
            Tuple t2 = client.generateEmptyTuple();
            t2.addColumn("c2", 2);
            client.appendTuple(t2);
            Assert.assertEquals(2, tupleList.size());

            breaker.onError(1, TimeUnit.MILLISECONDS, new RuntimeException("test"));
            breaker.onError(1, TimeUnit.MILLISECONDS, new RuntimeException("test"));
            breaker.onError(1, TimeUnit.MILLISECONDS, new RuntimeException("test"));
            breaker.onError(1, TimeUnit.MILLISECONDS, new RuntimeException("test"));
            breaker.onError(1, TimeUnit.MILLISECONDS, new RuntimeException("test"));

            // Add a tuple.
            Tuple t3 = client.generateEmptyTuple();
            t3.addColumn("c2", 3);
            client.appendTuple(t3);
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | NoSuchFieldException e) {
            l.error("TestAppendTupleNotNull", e);
            throw new RuntimeException(e);
        }
    }

    @Test
    public void TestAppendTupleCircuitBreakOnAndClose() {
        try {
            String table = "TestAppendTupleCircuitBreak2";
            // Prepare MxClient.
            Cache cache = CacheFactory.getCacheInstance();
            CircuitBreakerConfig cbc = new CircuitBreakerConfig()
                    .setEnable()
                    .setMinimumNumberOfCalls(1)
                    .setSlidingWindowSize(10)
                    .setFailureRateThreshold(60.0f);
            ServerConfig sc = prepareServerConfig();
            sc.setCircuitBreakerConfig(cbc);
            MxServer server = MxServerFactory.getMxServerInstance(sc);
            MxClientImpl client = prepareMxClientWithServerShutdown(cache, server, table);
            Assert.assertNotNull(client);

            Method method = MxClientImpl.class.getDeclaredMethod("prepareMetadata", JobMetadataWrapper.class);
            Assert.assertNotNull(method);
            method.setAccessible(true);
            int columnLength = 5;
            method.invoke(client, JobMetadataWrapper.wrapJobMetadata(Utils.prepareJobMetadata(schema, table, "|", columnLength, "c")));

            CircuitBreaker breaker = CircuitBreakerFactory.getInstance().prepareCircuitBreaker(SERVER_URL_HTTP, schema, table, cbc);

            // Check tmp Tuples list.
            Field tmpTuplesList = MxClientImpl.class.getDeclaredField("tmpTuplesList");
            Assert.assertNotNull(tmpTuplesList);
            tmpTuplesList.setAccessible(true);
            List<Tuple> tupleList = (List<Tuple>) tmpTuplesList.get(client);
            Assert.assertNotNull(tupleList);
            Assert.assertEquals(0, tupleList.size());

            // Add a ready tuple.
            Tuple t1 = client.generateEmptyTuple();
            t1.addColumn("c2", 1);
            client.appendTuple(t1);
            Assert.assertEquals(1, tupleList.size());

            breaker.onSuccess(1, TimeUnit.MILLISECONDS);
            breaker.onSuccess(1, TimeUnit.MILLISECONDS);
            breaker.onSuccess(1, TimeUnit.MILLISECONDS);

            // Add a tuple.
            Tuple t2 = client.generateEmptyTuple();
            t2.addColumn("c2", 2);
            client.appendTuple(t2);
            Assert.assertEquals(2, tupleList.size());

            breaker.onError(1, TimeUnit.MILLISECONDS, new RuntimeException("test"));
            breaker.onError(1, TimeUnit.MILLISECONDS, new RuntimeException("test"));
            breaker.onError(1, TimeUnit.MILLISECONDS, new RuntimeException("test"));
            breaker.onError(1, TimeUnit.MILLISECONDS, new RuntimeException("test"));
            breaker.onError(1, TimeUnit.MILLISECONDS, new RuntimeException("test"));

            // Add a tuple.
            Tuple t3 = client.generateEmptyTuple();
            t3.addColumn("c2", 3);
            try {
                client.appendTuple(t3);
                System.out.println("not catch expected exception");
                Assert.assertTrue(false);
            } catch (Exception e) {
                System.out.printf("catch expected exception: %s\n", e.getMessage());
            }

            try {
                Thread.sleep(40000);
            } catch (InterruptedException e) {
                // do nothing
            }
            Assert.assertEquals(CircuitBreaker.State.HALF_OPEN, breaker.getState());

            client.appendTuple(t3);
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | NoSuchFieldException e) {
            l.error("TestAppendTupleNotNull", e);
            throw new RuntimeException(e);
        }
    }

    @Test
    public void TestWithCompress() {
        try {
            String table = "TestWithCompress";
            // Prepare MxClient.
            Cache cache = CacheFactory.getCacheInstance();
            ServerConfig sc = prepareServerConfig();
            MxServer server = MxServerFactory.getMxServerInstance(sc);
            MxClientImpl client = prepareMxClientWithServerShutdown(cache, server, table);
            Assert.assertNotNull(client);
            client.withCompress();
            client.withEnoughLinesToFlush(1);

            Method method = MxClientImpl.class.getDeclaredMethod("prepareMetadata", JobMetadataWrapper.class);
            Assert.assertNotNull(method);
            method.setAccessible(true);
            int columnLength = 5;
            method.invoke(client, JobMetadataWrapper.wrapJobMetadata(Utils.prepareJobMetadata(schema, table, "|", columnLength, "c")));

            // Add a ready tuple.
            Tuple t1 = client.generateEmptyTuple();
            t1.addColumn("c2", "c2_value");
            client.appendTuple(t1);
            client.flush();

            CountDownLatch latch = new CountDownLatch(1);
            Thread thread = new Thread() {
                @Override
                public void run() {
                    try {
                        Tuples tuples = cache.get();
                        Assert.assertNotNull(tuples);
                        Assert.assertTrue(tuples.needCompress());
                        latch.countDown();
                    } catch (InterruptedException e) {
                        l.error("Cache consume exception ", e);
                        throw new RuntimeException(e);
                    }
                }
            };
            thread.start();
            Assert.assertTrue(latch.await(3, TimeUnit.SECONDS));

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void TestWithBase64CompressBytesEncode() {
        try {
            String table = "TestWithBase64CompressBytesEncode";
            // Prepare MxClient.
            Cache cache = CacheFactory.getCacheInstance();
            ServerConfig sc = prepareServerConfig();
            MxServer server = MxServerFactory.getMxServerInstance(sc);
            MxClientImpl client = prepareMxClientWithServerShutdown(cache, server, table);
            Assert.assertNotNull(client);
            client.withEnoughLinesToFlush(1);
            client.withBase64Encode4Compress();

            Method method = MxClientImpl.class.getDeclaredMethod("prepareMetadata", JobMetadataWrapper.class);
            Assert.assertNotNull(method);
            method.setAccessible(true);
            int columnLength = 5;
            method.invoke(client, JobMetadataWrapper.wrapJobMetadata(Utils.prepareJobMetadata(schema, table, "|", columnLength, "c")));

            // Add a ready tuple.
            Tuple t1 = client.generateEmptyTuple();
            t1.addColumn("c2", "c2_value");
            client.appendTuple(t1);
            client.flush();

            CountDownLatch latch = new CountDownLatch(1);
            Thread thread = new Thread() {
                @Override
                public void run() {
                    try {
                        Tuples tuples = cache.get();
                        Assert.assertNotNull(tuples);
                        Assert.assertTrue(tuples.needBase64Encoding4CompressedBytes());
                        latch.countDown();
                    } catch (InterruptedException e) {
                        l.error("Cache consume exception ", e);
                        throw new RuntimeException(e);
                    }
                }
            };
            thread.start();
            Assert.assertTrue(latch.await(3, TimeUnit.SECONDS));

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void TestWithoutBase64CompressBytesEncode() {
        try {
            String table = "TestWithoutBase64CompressBytesEncode";
            // Prepare MxClient.
            Cache cache = CacheFactory.getCacheInstance();
            ServerConfig sc = prepareServerConfig();
            MxServer server = MxServerFactory.getMxServerInstance(sc);
            MxClientImpl client = prepareMxClientWithServerShutdown(cache, server, table);
            Assert.assertNotNull(client);
            client.withEnoughLinesToFlush(1);
            client.withoutBase64EncodeCompress();

            Method method = MxClientImpl.class.getDeclaredMethod("prepareMetadata", JobMetadataWrapper.class);
            Assert.assertNotNull(method);
            method.setAccessible(true);
            int columnLength = 5;
            method.invoke(client, JobMetadataWrapper.wrapJobMetadata(Utils.prepareJobMetadata(schema, table, "|", columnLength, "c")));

            // Add a ready tuple.
            Tuple t1 = client.generateEmptyTuple();
            t1.addColumn("c2", "c2_value");
            client.appendTuple(t1);
            client.flush();

            CountDownLatch latch = new CountDownLatch(1);
            Thread thread = new Thread() {
                @Override
                public void run() {
                    try {
                        Tuples tuples = cache.get();
                        Assert.assertNotNull(tuples);
                        Assert.assertFalse(tuples.needBase64Encoding4CompressedBytes());
                        latch.countDown();
                    } catch (InterruptedException e) {
                        l.error("Cache consume exception ", e);
                        throw new RuntimeException(e);
                    }
                }
            };
            thread.start();
            Assert.assertTrue(latch.await(3, TimeUnit.SECONDS));

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void TestWithCompressBlocking() {
        try {
            String table = "TestWithCompressBlocking";
            // Prepare MxClient.
            Cache cache = CacheFactory.getCacheInstance();
            ServerConfig sc = prepareServerConfig();
            MxServer server = MxServerFactory.getMxServerInstance(sc);
            MxClientImpl client = prepareMxClientWithServerShutdown(cache, server, table);
            Assert.assertNotNull(client);
            client.withEnoughLinesToFlush(1);

            Method method = MxClientImpl.class.getDeclaredMethod("prepareMetadata", JobMetadataWrapper.class);
            Assert.assertNotNull(method);
            method.setAccessible(true);
            int columnLength = 5;
            method.invoke(client, JobMetadataWrapper.wrapJobMetadata(Utils.prepareJobMetadata(schema, table, "|", columnLength, "c")));

            Method generateTuples = MxClientImpl.class.getDeclaredMethod("generateTuples");
            Assert.assertNotNull(generateTuples);
            generateTuples.setAccessible(true);
            Tuples tuples = (Tuples) generateTuples.invoke(client);
            Assert.assertNotNull(tuples);
            Assert.assertFalse(tuples.needCompress());

            client.withCompress();
            Tuples tuples1 = (Tuples) generateTuples.invoke(client);
            Assert.assertNotNull(tuples1);
            Assert.assertTrue(tuples1.needCompress());

            client.withoutCompress();
            Tuples tuples2 = (Tuples) generateTuples.invoke(client);
            Assert.assertNotNull(tuples2);
            Assert.assertFalse(tuples2.needCompress());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void TestWithBase64CompressBytesEncodeBlocking() {
        try {
            String table = "TestWithBase64CompressBytesEncodeBlocking";
            // Prepare MxClient.
            Cache cache = CacheFactory.getCacheInstance();
            ServerConfig sc = prepareServerConfig();
            MxServer server = MxServerFactory.getMxServerInstance(sc);
            MxClientImpl client = prepareMxClientWithServerShutdown(cache, server, table);
            Assert.assertNotNull(client);
            client.withEnoughLinesToFlush(1);

            Method method = MxClientImpl.class.getDeclaredMethod("prepareMetadata", JobMetadataWrapper.class);
            Assert.assertNotNull(method);
            method.setAccessible(true);
            int columnLength = 5;
            method.invoke(client, JobMetadataWrapper.wrapJobMetadata(Utils.prepareJobMetadata(schema, table, "|", columnLength, "c")));

            Method generateTuples = MxClientImpl.class.getDeclaredMethod("generateTuples");
            Assert.assertNotNull(generateTuples);
            generateTuples.setAccessible(true);
            Tuples tuples = (Tuples) generateTuples.invoke(client);
            Assert.assertNotNull(tuples);
            Assert.assertFalse(tuples.needBase64Encoding4CompressedBytes());

            client.withBase64Encode4Compress();
            Tuples tuples1 = (Tuples) generateTuples.invoke(client);
            Assert.assertNotNull(tuples1);
            Assert.assertTrue(tuples1.needBase64Encoding4CompressedBytes());

            client.withoutBase64EncodeCompress();
            Tuples tuples2 = (Tuples) generateTuples.invoke(client);
            Assert.assertNotNull(tuples2);
            Assert.assertFalse(tuples2.needBase64Encoding4CompressedBytes());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    @Test
    public void TestWithCompressBlockingTuplesPool() {
        try {
            String table = "TestWithCompressBlockingTuplesPool";
            // Prepare MxClient.
            Cache cache = CacheFactory.getCacheInstance();
            ServerConfig sc = prepareServerConfig();
            MxServer server = MxServerFactory.getMxServerInstance(sc);
            MxClientImpl client = prepareMxClientWithServerShutdown(cache, server, table);
            Assert.assertNotNull(client);
            client.withEnoughLinesToFlush(1);

            Method method = MxClientImpl.class.getDeclaredMethod("prepareMetadata", JobMetadataWrapper.class);
            Assert.assertNotNull(method);
            method.setAccessible(true);
            int columnLength = 5;
            method.invoke(client, JobMetadataWrapper.wrapJobMetadata(Utils.prepareJobMetadata(schema, table, "|", columnLength, "c")));

            client.useTuplesPool(10);
            Method getTuples = MxClientImpl.class.getDeclaredMethod("getTuples");
            Assert.assertNotNull(getTuples);
            getTuples.setAccessible(true);
            Tuples tuples = (Tuples) getTuples.invoke(client);
            Assert.assertNotNull(tuples);
            Assert.assertFalse(tuples.needCompress());

            client.withCompress();
            Tuples tuples1 = (Tuples) getTuples.invoke(client);
            Assert.assertNotNull(tuples1);
            Assert.assertTrue(tuples1.needCompress());

            client.withoutCompress();
            Tuples tuples2 = (Tuples) getTuples.invoke(client);
            Assert.assertNotNull(tuples2);
            Assert.assertFalse(tuples2.needCompress());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void TestWithOutCompress() {
        try {
            String table = "TestWithOutCompress";
            // Prepare MxClient.
            Cache cache = CacheFactory.getCacheInstance();
            ServerConfig sc = prepareServerConfig();
            MxServer server = MxServerFactory.getMxServerInstance(sc);
            MxClientImpl client = prepareMxClientWithServerShutdown(cache, server, table);
            Assert.assertNotNull(client);
            client.withoutCompress();
            client.withEnoughLinesToFlush(1);

            Method method = MxClientImpl.class.getDeclaredMethod("prepareMetadata", JobMetadataWrapper.class);
            Assert.assertNotNull(method);
            method.setAccessible(true);
            int columnLength = 5;
            method.invoke(client, JobMetadataWrapper.wrapJobMetadata(Utils.prepareJobMetadata(schema, table, "|", columnLength, "c")));

            // Add a ready tuple.
            Tuple t1 = client.generateEmptyTuple();
            t1.addColumn("c2", "c2_value");
            client.appendTuple(t1);
            client.flush();

            CountDownLatch latch = new CountDownLatch(1);
            Thread thread = new Thread() {
                @Override
                public void run() {
                    try {
                        Tuples tuples = cache.get();
                        Assert.assertNotNull(tuples);
                        Assert.assertFalse(tuples.needCompress());
                        latch.countDown();
                    } catch (InterruptedException e) {
                        l.error("Cache consume exception ", e);
                        throw new RuntimeException(e);
                    }
                }
            };
            thread.start();
            Assert.assertTrue(latch.await(3, TimeUnit.SECONDS));

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    @Test(expected = IllegalStateException.class)
    public void TestAppendTupleBlockingCircuitBreak() {
        try {
            String table = "AppendTupleBlockingCircuitBreak";
            // Prepare MxClient.
            Cache cache = CacheFactory.getCacheInstance();
            CircuitBreakerConfig cbc = new CircuitBreakerConfig()
                    .setEnable()
                    .setMinimumNumberOfCalls(1)
                    .setSlidingWindowSize(10)
                    .setFailureRateThreshold(60.0f);
            ServerConfig sc = prepareServerConfig();
            sc.setCircuitBreakerConfig(cbc);
            MxServer server = MxServerFactory.getMxServerInstance(sc);
            MxClientImpl client = prepareMxClientWithServerShutdown(cache, server, table);
            Assert.assertNotNull(client);

            Method method = MxClientImpl.class.getDeclaredMethod("prepareMetadata", JobMetadataWrapper.class);
            Assert.assertNotNull(method);
            method.setAccessible(true);
            int columnLength = 5;
            method.invoke(client, JobMetadataWrapper.wrapJobMetadata(Utils.prepareJobMetadata(schema, table, "|", columnLength, "c")));

            CircuitBreaker breaker = CircuitBreakerFactory.getInstance().prepareCircuitBreaker(SERVER_URL_HTTP, schema, table, cbc);

            // Check tmp Tuples list.
            Field tmpTuplesList = MxClientImpl.class.getDeclaredField("tmpTuplesListBlocking");
            Assert.assertNotNull(tmpTuplesList);
            tmpTuplesList.setAccessible(true);
            List<Tuple> tupleList = (List<Tuple>) tmpTuplesList.get(client);
            Assert.assertNotNull(tupleList);
            Assert.assertEquals(0, tupleList.size());

            // Add a ready tuple.
            Tuple t1 = client.generateEmptyTuple();
            t1.addColumn("c2", 1);
            client.appendTupleBlocking(t1);
            Assert.assertEquals(1, tupleList.size());

            breaker.onSuccess(1, TimeUnit.MILLISECONDS);
            breaker.onSuccess(1, TimeUnit.MILLISECONDS);
            breaker.onSuccess(1, TimeUnit.MILLISECONDS);

            // Add a tuple.
            Tuple t2 = client.generateEmptyTuple();
            t2.addColumn("c2", 2);
            client.appendTupleBlocking(t2);
            Assert.assertEquals(2, tupleList.size());

            breaker.onError(1, TimeUnit.MILLISECONDS, new RuntimeException("test"));
            breaker.onError(1, TimeUnit.MILLISECONDS, new RuntimeException("test"));
            breaker.onError(1, TimeUnit.MILLISECONDS, new RuntimeException("test"));
            breaker.onError(1, TimeUnit.MILLISECONDS, new RuntimeException("test"));
            breaker.onError(1, TimeUnit.MILLISECONDS, new RuntimeException("test"));

            // Add a tuple.
            Tuple t3 = client.generateEmptyTuple();
            t3.addColumn("c2", 3);
            client.appendTupleBlocking(t3);
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | NoSuchFieldException e) {
            l.error("TestAppendTupleNotNull", e);
            throw new RuntimeException(e);
        }
    }

    @Test(expected = IllegalStateException.class)
    public void TestFlushBlockingCircuitBreak() {
        try {
            String table = "FlushBlockingCircuitBreak";
            // Prepare MxClient.
            Cache cache = CacheFactory.getCacheInstance();
            CircuitBreakerConfig cbc = new CircuitBreakerConfig()
                    .setEnable()
                    .setMinimumNumberOfCalls(1)
                    .setSlidingWindowSize(10)
                    .setFailureRateThreshold(60.0f);
            ServerConfig sc = prepareServerConfig();
            sc.setCircuitBreakerConfig(cbc);
            MxServer server = MxServerFactory.getMxServerInstance(sc);
            MxClientImpl client = prepareMxClientWithServerShutdown(cache, server, table);
            Assert.assertNotNull(client);
            client.withEnoughBytesToFlush(30);

            Method method = MxClientImpl.class.getDeclaredMethod("prepareMetadata", JobMetadataWrapper.class);
            Assert.assertNotNull(method);
            method.setAccessible(true);
            int columnLength = 5;
            method.invoke(client, JobMetadataWrapper.wrapJobMetadata(Utils.prepareJobMetadata(schema, table, "|", columnLength, "c")));

            CircuitBreaker breaker = CircuitBreakerFactory.getInstance().prepareCircuitBreaker(SERVER_URL_HTTP, schema, table, cbc);

            // Check tmp Tuples list.
            Field tmpTuplesList = MxClientImpl.class.getDeclaredField("tmpTuplesListBlocking");
            Assert.assertNotNull(tmpTuplesList);
            tmpTuplesList.setAccessible(true);
            List<Tuple> tupleList = (List<Tuple>) tmpTuplesList.get(client);
            Assert.assertNotNull(tupleList);
            Assert.assertEquals(0, tupleList.size());

            // Add a ready tuple.
            Tuple t1 = client.generateEmptyTuple();
            t1.addColumn("c2", 1);
            client.appendTupleBlocking(t1);
            Assert.assertEquals(1, tupleList.size());

            breaker.onSuccess(1, TimeUnit.MILLISECONDS);
            breaker.onSuccess(1, TimeUnit.MILLISECONDS);
            breaker.onSuccess(1, TimeUnit.MILLISECONDS);
            breaker.onError(1, TimeUnit.MILLISECONDS, new RuntimeException("test"));

            // Add a tuple.
            Tuple t2 = client.generateEmptyTuple();
            t2.addColumn("c2", 2);
            client.appendTupleBlocking(t2);
            Assert.assertEquals(2, tupleList.size());

            breaker.onError(1, TimeUnit.MILLISECONDS, new RuntimeException("test"));
            breaker.onError(1, TimeUnit.MILLISECONDS, new RuntimeException("test"));
            breaker.onError(1, TimeUnit.MILLISECONDS, new RuntimeException("test"));
            breaker.onError(1, TimeUnit.MILLISECONDS, new RuntimeException("test"));

            client.flushBlocking();
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | NoSuchFieldException e) {
            l.error("TestAppendTupleNotNull", e);
            throw new RuntimeException(e);
        }
    }

    @Test
    public void TestMxClientCSVConstructionSize() {
        try {
            String table = "CSVConstructionSize";

            // Prepare MxClient.
            Cache cache = CacheFactory.getCacheInstance();
            ServerConfig sc = prepareServerConfig();
            MxServer server = MxServerFactory.getMxServerInstance(sc);
            MxClientImpl client = prepareMxClientWithServerShutdown(cache, server, table);
            Assert.assertNotNull(client);

            Field csvSize = client.getClass().getDeclaredField("csvConstructionBatchSize");
            Assert.assertNotNull(csvSize);
            csvSize.setAccessible(true);
            // By default, the csv construction batch size == 2000;
            Assert.assertEquals(csvSize.get(client), 2000);

            int newBatchSize = 3000;
            client.withCSVConstructionBatchSize(newBatchSize);
            Assert.assertEquals(csvSize.get(client), newBatchSize);

            Method generateTuplesMethod = client.getClass().getDeclaredMethod("generateTuples");
            Assert.assertNotNull(generateTuplesMethod);
            generateTuplesMethod.setAccessible(true);

            Tuples tuples = (Tuples) generateTuplesMethod.invoke(client);
            Assert.assertNotNull(tuples);
            Assert.assertEquals(tuples.getCSVBatchSize(), newBatchSize);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test(expected = InvalidParameterException.class)
    public void TestMxClientCSVConstructionSizeWithException() {
        String table = "CSVConstructionSize";

        // Prepare MxClient.
        Cache cache = CacheFactory.getCacheInstance();
        ServerConfig sc = prepareServerConfig();
        MxServer server = MxServerFactory.getMxServerInstance(sc);
        MxClientImpl client = prepareMxClientWithServerShutdown(cache, server, table);
        Assert.assertNotNull(client);

        // Will throw exception, batch size must be positive.
        client.withCSVConstructionBatchSize(0);
    }

    @Test(expected = InvalidParameterException.class)
    public void TestMxClientCSVConstructionSizeWithException2() {
        String table = "CSVConstructionSize";

        // Prepare MxClient.
        Cache cache = CacheFactory.getCacheInstance();
        ServerConfig sc = prepareServerConfig();
        MxServer server = MxServerFactory.getMxServerInstance(sc);
        MxClientImpl client = prepareMxClientWithServerShutdown(cache, server, table);
        Assert.assertNotNull(client);

        // Will throw exception, batch size must be positive.
        client.withCSVConstructionBatchSize(-10);
    }


    public static MxClientImpl prepareMxClientWithServerShutdown(Cache cache, MxServer server, String tableName) {
        return prepareMxClientNormal(cache, server, tableName);
    }

    static MxClientImpl prepareMxClientNormal(Cache cache, MxServer server, String tableName) {
        return new MxClientImpl(1, prepareClientConfig(tableName), prepareHTTPConfig(), cache, server);
    }

    public static ServerConfig prepareServerConfig() {
        ServerConfig sc = new ServerConfig();
        sc.setRequestType(RequestType.WithHTTP);
        sc.setConcurrency(10);
        sc.setTimeoutMillis(500);
        sc.setMaxRetryAttempts(3);
        sc.setWaitRetryDurationMillis(300);
        sc.setCSVConstructionParallel(10);
        return sc;
    }

    static HTTPConfig prepareHTTPConfig() {
        HTTPConfig hc = new HTTPConfig();
        hc.setWaitRetryDurationLimitation(300);
        hc.setMaxRetryAttempts(3);
        hc.setServerURLGRPC(SERVER_URL_GRPC);
        hc.setServerURLDataSending(SERVER_URL_HTTP);
        hc.setRequestTimeoutMillis(500);
        return hc;
    }

    static ClientConfig prepareClientConfig(String table) {
        return new ClientConfig(schema, table);
    }

    private Tuple prepareEmptyTuple(final int size) {
        return new Tuple() {
            @Override
            public void addColumn(String key, Object value) {

            }

            @Override
            public String getTableName() {
                return null;
            }

            @Override
            public Column[] getColumns() {
                return new Column[0];
            }

            @Override
            public byte[] toBytes() {
                return new byte[0];
            }

            @Override
            public void setDelimiter(String delimiter) {

            }

            @Override
            public String getDelimiter() {
                return null;
            }

            @Override
            public int size() {
                return size;
            }

            @Override
            public void readinessCheck() {

            }

            @Override
            public String toCSVLineStr() {
                return null;
            }

            @Override
            public String getRawInputString() {
                return "";
            }

            @Override
            public void setSerialNum(long sn) {

            }

            @Override
            public long getSerialNum() {
                return 0;
            }

            @Override
            public void reset() {

            }
        };
    }

    private Tuples getTuples() {
        return new Tuples() {

            private TuplesTarget target;

            private static final String delimiter = "|";
            private static final String schema = "public";
            private static final String table = "table";

            private static final int size = 4;

            private static final int csvBatchSize = 10;

            private Tuple generateTuple(String delimiter, String schema, String table) {
                return cn.ymatrix.builder.Utils.generateEmptyTupleLite(delimiter, schema, table);
            }

            private List<Tuple> generateTuplesList(int size, String delimiter, String schema, String table) {
                List<Tuple> tuplesList = new ArrayList<>(size);
                for (int i = 0; i < size; i++) {
                    Tuple tuple = generateTuple(delimiter, schema, table);
                    tuple.addColumn("c1", i);
                    tuple.addColumn("c2", i);
                    tuple.addColumn("c3", i);
                    tuple.addColumn("c4", i);
                    tuple.addColumn("c5", i);
                    tuplesList.add(tuple);
                }
                return tuplesList;
            }

            @Override
            public void append(Tuple tuple) {

            }

            @Override
            public void appendTuples(Tuple... tuples) {

            }

            @Override
            public void appendTupleList(List<Tuple> tupleList) {

            }

            @Override
            public List<Tuple> getTuplesList() {
                return generateTuplesList(size, delimiter, schema, table);
            }

            @Override
            public int size() {
                return size;
            }

            @Override
            public void setSchema(String schema) {

            }

            @Override
            public void setTable(String table) {

            }

            @Override
            public String getSchema() {
                return schema;
            }

            @Override
            public String getTable() {
                return table;
            }

            @Override
            public void setTarget(TuplesTarget target) {
                this.target = target;
            }

            @Override
            public TuplesTarget getTarget() {
                return this.target;
            }

            @Override
            public Tuple getTupleByIndex(int index) {
                return null;
            }

            @Override
            public void setSenderID(String senderID) {

            }

            @Override
            public String getSenderID() {
                return null;
            }

            @Override
            public boolean isEmpty() {
                return false;
            }

            @Override
            public void reset() {

            }

            @Override
            public void setDelimiter(String delimiter) {

            }

            @Override
            public String getDelimiter() {
                return delimiter;
            }

            @Override
            public boolean needCompress() {
                return false;
            }

            @Override
            public void setCompress(boolean compress) {

            }

            @Override
            public boolean needBase64Encoding4CompressedBytes() {
                return false;
            }

            @Override
            public void setBase64Encoding4CompressedBytes(boolean base64Encoding) {

            }

            @Override
            public int getCSVBatchSize() {
                return csvBatchSize;
            }
        };
    }

}
