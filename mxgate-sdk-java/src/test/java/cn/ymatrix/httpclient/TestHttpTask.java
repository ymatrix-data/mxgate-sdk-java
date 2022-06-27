package cn.ymatrix.httpclient;

import cn.ymatrix.api.StatusCode;
import cn.ymatrix.apiserver.SendDataListener;
import cn.ymatrix.apiserver.SendDataResult;
import cn.ymatrix.builder.CircuitBreakerConfig;
import cn.ymatrix.builder.MxBuilder;
import cn.ymatrix.builder.Utils;
import cn.ymatrix.data.Tuple;
import cn.ymatrix.data.Tuples;
import cn.ymatrix.data.TuplesTarget;
import cn.ymatrix.exception.CircuitBreakException;
import cn.ymatrix.exception.RetryException;
import cn.ymatrix.faulttolerance.CircuitBreakerFactory;
import cn.ymatrix.faulttolerance.RetryConfiguration;
import cn.ymatrix.faulttolerance.RetryStatistic;
import cn.ymatrix.logger.MxLogger;
import cn.ymatrix.utils.CSVConstructor;
import org.checkerframework.checker.units.qual.C;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestHttpTask {
    private static final Logger l = MxLogger.init(TestHttpTask.class);

    private CSVConstructor constructor = new CSVConstructor(10);

    @Test
    public void TestCanRetry() {
        try {
            HttpTask httpTask = new HttpTask(null, SingletonHTTPClient.getInstance(1024).getClient(),
                    constructor) {
                @Override
                public void run() {

                }
            };

            // With no RetryConfiguration, canRetry() will be false.
            Assert.assertFalse(httpTask.canRetry());

            RetryConfiguration rc = new RetryConfiguration(3, 3000, RetryException.class);
            httpTask.withRetry(rc);

            Assert.assertTrue(httpTask.canRetry());

            Field rsField = HttpTask.class.getDeclaredField("rs");
            Assert.assertNotNull(rsField);
            rsField.setAccessible(true);

            RetryStatistic rs = (RetryStatistic) rsField.get(httpTask);
            Assert.assertNotNull(rs);
            Assert.assertEquals(rs.actuallyRetryTimes(), 0);
            Assert.assertFalse(rs.exceedMaxRetryTimes());

            rs.increaseRetryTimes();
            Assert.assertTrue(httpTask.canRetry());
            Assert.assertEquals(rs.actuallyRetryTimes(), 1);
            Assert.assertFalse(rs.exceedMaxRetryTimes());

            rs.increaseRetryTimes();
            Assert.assertTrue(httpTask.canRetry());
            Assert.assertEquals(rs.actuallyRetryTimes(), 2);
            Assert.assertFalse(rs.exceedMaxRetryTimes());

            rs.increaseRetryTimes();
            Assert.assertFalse(httpTask.canRetry());
            Assert.assertEquals(rs.actuallyRetryTimes(), 3);
            Assert.assertTrue(rs.exceedMaxRetryTimes());

        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    @Test(expected = NullPointerException.class)
    public void TestSendDataBlockingNullableTuples() {
        HttpTask httpTask = new HttpTask(null, SingletonHTTPClient.getInstance(1024).getClient(), constructor) {
            @Override
            public void run() {
                // Nothing to do for sync blocking request.
            }
        };
        httpTask.requestBlocking(null);
    }

    @Test(expected = NullPointerException.class)
    public void TestSendDataBlockingNullableTuplesTarget() {
        HttpTask httpTask = new HttpTask(null, SingletonHTTPClient.getInstance(1024).getClient(), constructor) {
            @Override
            public void run() {
                // Nothing to do for sync blocking request.
            }
        };
        Tuples tuples = prepareTuples(5, "");
        tuples.setTarget(null);
        httpTask.requestBlocking(tuples);
    }

    @Test(expected = NullPointerException.class)
    public void TestSendDataBlockingNullableTuplesTargetURL() {
        HttpTask httpTask = new HttpTask(null, SingletonHTTPClient.getInstance(1024).getClient(), constructor) {
            @Override
            public void run() {
                // Nothing to do for sync blocking request.
            }
        };
        Tuples tuples = prepareTuples(5, "");
        TuplesTarget target = new TuplesTarget();
        target.setURL(null);
        tuples.setTarget(target);
        httpTask.requestBlocking(tuples);
    }

    @Test(expected = NullPointerException.class)
    public void TestSendDataBlockingEmptyTuplesTargetURL() {
        HttpTask httpTask = new HttpTask(null, SingletonHTTPClient.getInstance(1024).getClient(), constructor) {
            @Override
            public void run() {
                // Nothing to do for sync blocking request.
            }
        };
        Tuples tuples = prepareTuples(5, "");
        TuplesTarget target = new TuplesTarget();
        target.setURL("");
        tuples.setTarget(target);
        httpTask.requestBlocking(tuples);
    }

    @Test
    public void TestSendDataBlocking204() {
        // No response body.
        MockHttpServer.startServerBlocking(9000, new MockHttpServer.RootHandler(204, ""));
        HttpTask httpTask = new HttpTask(null, SingletonHTTPClient.getInstance(1024).getClient(), constructor) {
            @Override
            public void run() {

            }
        };

        final SendDataResult[] callbackResult = {null};
        httpTask.registerListener(new SendDataListener() {
            @Override
            public void onSuccess(SendDataResult result, Tuples tuples) {
                callbackResult[0] = result;
            }

            @Override
            public void onFailure(SendDataResult result, Tuples tuples) {

            }
        });
        Tuples tuples = prepareTuples(5, "");
        TuplesTarget target = new TuplesTarget();
        target.setURL("http://localhost:9000/");
        tuples.setTarget(target);
        SendDataResult result = httpTask.requestBlocking(tuples);
        Assert.assertNotNull(result);
        Assert.assertEquals(StatusCode.NORMAL, result.getCode());
        Assert.assertEquals("Send 5 tuples succeed.", result.getMsg());
        Assert.assertNull(result.getErrorLinesMap());

        // Also check the callback listener.
        Assert.assertNotNull(callbackResult[0]);
        Assert.assertEquals(StatusCode.NORMAL, callbackResult[0].getCode());
        Assert.assertEquals("Send 5 tuples succeed.", callbackResult[0].getMsg());
        Assert.assertNull(callbackResult[0].getErrorLinesMap());
    }

    @Test
    public void TestDataSendingBlocking200NoResponse() {
        // No response body.
        MockHttpServer.startServerBlocking(9001, new MockHttpServer.RootHandler(200, ""));
        HttpTask httpTask = new HttpTask(null, SingletonHTTPClient.getInstance(1024).getClient(), constructor) {
            @Override
            public void run() {

            }
        };

        final SendDataResult[] callbackResult = {null};
        httpTask.registerListener(new SendDataListener() {
            @Override
            public void onSuccess(SendDataResult result, Tuples tuples) {
                callbackResult[0] = result;
            }

            @Override
            public void onFailure(SendDataResult result, Tuples tuples) {

            }
        });
        Tuples tuples = prepareTuples(5, "");
        TuplesTarget target = new TuplesTarget();
        target.setURL("http://localhost:9001/");
        tuples.setTarget(target);
        SendDataResult result = httpTask.requestBlocking(tuples);
        Assert.assertNotNull(result);
        Assert.assertEquals(StatusCode.NORMAL, result.getCode());
        Assert.assertEquals("Send 5 tuples succeed.", result.getMsg());
        Assert.assertNull(result.getErrorLinesMap());

        // Also check the callback listener.
        Assert.assertNotNull(callbackResult[0]);
        Assert.assertEquals(callbackResult[0].getCode(), StatusCode.NORMAL);
        Assert.assertEquals("Send 5 tuples succeed.", callbackResult[0].getMsg());
        Assert.assertNull(callbackResult[0].getErrorLinesMap());
    }

    @Test
    public void TestDataSendingBlocking200WithResponse() {
        String responseBody = "At line: 3 \n" +
                "Invalid input of type timestamp in column ts \n" +
                "This is the second reason line.\n" +
                "At line: 5 \n" +
                "Invalid input of type timestamp in column ts in line 5";
        ;
        // No response body.
        MockHttpServer.startServerBlocking(9002, new MockHttpServer.RootHandler(200, responseBody));
        HttpTask httpTask = new HttpTask(null, SingletonHTTPClient.getInstance(1024).getClient(), constructor) {
            @Override
            public void run() {

            }
        };

        final boolean[] onFailureCallback = {false};
        httpTask.registerListener(new SendDataListener() {
            @Override
            public void onSuccess(SendDataResult result, Tuples tuples) {

            }

            @Override
            public void onFailure(SendDataResult result, Tuples tuples) {
                resultPartiallyCheck(result, responseBody);
                onFailureCallback[0] = true;
            }
        });
        Tuples tuples = prepareTuples(5, "");
        TuplesTarget target = new TuplesTarget();
        target.setURL("http://localhost:9002/");
        tuples.setTarget(target);
        SendDataResult result = httpTask.requestBlocking(tuples);
        resultPartiallyCheck(result, responseBody);
        Assert.assertTrue(onFailureCallback[0]);
    }

    @Test
    public void TestAllTuplesFailWithResponse500() {
        String responseBody = "All tuples fail.";
        MockHttpServer.startServerBlocking(9003, new MockHttpServer.RootHandler(500, responseBody));
        HttpTask httpTask = new HttpTask(null, SingletonHTTPClient.getInstance(1024).getClient(), constructor) {
            @Override
            public void run() {

            }
        };
        final boolean[] onFailureCallback = {false};
        httpTask.registerListener(new SendDataListener() {
            @Override
            public void onSuccess(SendDataResult result, Tuples tuples) {

            }

            @Override
            public void onFailure(SendDataResult result, Tuples tuples) {
                resultAllCheck(result, responseBody);
                onFailureCallback[0] = true;
            }
        });

        Tuples tuples = prepareTuples(5, "");
        TuplesTarget target = new TuplesTarget();
        target.setURL("http://localhost:9003/");
        tuples.setTarget(target);
        SendDataResult result = httpTask.requestBlocking(tuples);

        resultAllCheck(result, responseBody);
    }

    private void resultPartiallyCheck(SendDataResult result, String responseBody) {
        Assert.assertNotNull(result);
        Assert.assertEquals(StatusCode.PARTIALLY_TUPLES_FAIL, result.getCode());
        Assert.assertEquals(responseBody, result.getMsg());
        Assert.assertNotNull(result.getErrorLinesMap());
        Assert.assertEquals(2, result.getErrorLinesMap().size());
        boolean findErrorLine1 = false;
        boolean findErrorLine2 = false;
        for (Map.Entry<Long, String> entry : result.getErrorLinesMap().entrySet()) {
            if (entry.getKey() == 2) {
                Assert.assertEquals("Invalid input of type timestamp in column ts \n" +
                        "This is the second reason line.\n", entry.getValue());
                findErrorLine1 = true;
            }
            if (entry.getKey() == 4) {
                Assert.assertEquals("Invalid input of type timestamp in column ts in line 5\n", entry.getValue());
                findErrorLine2 = true;
            }
        }
        Assert.assertTrue(findErrorLine1);
        Assert.assertTrue(findErrorLine2);
    }

    private void resultAllCheck(SendDataResult result, String responseBody) {
        Assert.assertNotNull(result);
        Assert.assertEquals(StatusCode.ALL_TUPLES_FAIL, result.getCode());
        Assert.assertEquals(responseBody, result.getMsg());
        Assert.assertNull(result.getErrorLinesMap());
    }

    @Test
    public void TestParsePartiallyErrorResponse() {
        HttpTask httpTask = new HttpTask(null, SingletonHTTPClient.getInstance(1024).getClient(), constructor) {
            @Override
            public void run() {
                // Nothing to do.
            }
        };

        try {
            Method parseResp = HttpTask.class.getDeclaredMethod("parsePartiallyErrorResponse", String.class);
            Assert.assertNotNull(parseResp);
            parseResp.setAccessible(true);
            String validResp = "At line: 1 \n" +
                    "Invalid input of type timestamp in column ts";
            Object obj = parseResp.invoke(httpTask, validResp);
            Assert.assertNotNull(obj);
            Map<Long, String> parsedKeyValue = (HashMap<Long, String>) obj;
            Assert.assertNotNull(parsedKeyValue);
            Assert.assertEquals(1, parsedKeyValue.size());
            for (Map.Entry<Long, String> entry : parsedKeyValue.entrySet()) {
                long errorLineNumber = entry.getKey();
                Assert.assertEquals(0, errorLineNumber); // For http request, the first line in CSV is schema.table.
                Assert.assertEquals("Invalid input of type timestamp in column ts\n", entry.getValue());
            }

            String invalidResp = "Invalid resp";
            Object obj1 = parseResp.invoke(httpTask, invalidResp);
            Assert.assertNotNull(obj);
            Map<Long, String> parsedKeyValue1 = (HashMap<Long, String>) obj1;
            Assert.assertNotNull(parsedKeyValue1);
            Assert.assertEquals(0, parsedKeyValue1.size());

            String invalidResp2 = "At line: 1x \n" +
                    "Invalid input of type timestamp in column ts";
            Object obj2 = parseResp.invoke(httpTask, invalidResp2);
            Assert.assertNotNull(obj2);
            Map<Long, String> parsedKeyValue2 = (HashMap<Long, String>) obj2;
            Assert.assertNotNull(parsedKeyValue2);
            Assert.assertEquals(0, parsedKeyValue2.size());

            String validRespWithMultipleLines = "At line: 3 \n" +
                    "Invalid input of type timestamp in column ts \n" +
                    "This is the second reason line.";
            Object obj3 = parseResp.invoke(httpTask, validRespWithMultipleLines);
            Assert.assertNotNull(obj3);
            Map<Long, String> parsedKeyValue3 = (HashMap<Long, String>) obj3;
            Assert.assertNotNull(parsedKeyValue3);
            Assert.assertEquals(1, parsedKeyValue3.size());
            for (Map.Entry<Long, String> entry : parsedKeyValue3.entrySet()) {
                long errorLineNumber = entry.getKey();
                Assert.assertEquals(2, errorLineNumber);
                Assert.assertEquals("Invalid input of type timestamp in column ts \n" +
                        "This is the second reason line.\n", entry.getValue());
            }

            String validRespWithMultipleLines2 = "At line: 3 \n" +
                    "Invalid input of type timestamp in column ts \n" +
                    "This is the second reason line.\n" +
                    "At line: 5 \n" +
                    "Invalid input of type timestamp in column ts in line 5";
            Object obj4 = parseResp.invoke(httpTask, validRespWithMultipleLines2);
            Map<Long, String> parsedKeyValue4 = (HashMap<Long, String>) obj4;
            Assert.assertNotNull(parsedKeyValue4);
            Assert.assertEquals(2, parsedKeyValue4.size());
            boolean findLine1 = false;
            boolean findLine2 = false;
            for (Map.Entry<Long, String> entry : parsedKeyValue4.entrySet()) {
                long errorLineNumber = entry.getKey();
                l.info("error line number {}", errorLineNumber);
                l.info("value {}", entry.getValue());
                if (errorLineNumber == 2) {
                    Assert.assertEquals("Invalid input of type timestamp in column ts \n" +
                            "This is the second reason line.\n", entry.getValue());
                    findLine1 = true;
                }
                if (errorLineNumber == 4) {
                    Assert.assertEquals("Invalid input of type timestamp in column ts in line 5\n", entry.getValue());
                    findLine2 = true;
                }
            }
            Assert.assertTrue(findLine1);
            Assert.assertTrue(findLine2);

            Object obj5 = parseResp.invoke(httpTask, "");
            Assert.assertNull(obj5);

        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void TestFindMultipleErrorReasonLines() {
        HttpTask httpTask = new HttpTask(null, SingletonHTTPClient.getInstance(1024).getClient(), constructor) {
            @Override
            public void run() {
                // Nothing to do.
            }
        };

        try {
            Method method = HttpTask.class.getDeclaredMethod("findMultipleErrorReasonLines", String[].class, int.class);
            Assert.assertNotNull(method);
            method.setAccessible(true);

            String[] toFind = new String[]{
                    "line 1, this is the line 1.",
                    "line 2, this is the line 2.",
                    HttpTask.lineNumPrefix,
                    "line 3, this is the line 3."
            };
            String findStr = (String) method.invoke(httpTask, toFind, 1);
            Assert.assertNotNull(findStr);
            Assert.assertEquals("line 2, this is the line 2.\n", findStr);

            String findStr2 = (String) method.invoke(httpTask, toFind, 2);
            Assert.assertNotNull(findStr2);
            Assert.assertEquals("", findStr2);

            String findStr3 = (String) method.invoke(httpTask, toFind, 0);
            Assert.assertNotNull(findStr3);
            Assert.assertEquals("line 1, this is the line 1.\n" + "line 2, this is the line 2.\n", findStr3);

            String findStr4 = (String) method.invoke(httpTask, toFind, 3);
            Assert.assertNotNull(findStr4);
            Assert.assertEquals("line 3, this is the line 3.\n", findStr4);


        } catch (Exception e) {
            l.error("TestFindMultipleErrorReasonLines exception ", e);
        }
    }

    @Test
    public void TestSendDataBlockingWithCircuitBreakerOnByFailure() {
        int port = 9004;
        float failureRate = 60.0f;
        String schema = "public";
        String table = "table";

        MockHttpServer.RootHandler mockHandler = new MockHttpServer.RootHandler(204, "")
                .withProcessException("test");
        MockHttpServer.startServerBlocking(port, mockHandler);

        CircuitBreakerConfig cbc = new CircuitBreakerConfig()
                .setEnable()
                .setMinimumNumberOfCalls(1)
                .setSlidingWindowSize(10)
                .setFailureRateThreshold(failureRate);
        cbc.setIgnoredExceptions(RetryException.class);
        HttpTask httpTask = new HttpTask(cbc, SingletonHTTPClient.getInstance(1024).getClient(), constructor) {
            @Override
            public void run() {

            }
        };

        // check if normal process logic is affected
        final SendDataResult[] callbackResult = {null};
        httpTask.registerListener(new SendDataListener() {
            @Override
            public void onSuccess(SendDataResult result, Tuples tuples) {
                callbackResult[0] = result;
            }

            @Override
            public void onFailure(SendDataResult result, Tuples tuples) {

            }
        });

        TuplesTarget target = new TuplesTarget();
        String targetURL = String.format("http://localhost:%d/", port);
        target.setURL(targetURL);

        Tuples t1 = prepareTuples(5, "");
        t1.setTarget(target);
        t1.setSchema(schema);
        t1.setTable(table);
        SendDataResult result = httpTask.requestBlocking(t1);
        Assert.assertNotNull(result);
        Assert.assertEquals(StatusCode.NORMAL, result.getCode());
        Assert.assertEquals("Send 5 tuples succeed.", result.getMsg());
        Assert.assertNull(result.getErrorLinesMap());

        // Also check the callback listener.
        Assert.assertNotNull(callbackResult[0]);
        Assert.assertEquals(StatusCode.NORMAL, callbackResult[0].getCode());
        Assert.assertEquals("Send 5 tuples succeed.", callbackResult[0].getMsg());
        Assert.assertNull(callbackResult[0].getErrorLinesMap());

        // check circuit breaker behaviour
        // trigger breaker open
        Tuples t2 = prepareTuples(5, "exception");
        t2.setTarget(target);
        t2.setSchema("public");
        t2.setTable(table);
        try {
            httpTask.requestBlocking(t2);
        } catch(CircuitBreakException e){
            // do nothing
        } catch (Exception e) {
            Assert.assertTrue(String.format("unexpected exception %s", e), false);
        }

        Assert.assertTrue(CircuitBreakerFactory.getInstance().isBreakerExist(targetURL, schema, table));
        Assert.assertFalse(CircuitBreakerFactory.getInstance().isCircuitBreak(targetURL, schema, table));
        l.info("failure rate {}", CircuitBreakerFactory.getInstance().getFailureRate(targetURL, schema, table));
        Assert.assertEquals(0, Float.compare(CircuitBreakerFactory.getInstance().getFailureRate(targetURL, schema, table), 50.0f));

        try {
            httpTask.requestBlocking(t2);
        } catch(CircuitBreakException e){
            // do nothing
        } catch (Exception e) {
            Assert.assertTrue(String.format("unexpected exception %s", e), false);
        }
        Assert.assertTrue(CircuitBreakerFactory.getInstance().isCircuitBreak(targetURL, schema, table));
        Assert.assertTrue(Float.compare(CircuitBreakerFactory.getInstance().getFailureRate(targetURL, schema, table), failureRate) > 0);
        l.info("failure rate {}", CircuitBreakerFactory.getInstance().getFailureRate(targetURL, schema, table));

        // Wait breaker to close.
        try {
            Thread.sleep(50000);
        } catch (Exception e) {
            // Nothing to do.
        }
        Assert.assertFalse(CircuitBreakerFactory.getInstance().isCircuitBreak(targetURL, schema, table));
        httpTask.requestBlocking(t1);
        Assert.assertFalse(CircuitBreakerFactory.getInstance().isCircuitBreak(targetURL, schema, table));
        Assert.assertTrue(Float.compare(CircuitBreakerFactory.getInstance().getFailureRate(targetURL, schema, table), failureRate) < 0);
    }
    
    @Test
    public void TestSendDataBlockingWithCircuitBreakerNotOnByRetryFailure() {
        int port = 9005;
        float failureRate = 60.0f;
        String schema = "public";
        String table = "table";

        MockHttpServer.RootHandler mockHandler = new MockHttpServer.RootHandler(204, "")
                .withProcessException("test");
        MockHttpServer.startServerBlocking(port, mockHandler);

        CircuitBreakerConfig cbc = new CircuitBreakerConfig()
                .setEnable()
                .setMinimumNumberOfCalls(1)
                .setSlidingWindowSize(10)
                .setFailureRateThreshold(failureRate);
        cbc.setIgnoredExceptions(RetryException.class);
        HttpTask httpTask = new HttpTask(cbc, SingletonHTTPClient.getInstance(1024).getClient(), constructor) {
            @Override
            public void run() {

            }
        };
        int retryCnt = 5;
        httpTask.withRetry(new RetryConfiguration(retryCnt, 50, RetryException.class));

        // check if normal process logic is affected
        final SendDataResult[] callbackResult = {null};
        httpTask.registerListener(new SendDataListener() {
            @Override
            public void onSuccess(SendDataResult result, Tuples tuples) {
                callbackResult[0] = result;
            }

            @Override
            public void onFailure(SendDataResult result, Tuples tuples) {

            }
        });

        TuplesTarget target = new TuplesTarget();
        String targetURL = String.format("http://localhost:%d/", port);
        target.setURL(targetURL);

        Tuples t1 = prepareTuples(5, "");
        t1.setTarget(target);
        t1.setSchema(schema);
        t1.setTable(table);
        SendDataResult result = httpTask.requestBlocking(t1);
        Assert.assertNotNull(result);
        Assert.assertEquals(StatusCode.NORMAL, result.getCode());
        Assert.assertEquals("Send 5 tuples succeed.", result.getMsg());
        Assert.assertNull(result.getErrorLinesMap());

        // Also check the callback listener.
        Assert.assertNotNull(callbackResult[0]);
        Assert.assertEquals(StatusCode.NORMAL, callbackResult[0].getCode());
        Assert.assertEquals("Send 5 tuples succeed.", callbackResult[0].getMsg());
        Assert.assertNull(callbackResult[0].getErrorLinesMap());

        // check circuit breaker behaviour
        // trigger breaker open, and expect not open
        Tuples t2 = prepareTuples(5, "exception");
        t2.setTarget(target);
        t2.setSchema("public");
        t2.setTable(table);
        try {
            httpTask.requestBlocking(t2);
        } catch(RetryException e){
            retryCnt --;
            // do nothing
        } catch (Exception e) {
            Assert.assertTrue(String.format("unexpected exception %s", e), false);
        }
        Assert.assertTrue(CircuitBreakerFactory.getInstance().isBreakerExist(targetURL, schema, table));
        Assert.assertFalse(CircuitBreakerFactory.getInstance().isCircuitBreak(targetURL, schema, table));
        l.info("failure rate {}", CircuitBreakerFactory.getInstance().getFailureRate(targetURL, schema, table));
        Assert.assertEquals(0, Float.compare(CircuitBreakerFactory.getInstance().getFailureRate(targetURL, schema, table), 0.0f));

        try {
            httpTask.requestBlocking(t2);
        } catch(RetryException e){
            retryCnt--;
            // do nothing
        } catch (Exception e) {
            Assert.assertTrue(String.format("unexpected exception %s", e), false);
        }
        Assert.assertFalse(CircuitBreakerFactory.getInstance().isCircuitBreak(targetURL, schema, table));
        Assert.assertEquals(0, Float.compare(CircuitBreakerFactory.getInstance().getFailureRate(targetURL, schema, table), 0.0f));

        for (int i = 0; i < retryCnt; i ++) {
            try {
                httpTask.requestBlocking(t2);
            } catch(RetryException e){
                retryCnt--;
                // do nothing
            } catch (Exception e) {
                Assert.assertTrue(String.format("unexpected exception %s", e), false);
            }
        }
        try {
            httpTask.requestBlocking(t2);
        } catch(CircuitBreakException e){
            // do nothing
        } catch (Exception e) {
            Assert.assertTrue(String.format("unexpected exception %s", e), false);
        }
    }

    @Test
    public void TestSendDataBlockingWithCircuitBreakerNotOnByTupleBroken() {
        int port = 9006;
        float failureRate = 60.0f;
        String schema = "public";
        String table = "table";

        MockHttpServer.RootHandler mockHandler = new MockHttpServer.RootHandler(204, "")
                .withProcessException("test");
        MockHttpServer.startServerBlocking(port, mockHandler);

        CircuitBreakerConfig cbc = new CircuitBreakerConfig()
                .setEnable()
                .setMinimumNumberOfCalls(1)
                .setSlidingWindowSize(10)
                .setFailureRateThreshold(failureRate);
        cbc.setIgnoredExceptions(RetryException.class);
        HttpTask httpTask = new HttpTask(cbc, SingletonHTTPClient.getInstance(1024).getClient(), constructor) {
            @Override
            public void run() {

            }
        };
        int retryCnt = 5;
        httpTask.withRetry(new RetryConfiguration(retryCnt, 50, RetryException.class));

        // check if normal process logic is affected
        final SendDataResult[] callbackResult = {null};
        httpTask.registerListener(new SendDataListener() {
            @Override
            public void onSuccess(SendDataResult result, Tuples tuples) {
                callbackResult[0] = result;
            }

            @Override
            public void onFailure(SendDataResult result, Tuples tuples) {

            }
        });

        TuplesTarget target = new TuplesTarget();
        String targetURL = String.format("http://localhost:%d/", port);
        target.setURL(targetURL);

        Tuples t1 = prepareTuples(5, "");
        t1.setTarget(target);
        t1.setSchema(schema);
        t1.setTable(table);
        SendDataResult result = httpTask.requestBlocking(t1);
        Assert.assertNotNull(result);
        Assert.assertEquals(StatusCode.NORMAL, result.getCode());
        Assert.assertEquals("Send 5 tuples succeed.", result.getMsg());
        Assert.assertNull(result.getErrorLinesMap());

        // Also check the callback listener.
        Assert.assertNotNull(callbackResult[0]);
        Assert.assertEquals(StatusCode.NORMAL, callbackResult[0].getCode());
        Assert.assertEquals("Send 5 tuples succeed.", callbackResult[0].getMsg());
        Assert.assertNull(callbackResult[0].getErrorLinesMap());

        // check circuit breaker behaviour
        // trigger breaker open, and expect not open
        Tuples t2 = prepareTuples(5,"tuple broken");
        t2.setTarget(target);
        t2.setSchema("public");
        t2.setTable(table);
        try {
            httpTask.requestBlocking(t2);
        } catch(RetryException e){
            retryCnt --;
            // do nothing
        } catch (Exception e) {
            Assert.assertTrue(String.format("unexpected exception %s", e), false);
        }
        Assert.assertTrue(CircuitBreakerFactory.getInstance().isBreakerExist(targetURL, schema, table));
        Assert.assertFalse(CircuitBreakerFactory.getInstance().isCircuitBreak(targetURL, schema, table));
        l.info("failure rate {}", CircuitBreakerFactory.getInstance().getFailureRate(targetURL, schema, table));
        Assert.assertEquals(0, Float.compare(CircuitBreakerFactory.getInstance().getFailureRate(targetURL, schema, table), 0.0f));

        try {
            httpTask.requestBlocking(t2);
        } catch(RetryException e){
            retryCnt--;
            // do nothing
        } catch (Exception e) {
            Assert.assertTrue(String.format("unexpected exception %s", e), false);
        }
        Assert.assertFalse(CircuitBreakerFactory.getInstance().isCircuitBreak(targetURL, schema, table));
        Assert.assertEquals(0, Float.compare(CircuitBreakerFactory.getInstance().getFailureRate(targetURL, schema, table), 0.0f));

        for (int i = 0; i < retryCnt; i ++) {
            try {
                httpTask.requestBlocking(t2);
            } catch(RetryException e){
                retryCnt--;
                // do nothing
            } catch (Exception e) {
                Assert.assertTrue(String.format("unexpected exception %s", e), false);
            }
        }
        try {
            httpTask.requestBlocking(t2);
        } catch(CircuitBreakException e){
            // do nothing
        } catch (Exception e) {
            Assert.assertTrue(String.format("unexpected exception %s", e), false);
        }
    }

    @Ignore
    @Test
    public void TestSendDataBlockingWithCircuitBreakerOnBySlowCall() {
        int port = 9007;
        int slowCallDurationLimit = 200;
        float slowCallRate = 60.0f;
        String schema = "public";
        String table = "TestSendDataBlockingWithCircuitBreakerOnBySlowCall";

        MockHttpServer.RootHandler mockHandler = new MockHttpServer.RootHandler(204, "ok")
                .withProcessTime(slowCallDurationLimit + 1);
        MockHttpServer.startServerBlocking(port, mockHandler);

        CircuitBreakerConfig cbc = new CircuitBreakerConfig()
                .setEnable()
                .setMinimumNumberOfCalls(1)
                .setSlidingWindowSize(10)
                .setSlowCallDurationThresholdMillis(slowCallDurationLimit)
                .setSlowCallRateThreshold(slowCallRate);
        HttpTask httpTask = new HttpTask(cbc, SingletonHTTPClient.getInstance(1024).getClient(), constructor) {
            @Override
            public void run() {

            }
        };

        // check if normal process logic is affected
        final SendDataResult[] callbackResult = {null};
        httpTask.registerListener(new SendDataListener() {
            @Override
            public void onSuccess(SendDataResult result, Tuples tuples) {
                callbackResult[0] = result;
            }

            @Override
            public void onFailure(SendDataResult result, Tuples tuples) {

            }
        });

        TuplesTarget target = new TuplesTarget();
        String targetURL = String.format("http://localhost:%d/", port);
        target.setURL(targetURL);
        target.setTimeout(10 * slowCallDurationLimit);

        Tuples t1 = prepareTuples(5, "");
        t1.setTarget(target);
        t1.setSchema(schema);
        t1.setTable(table);
        SendDataResult result = httpTask.requestBlocking(t1);
        Assert.assertNotNull(result);
        Assert.assertEquals(StatusCode.NORMAL, result.getCode());
        Assert.assertEquals("Send 5 tuples succeed.", result.getMsg());
        Assert.assertNull(result.getErrorLinesMap());

        // Also check the callback listener.
        System.out.printf("check key rate: failure_rate=%f, slow_call_rate=%f\n", CircuitBreakerFactory.getInstance().getFailureRate(targetURL, schema, table), CircuitBreakerFactory.getInstance().getSlowCallRate(targetURL, schema, table));
        Assert.assertNotNull(callbackResult[0]);
        Assert.assertEquals(StatusCode.NORMAL, callbackResult[0].getCode());
        Assert.assertEquals("Send 5 tuples succeed.", callbackResult[0].getMsg());
        Assert.assertNull(callbackResult[0].getErrorLinesMap());

        // check circuit breaker behaviour
        // trigger breaker open
        Tuples t2 = prepareTuples(5, "slow");
        t2.setTarget(target);
        t2.setSchema("public");
        t2.setTable(table);
        try {
            httpTask.requestBlocking(t2);
            Thread.sleep(1000);
        } catch (Exception e) {
            // do nothing
        }

        System.out.printf("check key rate: failure_rate=%f, slow_call_rate=%f\n", CircuitBreakerFactory.getInstance().getFailureRate(targetURL, schema, table), CircuitBreakerFactory.getInstance().getSlowCallRate(targetURL, schema, table));
        Assert.assertTrue(CircuitBreakerFactory.getInstance().isBreakerExist(targetURL, schema, table));
        Assert.assertFalse(CircuitBreakerFactory.getInstance().isCircuitBreak(targetURL, schema, table));
        Assert.assertTrue(Float.compare(CircuitBreakerFactory.getInstance().getSlowCallRate(targetURL, schema, table), 50.0f) >= 0);

        try {
            httpTask.requestBlocking(t2);
        } catch (Exception e) {
            // do nothing
        }
        System.out.printf("check key rate: failure_rate=%f, slow_call_rate=%f\n", CircuitBreakerFactory.getInstance().getFailureRate(targetURL, schema, table), CircuitBreakerFactory.getInstance().getSlowCallRate(targetURL, schema, table));
        Assert.assertTrue(CircuitBreakerFactory.getInstance().isCircuitBreak(targetURL, schema, table));
        Assert.assertTrue(Float.compare(CircuitBreakerFactory.getInstance().getSlowCallRate(targetURL, schema, table), slowCallRate) > 0);

        // wait breaker close
        try {
            Thread.sleep(40000);
        } catch (Exception e) {
            // do nothing
        }
        httpTask.requestBlocking(t1);
        Assert.assertFalse(CircuitBreakerFactory.getInstance().isCircuitBreak(targetURL, schema, table));
        Assert.assertTrue(Float.compare(CircuitBreakerFactory.getInstance().getSlowCallRate(targetURL, schema, table), slowCallRate) < 0);
    }

    @Test
    public void TestSendDataBlockingWithCompress() {
        int port = 9008;
        String schema = "public";
        String table = "test_table";
        String rawData = "\"2022-05-18 16:30:06\"|\"102020030\"|\"244\"|\"245\"|\"246\"|\"247\"|\"CAPAC243\"|\"lTxFCVLwcDTKbNbjau_c6243\"|\"lTxFCVLwcDTKbNbjau_c7243\"|\"lTxFCVLwcDTKbNbjau_c8243\"";
        MockHttpServer.RootHandler mockHandler = new MockHttpServer.RootHandler(204, "ok")
                .withCompressDataDecoder(new CompressDataDecoder() {
                    @Override
                    public void decode(String data) {
                        l.info("Decode data in decoder. {}", data);
                        Assert.assertNotNull(data);
                        // After encode by compressor this data will not be equal with the raw data.
                        Assert.assertNotEquals(data, rawData);
                    }
                });
        MockHttpServer.startServerBlocking(port, mockHandler);

        HttpTask httpTask = new HttpTask(null, SingletonHTTPClient.getInstance(1024).getClient(), constructor) {
            @Override
            public void run() {

            }
        };

        // check if normal process logic is affected
        final SendDataResult[] callbackResult = {null};
        httpTask.registerListener(new SendDataListener() {
            @Override
            public void onSuccess(SendDataResult result, Tuples tuples) {
                callbackResult[0] = result;
            }

            @Override
            public void onFailure(SendDataResult result, Tuples tuples) {

            }
        });

        TuplesTarget target = new TuplesTarget();
        String targetURL = String.format("http://localhost:%d/", port);
        target.setURL(targetURL);

        Tuples t1 = prepareTuples(5, rawData);
        t1.setTarget(target);
        t1.setSchema(schema);
        t1.setTable(table);
        t1.setCompress(true);
        SendDataResult result = httpTask.requestBlocking(t1);
        Assert.assertNotNull(result);
        Assert.assertEquals(StatusCode.NORMAL, result.getCode());
        Assert.assertEquals("Send 5 tuples succeed.", result.getMsg());
        Assert.assertNull(result.getErrorLinesMap());

    }

    @Test
    public void TestSendDataBlockingWithNoCompress() {
        int port = 9009;
        String schema = "public";
        String table = "test_table";
        String rawData = "\"2022-05-18 16:30:06\"|\"102020030\"|\"244\"|\"245\"|\"246\"|\"247\"|\"CAPAC243\"|\"lTxFCVLwcDTKbNbjau_c6243\"|\"lTxFCVLwcDTKbNbjau_c7243\"|\"lTxFCVLwcDTKbNbjau_c8243\"";
        String expectedRawData = "\"\"\"2022-05-18 16:30:06\"\"|\"\"102020030\"\"|\"\"244\"\"|\"\"245\"\"|\"\"246\"\"|\"\"247\"\"|\"\"CAPAC243\"\"|\"\"lTxFCVLwcDTKbNbjau_c6243\"\"|\"\"lTxFCVLwcDTKbNbjau_c7243\"\"|\"\"lTxFCVLwcDTKbNbjau_c8243\"\"\"";
        MockHttpServer.RootHandler mockHandler = new MockHttpServer.RootHandler(204, "ok")
                .withCompressDataDecoder(new CompressDataDecoder() {
                    @Override
                    public void decode(String data) {
                        l.info("Decode data in decoder. {}", data);
                        Assert.assertNotNull(data);
                        // After encode by compressor this data will not be equal with the raw data.
                        Assert.assertEquals(data, expectedRawData);
                    }
                });
        MockHttpServer.startServerBlocking(port, mockHandler);

        HttpTask httpTask = new HttpTask(null, SingletonHTTPClient.getInstance(1024).getClient(), constructor) {
            @Override
            public void run() {

            }
        };

        // check if normal process logic is affected
        final SendDataResult[] callbackResult = {null};
        httpTask.registerListener(new SendDataListener() {
            @Override
            public void onSuccess(SendDataResult result, Tuples tuples) {
                callbackResult[0] = result;
            }

            @Override
            public void onFailure(SendDataResult result, Tuples tuples) {

            }
        });

        TuplesTarget target = new TuplesTarget();
        String targetURL = String.format("http://localhost:%d/", port);
        target.setURL(targetURL);

        Tuples t1 = prepareTuples(5, rawData);
        t1.setTarget(target);
        t1.setSchema(schema);
        t1.setTable(table);
        t1.setCompress(false);
        SendDataResult result = httpTask.requestBlocking(t1);
        Assert.assertNotNull(result);
        Assert.assertEquals(StatusCode.NORMAL, result.getCode());
        Assert.assertEquals("Send 5 tuples succeed.", result.getMsg());
        Assert.assertNull(result.getErrorLinesMap());

    }


    private Tuples prepareTuples(final int tupleSize, final String toStringResult) {
        return new Tuples() {
            private TuplesTarget target;
            private static final String delimiter = "|";
            private static final String schema = "public";
            private static final String table = "table";
            private final int size = tupleSize;
            private static final int csvBatchSize = 10;

            private Tuple generateTuple(String delimiter, String schema, String table) {
                return Utils.generateEmptyTupleLite(delimiter, schema, table);
            }

            private List<Tuple> generateTuplesList(int size, String delimiter, String schema, String table) {
                List<Tuple> tuplesList = new ArrayList<>(size);
                for (int i = 0; i < size; i++) {
                    Tuple tuple = generateTuple(delimiter, schema, table);
                    tuple.addColumn("c1", toStringResult);
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
