package cn.ymatrix.api;

import cn.ymatrix.apiserver.GetJobMetadataListener;
import cn.ymatrix.apiserver.SendDataListener;
import cn.ymatrix.apiserver.SendDataResult;
import cn.ymatrix.builder.CircuitBreakerConfig;
import cn.ymatrix.builder.Utils;
import cn.ymatrix.compress.CompressionFactory;
import cn.ymatrix.compress.Compressor;
import cn.ymatrix.data.Tuple;
import cn.ymatrix.data.Tuples;
import cn.ymatrix.data.TuplesTarget;
import cn.ymatrix.exception.RetryException;
import cn.ymatrix.faulttolerance.CircuitBreakerFactory;
import cn.ymatrix.logger.MxLogger;
import cn.ymatrix.utils.CSVConstructor;
import cn.ymatrix.utils.StrUtil;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static java.nio.charset.StandardCharsets.UTF_8;


/**
 * CREATE TABLE test_table(
 * ts timestamp
 * ,tag int NOT NULL
 * ,c1 float
 * ,c2 double precision
 * ,c3 text
 * )
 */

public class TestGrpcClient {
    Logger l = MxLogger.init(TestGrpcClient.class);
    private static final String host = "localhost";
    private static final String schema = "public";
    private static final String table = "test_table";
    private static final String delimiter = "|";
    private static final int timeoutMillis = 2000;
    private MxServerBackend gRPCServer;
    private MxGrpcClient gRPCClient;
    @Test
    public void TestMxClientGetNormalMetadata() {
        GetJobMetadataTestCaseFramework(new TestCase() {
            @Override
            public void tcRun() throws InterruptedException {
                startGRPCClientServer(9101, table, null, prepareNormalJobMetadata(), null);
            }
        }, new TestCaseCheck() {
            @Override
            public void checkBoolean(boolean excepted) {
                Assert.assertTrue(excepted);
            }

            @Override
            public void checkJobMetadataWrapperOnSuccess(JobMetadataWrapper metadata) {
                Assert.assertEquals(metadata.getDelimiter(), delimiter);
                Assert.assertEquals(metadata.getSchema(), schema);
                Assert.assertEquals(metadata.getTable(), table);
                Assert.assertEquals(metadata.getColumnMetaList().size(), 5); // Contains 5 columns.
            }

            @Override
            public void checkMsgOnFailure(String errorMsg) {
                // Nothing to do.
            }
        });
    }

    @Test
    public void TestGetJobMetadataInvalidResponseCode() {
        GetJobMetadataTestCaseFramework(new TestCase() {
            @Override
            public void tcRun() throws InterruptedException {
                // Inject invalid response code = 1.
                startGRPCClientServer(9102, table, null, prepareInvalidJobMetadataResponseCode(1), null);
            }
        }, new TestCaseCheck() {
            @Override
            public void checkBoolean(boolean excepted) {
                Assert.assertFalse(excepted);
            }

            @Override
            public void checkJobMetadataWrapperOnSuccess(JobMetadataWrapper metadata) {
                // Nothing to do.
            }

            @Override
            public void checkMsgOnFailure(String errorMsg) {
                Assert.assertTrue(errorMsg.contains("Get error from get job metadata gRPC API for table public.test_table"));
                Assert.assertTrue(errorMsg.contains("responseCode=1"));
            }
        });

        GetJobMetadataTestCaseFramework(new TestCase() {
            @Override
            public void tcRun() throws InterruptedException {
                // Inject invalid response code = 2.
                startGRPCClientServer(9103, table, null, prepareInvalidJobMetadataResponseCode(2), null);
            }
        }, new TestCaseCheck() {
            @Override
            public void checkBoolean(boolean excepted) {
                Assert.assertFalse(excepted);
            }

            @Override
            public void checkJobMetadataWrapperOnSuccess(JobMetadataWrapper metadata) {
                // Nothing to do.
            }

            @Override
            public void checkMsgOnFailure(String errorMsg) {
                Assert.assertTrue(errorMsg.contains("Get error from get job metadata gRPC API for table public.test_table"));
                Assert.assertTrue(errorMsg.contains("responseCode=2"));
            }
        });

        GetJobMetadataTestCaseFramework(new TestCase() {
            @Override
            public void tcRun() throws InterruptedException {
                // Inject invalid response code = 3.
                startGRPCClientServer(9104, table, null, prepareInvalidJobMetadataResponseCode(3), null);
            }
        }, new TestCaseCheck() {
            @Override
            public void checkBoolean(boolean excepted) {
                Assert.assertFalse(excepted);
            }

            @Override
            public void checkJobMetadataWrapperOnSuccess(JobMetadataWrapper metadata) {
                // Nothing to do.
            }

            @Override
            public void checkMsgOnFailure(String errorMsg) {
                Assert.assertTrue(errorMsg.contains("Get error from get job metadata gRPC API for table public.test_table"));
                Assert.assertTrue(errorMsg.contains("responseCode=3"));
            }
        });

        GetJobMetadataTestCaseFramework(new TestCase() {
            @Override
            public void tcRun() throws InterruptedException {
                // Inject invalid response code = 9.
                startGRPCClientServer(9105, table, null, prepareInvalidJobMetadataResponseCode(9), null);
            }
        }, new TestCaseCheck() {
            @Override
            public void checkBoolean(boolean excepted) {
                Assert.assertFalse(excepted);
            }

            @Override
            public void checkJobMetadataWrapperOnSuccess(JobMetadataWrapper metadata) {
                // Nothing to do.
            }

            @Override
            public void checkMsgOnFailure(String errorMsg) {
                Assert.assertTrue(errorMsg.contains("Get error from get job metadata gRPC API for table public.test_table"));
                Assert.assertTrue(errorMsg.contains("responseCode=9"));
            }
        });

        GetJobMetadataTestCaseFramework(new TestCase() {
            @Override
            public void tcRun() throws InterruptedException {
                // Inject invalid response code = 10.
                startGRPCClientServer(9106, table, null, prepareInvalidJobMetadataResponseCode(10), null);
            }
        }, new TestCaseCheck() {
            @Override
            public void checkBoolean(boolean excepted) {
                Assert.assertFalse(excepted);
            }

            @Override
            public void checkJobMetadataWrapperOnSuccess(JobMetadataWrapper metadata) {
                // Nothing to do.
            }

            @Override
            public void checkMsgOnFailure(String errorMsg) {
                Assert.assertTrue(errorMsg.contains("Get error from get job metadata gRPC API for table public.test_table"));
                Assert.assertTrue(errorMsg.contains("responseCode=10"));
            }
        });
    }

    @Test
    public void TestGetJobMetadataInvalidResponseCode2() {
        GetJobMetadataTestCaseFramework(new TestCase() {
            @Override
            public void tcRun() throws InterruptedException {
                // Inject invalid response code = 4.
                startGRPCClientServer(9107, table, null, prepareInvalidJobMetadataResponseCode(4), null);
            }
        }, new TestCaseCheck() {
            @Override
            public void checkBoolean(boolean excepted) {
                Assert.assertFalse(excepted);
            }

            @Override
            public void checkJobMetadataWrapperOnSuccess(JobMetadataWrapper metadata) {
                // Nothing to do.
            }

            @Override
            public void checkMsgOnFailure(String errorMsg) {
                Assert.assertTrue(errorMsg.contains("Invalid request of get job metadata gRPC API  of table public.test_table"));
                Assert.assertTrue(errorMsg.contains("responseCode=4"));
            }
        });

        GetJobMetadataTestCaseFramework(new TestCase() {
            @Override
            public void tcRun() throws InterruptedException {
                // Inject invalid response code = 5.
                startGRPCClientServer(9108, table, null, prepareInvalidJobMetadataResponseCode(5), null);
            }
        }, new TestCaseCheck() {
            @Override
            public void checkBoolean(boolean excepted) {
                Assert.assertFalse(excepted);
            }

            @Override
            public void checkJobMetadataWrapperOnSuccess(JobMetadataWrapper metadata) {
                // Nothing to do.
            }

            @Override
            public void checkMsgOnFailure(String errorMsg) {
                Assert.assertTrue(errorMsg.contains("Invalid request of get job metadata gRPC API  of table public.test_table"));
                Assert.assertTrue(errorMsg.contains("responseCode=5"));
            }
        });

        GetJobMetadataTestCaseFramework(new TestCase() {
            @Override
            public void tcRun() throws InterruptedException {
                // Inject invalid response code = 6.
                startGRPCClientServer(9109, table, null, prepareInvalidJobMetadataResponseCode(6), null);
            }
        }, new TestCaseCheck() {
            @Override
            public void checkBoolean(boolean excepted) {
                Assert.assertFalse(excepted);
            }

            @Override
            public void checkJobMetadataWrapperOnSuccess(JobMetadataWrapper metadata) {
                // Nothing to do.
            }

            @Override
            public void checkMsgOnFailure(String errorMsg) {
                Assert.assertTrue(errorMsg.contains("Invalid request of get job metadata gRPC API  of table public.test_table"));
                Assert.assertTrue(errorMsg.contains("responseCode=6"));
            }
        });
    }

    @Test(expected = RetryException.class)
    public void TestGetJobMetadataInvalidResponseCodeException() {
        try {
            GetJobMetadataTestCaseFramework(new TestCase() {
                @Override
                public void tcRun() throws InterruptedException {
                    // Inject invalid response code = 7.
                    startGRPCClientServer(9110, table, null, prepareInvalidJobMetadataResponseCode(7), null);
                }
            }, new TestCaseCheck() {
                @Override
                public void checkBoolean(boolean excepted) {
                    Assert.assertFalse(excepted);
                }

                @Override
                public void checkJobMetadataWrapperOnSuccess(JobMetadataWrapper metadata) {
                    // Nothing to do.
                }

                @Override
                public void checkMsgOnFailure(String errorMsg) {
                    Assert.assertTrue(errorMsg.contains("Server is not ready for get job metadata gRPC API  of table public.test_table"));
                    Assert.assertTrue(errorMsg.contains("responseCode=7"));
                }
            });
        } catch (RetryException e) {
            stopGRPCServer();
            throw e;
        }
    }

    @Test(expected = RetryException.class)
    public void TestGetJobMetadataInvalidResponseCodeException2() {
        try {
            GetJobMetadataTestCaseFramework(new TestCase() {
                @Override
                public void tcRun() throws InterruptedException {
                    // Inject invalid response code = 8.
                    startGRPCClientServer(9111, table, null, prepareInvalidJobMetadataResponseCode(8), null);
                }
            }, new TestCaseCheck() {
                @Override
                public void checkBoolean(boolean excepted) {
                    Assert.assertFalse(excepted);
                }

                @Override
                public void checkJobMetadataWrapperOnSuccess(JobMetadataWrapper metadata) {
                    // Nothing to do.
                }

                @Override
                public void checkMsgOnFailure(String errorMsg) {
                    Assert.assertTrue(errorMsg.contains("Server is not ready for get job metadata gRPC API  of table public.test_table"));
                    Assert.assertTrue(errorMsg.contains("responseCode=8"));
                }
            });
        } catch (RetryException e) {
            stopGRPCServer();
            throw e;
        }
    }


    @Test
    public void TestGetEmptyJobMetadata() {
        GetJobMetadataTestCaseFramework(new TestCase() {
            @Override
            public void tcRun() throws InterruptedException {
                startGRPCClientServer(9112, table, null, prepareInvalidJobMetadataEmptyResponse(), null);
            }
        }, new TestCaseCheck() {
            @Override
            public void checkBoolean(boolean excepted) {
                Assert.assertFalse(excepted);
            }

            @Override
            public void checkJobMetadataWrapperOnSuccess(JobMetadataWrapper metadata) {

            }

            @Override
            public void checkMsgOnFailure(String errorMsg) {
                Assert.assertTrue(errorMsg.contains("column metadata list is empty of table public.test_table"));
            }
        });
    }

    @Test
    public void TestGetJobMetadataNotDelimiter() {
        GetJobMetadataTestCaseFramework(new TestCase() {
            @Override
            public void tcRun() throws InterruptedException {
                startGRPCClientServer(9113, table, null, prepareJobMetadataNoDelimiter(), null);
            }
        }, new TestCaseCheck() {
            @Override
            public void checkBoolean(boolean excepted) {
                Assert.assertFalse(excepted);
            }

            @Override
            public void checkJobMetadataWrapperOnSuccess(JobMetadataWrapper metadata) {
                // Nothing to do.
            }

            @Override
            public void checkMsgOnFailure(String errorMsg) {
                Assert.assertTrue(errorMsg.contains("no delimiter set for table public.test_table"));
            }
        });
    }

    @Test
    public void TestGetJobMetadataNoNum() {
        GetJobMetadataTestCaseFramework(new TestCase() {
            @Override
            public void tcRun() throws InterruptedException {
                startGRPCClientServer(9114, table, null, prepareJobMetadataNoNum(), null);
            }
        }, new TestCaseCheck() {
            @Override
            public void checkBoolean(boolean excepted) {
                Assert.assertFalse(excepted);
            }

            @Override
            public void checkJobMetadataWrapperOnSuccess(JobMetadataWrapper metadata) {
                // Nothing to do.
            }

            @Override
            public void checkMsgOnFailure(String errorMsg) {
                Assert.assertTrue(errorMsg.contains("column num is <= 0 of table public.test_table"));
            }
        });
    }

    @Test
    public void TestGetJobMetadataNoType() {
        GetJobMetadataTestCaseFramework(new TestCase() {
            @Override
            public void tcRun() throws InterruptedException {
                startGRPCClientServer(9115, table, null, prepareJobMetadataNoType(), null);
            }
        }, new TestCaseCheck() {
            @Override
            public void checkBoolean(boolean excepted) {
                Assert.assertFalse(excepted);
            }

            @Override
            public void checkJobMetadataWrapperOnSuccess(JobMetadataWrapper metadata) {
                // Nothing to do.
            }

            @Override
            public void checkMsgOnFailure(String errorMsg) {
                Assert.assertTrue(errorMsg.contains("column type is empty of table public.test_table"));
            }
        });
    }

    @Test
    public void TestGateJobMetadataNoName() {
        GetJobMetadataTestCaseFramework(new TestCase() {
            @Override
            public void tcRun() throws InterruptedException {
                startGRPCClientServer(9116, table, null, prepareJobMetadataNoName(), null);
            }
        }, new TestCaseCheck() {
            @Override
            public void checkBoolean(boolean excepted) {
                Assert.assertFalse(excepted);
            }

            @Override
            public void checkJobMetadataWrapperOnSuccess(JobMetadataWrapper metadata) {
                // Nothing to do.
            }

            @Override
            public void checkMsgOnFailure(String errorMsg) {
                Assert.assertTrue(errorMsg.contains("column name is empty of table public.test_table"));
            }
        });
    }


    private void GetJobMetadataTestCaseFramework(TestCase tc, TestCaseCheck checker) throws NullPointerException, RetryException {
        boolean exceptionHappen = false;

        // Run testcase.
        try {
            tc.tcRun();
        } catch (Exception e) {
            l.error("Start gRPC client and server exception: {}", e.getMessage());
            exceptionHappen = true;
        }
        Assert.assertFalse(exceptionHappen);

        // Get job metadata.
        // GetJobMetadataListener will be called in another thread.
        CountDownLatch latch = new CountDownLatch(1);
        final boolean[] expected = {false};
        getJobMetadata(new GetJobMetadataListener() {
            @Override
            public void onSuccess(JobMetadataWrapper metadata) {
                l.info("Test get job metadata success.");
                expected[0] = true;
                latch.countDown();
                checker.checkJobMetadataWrapperOnSuccess(metadata);
            }

            @Override
            public void onFailure(String failureMsg) {
                l.info("Test get job metadata failure: {}", failureMsg);
                expected[0] = false;
                latch.countDown();
                checker.checkMsgOnFailure(failureMsg);
            }
        });

        // Wait
        try {
            Assert.assertTrue(latch.await(3, TimeUnit.SECONDS));
        } catch (Exception e) {
            l.error("CountLatch wait exception: {}", e.getMessage());
            exceptionHappen = true;
        }

        // gRPC Server must be stopped successfully.
        Assert.assertTrue(stopGRPCServer());

        // No exception happened.
        Assert.assertFalse(exceptionHappen);

        // Get job metadata will fail for the response code == 1.
        checker.checkBoolean(expected[0]);
    }

    @Test
    public void TestSendTuplesAllSuccess() {
        sendDataNormalTestFramework(9117, null);
    }

    @Test
    public void TestSendTuplesPartialFailed() {
        Map<Long, String> errorLines = new HashMap<>();
        errorLines.put(new Long(3), "bulabulabula");
        errorLines.put(new Long(8), "bulabulabula again");
        sendDataNormalTestFramework(9118, errorLines);
    }

    private void sendDataNormalTestFramework(int port, Map<Long, String> errorLines) {
        int tupleCnt = 10;

        Tuples tuples = prepareTuplesWithRawData("sendDataNormalTestFramework", tupleCnt);
        TuplesTarget target = new TuplesTarget();
        target.setURL(String.format("localhost:%d", port));
        tuples.setTarget(target);
        tuples.setSchema(schema);
        tuples.setTable(table);

        try {
            startGRPCClientServer(port, table, null, prepareNormalJobMetadata(), prepareSendDataNormal(tupleCnt, errorLines));
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        final SendDataResult[] callbackResult = {null};
        final CountDownLatch latch = new CountDownLatch(1);
        SendDataListener listener = new SendDataListener() {
            @Override
            public void onSuccess(SendDataResult result, Tuples tuples) {
                callbackResult[0] = result;
                latch.countDown();
            }

            @Override
            public void onFailure(SendDataResult result, Tuples tuples) {
                callbackResult[0] = result;
                latch.countDown();
            }
        };
        SendDataResult result = gRPCClient.sendDataBlocking(prepareTuplesWithRawData("", 1), null, false, false, listener);

        try {
            Assert.assertTrue(latch.await(1, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        Assert.assertNotNull(result);
        Assert.assertNotNull(callbackResult[0]);
        String expectMsg;
        if (errorLines == null) {
            Assert.assertEquals(StatusCode.NORMAL, result.getCode());
            Assert.assertEquals(StatusCode.NORMAL, callbackResult[0].getCode());

            Assert.assertNull(result.getErrorLinesMap());
            Assert.assertNull(callbackResult[0].getErrorLinesMap());

            expectMsg = StrUtil.connect("Send data successfully for table ", schema, ".", table);
        } else {
            Assert.assertEquals(StatusCode.PARTIALLY_TUPLES_FAIL, result.getCode());
            Assert.assertEquals(StatusCode.PARTIALLY_TUPLES_FAIL, callbackResult[0].getCode());

            Assert.assertNotNull(result.getErrorLinesMap());
            Assert.assertNotNull(callbackResult[0].getErrorLinesMap());
            Assert.assertEquals(errorLines, result.getErrorLinesMap());
            Assert.assertEquals(errorLines, callbackResult[0].getErrorLinesMap());
            expectMsg = StrUtil.connect("Send data to server with ", String.valueOf(errorLines.size()),
                    " error lines from gRPC response of table", schema, ".", table);
        }
        Assert.assertEquals(expectMsg, result.getMsg());
        Assert.assertEquals(expectMsg, callbackResult[0].getMsg());
    }

    @Test(expected = RetryException.class)
    public void TestSendTuplesTimeout() {
        String m = "whoops...";
        int tupleCnt = 5;
        int port = 9119;

        Tuples tuples = prepareTuplesWithRawData(m, tupleCnt);
        TuplesTarget target = new TuplesTarget();
        target.setURL(String.format("%s:%d", host, port));
        tuples.setTarget(target);
        tuples.setSchema(schema);
        tuples.setTable(table);

        try {
            startGRPCClientServer(port, table, null, prepareNormalJobMetadata(), prepareSendDataFailedWithMsg(MxGrpcClient.GRPC_RESPONSE_CODE_TIMEOUT, m));
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        final SendDataResult[] callbackResult = {null};
        final CountDownLatch latch = new CountDownLatch(1);
        SendDataListener listener = new SendDataListener() {
            @Override
            public void onSuccess(SendDataResult result, Tuples tuples) {}

            @Override
            public void onFailure(SendDataResult result, Tuples tuples) {
                callbackResult[0] = result;
                latch.countDown();
            }
        };
        gRPCClient.sendDataBlocking(prepareTuplesWithRawData("", 1), null, false, false, listener);
    }

    @Test(expected = RetryException.class)
    public void TestSendTuplesAllFailed() {
        String m = "whoops...";
        int tupleCnt = 5;
        int port = 9120;

        Tuples tuples = prepareTuplesWithRawData(m, tupleCnt);
        TuplesTarget target = new TuplesTarget();
        target.setURL(String.format("%s:%d", host, port));
        tuples.setTarget(target);
        tuples.setSchema(schema);
        tuples.setTable(table);

        try {
            startGRPCClientServer(port, table, null, prepareNormalJobMetadata(), prepareSendDataFailedWithMsg(MxGrpcClient.GRPC_RESPONSE_CODE_ALL_TUPLES_FAILED, m));
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        final SendDataResult[] callbackResult = {null};
        final CountDownLatch latch = new CountDownLatch(1);
        SendDataListener listener = new SendDataListener() {
            @Override
            public void onSuccess(SendDataResult result, Tuples tuples) {}

            @Override
            public void onFailure(SendDataResult result, Tuples tuples) {
                callbackResult[0] = result;
                latch.countDown();
            }
        };
        gRPCClient.sendDataBlocking(prepareTuplesWithRawData("", 1), null, false, false, listener);
    }

    @Test
    public void TestSendTuplesInvalidRequest() {
        String mHeader = "Invalid request of send data gRPC API ";
        String m = "whoops...";
        failTestFramework(9121, mHeader, m, MxGrpcClient.GRPC_RESPONSE_CODE_INVALID_REQUEST, StatusCode.ERROR);
    }

    @Test
    public void TestSendTuplesInvalidConfig() {
        String mHeader = "Invalid request of send data gRPC API ";
        String m = "whoops...";
        failTestFramework(9122, mHeader, m, MxGrpcClient.GRPC_RESPONSE_CODE_INVALID_CONFIG, StatusCode.ERROR);
    }

    @Test
    public void TestSendTuplesInvalidTable() {
        String mHeader = "Invalid request of send data gRPC API ";
        String m = "whoops...";
        failTestFramework(9123, mHeader, m, MxGrpcClient.GRPC_RESPONSE_CODE_INVALID_TABLE, StatusCode.ERROR);
    }

    @Test
    public void TestSendTuplesUnDefinedError() {
        String mHeader = "Get error from send data gRPC API ";
        String m = "whoops...";
        failTestFramework(9124, mHeader, m, MxGrpcClient.GRPC_RESPONSE_CODE_ALL_UNDEFINED_ERROR, StatusCode.ALL_TUPLES_FAIL);
    }

    private void failTestFramework(int port, String errMsgHeader, String errMsg, int errCode, StatusCode expectCode) {
        int tupleCnt = 5;

        Tuples tuples = prepareTuplesWithRawData("failTestFramework", tupleCnt);
        TuplesTarget target = new TuplesTarget();
        target.setURL(String.format("%s:%d", host, port));
        tuples.setTarget(target);
        tuples.setSchema(schema);
        tuples.setTable(table);

        try {
            startGRPCClientServer(port, table, null, prepareNormalJobMetadata(), prepareSendDataFailedWithMsg(errCode, errMsg));
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        final SendDataResult[] callbackResult = {null};
        final CountDownLatch latch = new CountDownLatch(1);
        SendDataListener listener = new SendDataListener() {
            @Override
            public void onSuccess(SendDataResult result, Tuples tuples) {
            }

            @Override
            public void onFailure(SendDataResult result, Tuples tuples) {
                callbackResult[0] = result;
                latch.countDown();
            }
        };
        SendDataResult result = gRPCClient.sendDataBlocking(prepareTuplesWithRawData("", 1), null, false, false, listener);

        try {
            Assert.assertTrue(latch.await(1, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        Assert.assertNotNull(result);
        Assert.assertEquals(expectCode, result.getCode());
        String expectMsg = StrUtil.connect(errMsgHeader, errMsg, " of table ", schema, ".", table);;
        Assert.assertEquals(expectMsg, result.getMsg());
        Assert.assertNull(result.getErrorLinesMap());
        // Also check the callback listener.
        Assert.assertNotNull(callbackResult[0]);
        Assert.assertEquals(expectCode, callbackResult[0].getCode());
        Assert.assertEquals(expectMsg, callbackResult[0].getMsg());
        Assert.assertNull(callbackResult[0].getErrorLinesMap());
    }

    @Test
    public void TestSendTuplesWithCircuitBreakerOnByFailure() {
        int port = 9125;
        String targetURL = String.format("%s:%d", host, port);
        String table = "breakerOnByFailure";
        Tuples tuples = prepareTuplesWithRawData(table, 5);
        TuplesTarget target = new TuplesTarget();
        target.setURL(targetURL);
        tuples.setTarget(target);
        tuples.setSchema(schema);
        tuples.setTable(table);

        CircuitBreakerConfig cbc = new CircuitBreakerConfig()
                .setEnable()
                .setMinimumNumberOfCalls(1)
                .setSlidingWindowSize(10)
                .setFailureRateThreshold(60.0f);

        try {
            startGRPCClientServer(port, table, cbc, prepareNormalJobMetadata(), prepareSendDataGetException(5, "test"));
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        gRPCClient.sendDataBlocking(prepareTuplesWithRawData("success", 1), null, false, false, null); // success
        Assert.assertFalse(CircuitBreakerFactory.getInstance().isCircuitBreak(targetURL, schema, table));

        try {
            gRPCClient.sendDataBlocking(prepareTuplesWithRawData("exception", 1), null, false, false, null); // failed
        } catch (Exception e) {
            // do nothing
        }
        gRPCClient.sendDataBlocking(prepareTuplesWithRawData("success", 1), null, false, false, null); // success
        try {
            gRPCClient.sendDataBlocking(prepareTuplesWithRawData("exception", 1), null, false, false, null); // failed
        } catch (Exception e) {
            // do nothing
        }
        try {
            gRPCClient.sendDataBlocking(prepareTuplesWithRawData("exception", 1), null, false, false, null); // failed
        } catch (Exception e) {
            // do nothing
        }
        Assert.assertTrue(CircuitBreakerFactory.getInstance().isCircuitBreak(targetURL, schema, table));
        try {
            Thread.sleep(40000);
        } catch (Exception e){
            // do nothing
        }
        gRPCClient.sendDataBlocking(prepareTuplesWithRawData("success", 1), null, false, false, null); // success
        Assert.assertFalse(CircuitBreakerFactory.getInstance().isCircuitBreak(targetURL, schema, table));
    }

    @Test
    public void TestSendTuplesWithCircuitBreakerOnBySlowCall() {
        int port = 9126;
        String targetURL = String.format("%s:%d", host, port);
        String table = "breakerOnBySlowCall";
        Tuples tuples = prepareTuplesWithRawData(table,5);
        TuplesTarget target = new TuplesTarget();
        target.setURL(targetURL);
        tuples.setTarget(target);
        tuples.setSchema(schema);
        tuples.setTable(table);

        int slowLimit = 800;
        CircuitBreakerConfig cbc = new CircuitBreakerConfig()
                .setEnable()
                .setMinimumNumberOfCalls(1)
                .setSlidingWindowSize(10)
                .setFailureRateThreshold(100.0f)
                .setSlowCallDurationThresholdMillis(slowLimit)
                .setSlowCallRateThreshold(60.0f);

        try {
            startGRPCClientServer(port, table, cbc, prepareNormalJobMetadata(), prepareSendDataWithTimeCost(5, 2*slowLimit));
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        gRPCClient.sendDataBlocking(prepareTuplesWithRawData("success", 1), null, false, false, null); // normal
        Assert.assertFalse(CircuitBreakerFactory.getInstance().isCircuitBreak(targetURL, schema, table));

        try {
            gRPCClient.sendDataBlocking(prepareTuplesWithRawData("slow", 1), null, false, false, null); // slow call
        } catch (Exception e) {
            // do nothing
        }
        gRPCClient.sendDataBlocking(prepareTuplesWithRawData("success", 1), null, false, false, null); // normal
        try {
            gRPCClient.sendDataBlocking(prepareTuplesWithRawData("slow", 1), null, false, false, null); // slow call
        } catch (Exception e) {
            // do nothing
        }
        try {
            gRPCClient.sendDataBlocking(prepareTuplesWithRawData("slow", 1), null, false, false, null); // slow call
        } catch (Exception e) {
            // do nothing
        }
        try {
            gRPCClient.sendDataBlocking(prepareTuplesWithRawData("slow", 1), null, false, false, null); // slow call
        } catch (Exception e) {
            // do nothing
        }
        System.out.println(CircuitBreakerFactory.getInstance().getFailureRate(targetURL, schema, table));
        System.out.println(CircuitBreakerFactory.getInstance().getSlowCallRate(targetURL, schema, table));
        Assert.assertTrue(CircuitBreakerFactory.getInstance().isCircuitBreak(targetURL, schema, table));
        try {
            Thread.sleep(40000);
        } catch (Exception e){
            // do nothing
        }
        gRPCClient.sendDataBlocking(prepareTuplesWithRawData("success", 1), null, false, false, null); // normal
        Assert.assertFalse(CircuitBreakerFactory.getInstance().isCircuitBreak(targetURL, schema, table));
    }

    @Test
    public void TestSendCompressedData() {
        int port = 9127;
        String rawDataString = "1234";
        CountDownLatch latch = new CountDownLatch(1);
        try {
            startGRPCClientServer(port, table, null,
                    prepareNormalJobMetadata(), prepareSendDataHookWithCompressDataDecoder(rawDataString, latch, new CompressDataDecoder() {
                        @Override
                        public void decode(String data) {
                            byte[] decodeBytes = Base64.getDecoder().decode(data.getBytes(UTF_8));
                            Assert.assertNotNull(decodeBytes);
                            Compressor compressor = CompressionFactory.getCompressor();
                            // With " and \n the length need to plus 3.
                            byte[] decompressedByte =  compressor.decompress(decodeBytes, rawDataString.getBytes(UTF_8).length + 3);
                            Assert.assertNotNull(decompressedByte);
                            l.info("decode bytes in hook {}", new String(decompressedByte, UTF_8));
                            Assert.assertEquals(new String(decompressedByte, UTF_8), "\"" + rawDataString + "\"" + "\n");
                            latch.countDown();
                        }
                    }));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        gRPCClient.sendDataBlocking(prepareTuplesWithRawData(rawDataString, 1), null, true, true, null);

        try {
            Assert.assertTrue(latch.await(5, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void TestSendNoCompressedData() {
        int port = 9128;
        String rawDataString = "This is a raw data string for no compress test";
        CountDownLatch latch = new CountDownLatch(1);
        try {
            startGRPCClientServer(port, table, null,
                    prepareNormalJobMetadata(), prepareSendDataHookWithCompressDataDecoder(rawDataString, latch, new CompressDataDecoder() {
                        @Override
                        public void decode(String data) {
                            l.info("decode data {}", data);
                            Assert.assertEquals("\"" + rawDataString + "\"" + "\n", data);
                            latch.countDown();
                        }
                    }));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        gRPCClient.sendDataBlocking(prepareTuplesWithRawData(rawDataString, 1), null, false, false, null);

        try {
            Assert.assertTrue(latch.await(5, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void startGRPCClientServer(int port, String table, CircuitBreakerConfig cbc,
                                       JobMetadataHook hook1, SendDataHook hook2) throws InterruptedException {
        // Start gRPC server and client.
        newMxGRPCClient(port, table, cbc);
        newMxServerBackendWithHook(port, hook1, hook2);
        startRPCServerBlocking();
        // Sleep some seconds to wait for the server to start.
        threadSleep(3000);
    }


    private void startRPCServerBlocking() {
        if (gRPCServer != null) {
            new Thread() {
                @Override
                public void run() {
                    gRPCServer.startBlocking();
                }
            }.start();
        }
    }

    private boolean stopGRPCServer() {
        if (this.gRPCServer != null) {
            try {
                this.gRPCServer.stop();
                return true;
            } catch (InterruptedException e) {
                l.error("Stop gRPC server exception: {}", e.getMessage());
                return false;
            }
        }
        return false;
    }

    private void getJobMetadata(GetJobMetadataListener listener) throws NullPointerException, RetryException {
        if (gRPCClient != null) {
            gRPCClient.getJobMetadataBlocking(listener, null);
        }
    }

    private void newMxServerBackendWithHook(int port, JobMetadataHook hook1, SendDataHook hook2) {
        gRPCServer = MxServerBackend.getServerInstance(port, hook1, hook2);
        l.info("new grpc server.");
    }

    private void newMxGRPCClient(int port, String table, CircuitBreakerConfig cbc) {
        gRPCClient = MxGrpcClient.prepareMxGrpcClient(host + ":" + port, schema, table, timeoutMillis, cbc, new CSVConstructor(10));
    }

    private void threadSleep(long millis) throws InterruptedException {
        Thread.sleep(millis);
    }

    private JobMetadataHook prepareNormalJobMetadata() {
        JobMetadataHook.Builder builder = JobMetadataHook.newBuilder();
        builder.setDelimiter(delimiter);
        builder.setSchemaTable(schema, table);
        builder.addJobMetadataHook(MetadataHook.newBuilder().setNum(1).setType("timestamp").setName("ts").Build());
        builder.addJobMetadataHook(MetadataHook.newBuilder().setNum(2).setType("int").setName("tag").Build());
        builder.addJobMetadataHook(MetadataHook.newBuilder().setNum(3).setType("float").setName("c1").Build());
        builder.addJobMetadataHook(MetadataHook.newBuilder().setNum(4).setType("double precision").setName("c2").Build());
        builder.addJobMetadataHook(MetadataHook.newBuilder().setNum(5).setType("text").setName("c3").Build());
        builder.setResponseCode(0);
        return builder.build();
    }

    private JobMetadataHook prepareJobMetadataNoDelimiter() {
        JobMetadataHook.Builder builder = JobMetadataHook.newBuilder();
        builder.setSchemaTable(schema, table);
        builder.addJobMetadataHook(MetadataHook.newBuilder().setNum(1).setType("timestamp").setName("ts").Build());
        builder.addJobMetadataHook(MetadataHook.newBuilder().setNum(2).setType("int").setName("tag").Build());
        builder.addJobMetadataHook(MetadataHook.newBuilder().setNum(3).setType("float").setName("c1").Build());
        builder.addJobMetadataHook(MetadataHook.newBuilder().setNum(4).setType("double precision").setName("c2").Build());
        builder.addJobMetadataHook(MetadataHook.newBuilder().setNum(5).setType("text").setName("c3").Build());
        builder.setResponseCode(0);
        return builder.build();
    }

    private JobMetadataHook prepareJobMetadataNoNum() {
        JobMetadataHook.Builder builder = JobMetadataHook.newBuilder();
        builder.setDelimiter(delimiter);
        builder.setSchemaTable(schema, table);
        builder.addJobMetadataHook(MetadataHook.newBuilder().setType("timestamp").setName("ts").Build());
        builder.addJobMetadataHook(MetadataHook.newBuilder().setType("int").setName("tag").Build());
        builder.addJobMetadataHook(MetadataHook.newBuilder().setType("float").setName("c1").Build());
        builder.addJobMetadataHook(MetadataHook.newBuilder().setType("double precision").setName("c2").Build());
        builder.addJobMetadataHook(MetadataHook.newBuilder().setType("text").setName("c3").Build());
        builder.setResponseCode(0);
        return builder.build();
    }

    private JobMetadataHook prepareJobMetadataNoType() {
        JobMetadataHook.Builder builder = JobMetadataHook.newBuilder();
        builder.setDelimiter("|");
        builder.setSchemaTable(schema, table);
        builder.addJobMetadataHook(MetadataHook.newBuilder().setNum(1).setName("ts").Build());
        builder.addJobMetadataHook(MetadataHook.newBuilder().setNum(2).setName("tag").Build());
        builder.addJobMetadataHook(MetadataHook.newBuilder().setNum(3).setName("c1").Build());
        builder.addJobMetadataHook(MetadataHook.newBuilder().setNum(4).setName("c2").Build());
        builder.addJobMetadataHook(MetadataHook.newBuilder().setNum(5).setName("c3").Build());
        builder.setResponseCode(0);
        return builder.build();
    }

    private JobMetadataHook prepareJobMetadataNoName() {
        JobMetadataHook.Builder builder = JobMetadataHook.newBuilder();
        builder.setDelimiter(delimiter);
        builder.setSchemaTable(schema, table);
        builder.addJobMetadataHook(MetadataHook.newBuilder().setNum(1).setType("timestamp").Build());
        builder.addJobMetadataHook(MetadataHook.newBuilder().setNum(2).setType("int").Build());
        builder.addJobMetadataHook(MetadataHook.newBuilder().setNum(3).setType("float").Build());
        builder.addJobMetadataHook(MetadataHook.newBuilder().setNum(4).setType("double precision").Build());
        builder.addJobMetadataHook(MetadataHook.newBuilder().setNum(5).setType("text").Build());
        builder.setResponseCode(0);
        return builder.build();
    }

    // Empty job metadata columns
    private JobMetadataHook prepareInvalidJobMetadataEmptyResponse() {
        JobMetadataHook.Builder builder = JobMetadataHook.newBuilder();
        builder.setDelimiter(delimiter);
        builder.setSchemaTable(schema, table);
        // No job metadata columns.
        builder.setResponseCode(0);
        return builder.build();
    }

    private JobMetadataHook prepareInvalidJobMetadataResponseCode(int responseCode) {
        JobMetadataHook.Builder builder = JobMetadataHook.newBuilder();
        builder.setDelimiter(delimiter);
        builder.setSchemaTable(schema, table);
        builder.addJobMetadataHook(MetadataHook.newBuilder().setNum(1).setType("timestamp").setName("ts").Build());
        builder.addJobMetadataHook(MetadataHook.newBuilder().setNum(2).setType("int").setName("tag").Build());
        builder.addJobMetadataHook(MetadataHook.newBuilder().setNum(3).setType("float").setName("c1").Build());
        builder.addJobMetadataHook(MetadataHook.newBuilder().setNum(4).setType("double precision").setName("c2").Build());
        builder.addJobMetadataHook(MetadataHook.newBuilder().setNum(5).setType("text").setName("c3").Build());
        // No job metadata columns.
        builder.setResponseCode(responseCode);
        return builder.build();
    }

    private SendDataHook prepareSendDataNormal(int tupleCnt, Map<Long, String> errorLines) {
        SendDataHook.Builder builder = SendDataHook.newBuilder();
        if (errorLines != null) {
            builder.setErrorLines(errorLines);
        }
        builder.setTupleCount(tupleCnt);
        return builder.build();
    }

    private SendDataHook prepareSendDataFailedWithMsg(int code, String msg) {
        SendDataHook.Builder builder = SendDataHook.newBuilder();
        builder.setResponseCode(code);
        builder.setMessage(msg);
        return builder.build();
    }

    private SendDataHook prepareSendDataWithTimeCost(int tupleCnt, int t) {
        SendDataHook.Builder builder = SendDataHook.newBuilder();
        if (t > 0) {
            builder.setSleepMillis(t);
        }
        builder.setTupleCount(tupleCnt);
        return builder.build();
    }

    private SendDataHook prepareSendDataHookWithCompressDataDecoder(final String rawData, final CountDownLatch latch, final CompressDataDecoder decoder) {
        SendDataHook.Builder builder = SendDataHook.newBuilder();
        builder.setCompressDataDecoder(decoder);
        return builder.build();
    }

    private SendDataHook prepareSendDataGetException(int tupleCnt, String m) {
        SendDataHook.Builder builder = SendDataHook.newBuilder();
        if (m != null) {
            builder.setException(m);
        }
        builder.setTupleCount(tupleCnt);
        return builder.build();
    }

    private interface TestCase {
        void tcRun() throws InterruptedException;
    }

    private interface TestCaseCheck {
        void checkBoolean(boolean excepted);

        void checkJobMetadataWrapperOnSuccess(JobMetadataWrapper metadata);

        void checkMsgOnFailure(String errorMsg);
    }

    private Tuples prepareTuplesWithRawData(final String rawDataString, final int s) {
        return new Tuples() {

            private TuplesTarget target;

            private static final String delimiter = "|";
            private static final String schema = "public";
            private static final String table = "table";

            private final int size = s;
            private static final int csvBatchSize = 10;

            private Tuple generateTuple(String delimiter, String schema, String table) {
                return Utils.generateEmptyTupleLite(delimiter, schema, table);
            }

            private List<Tuple> generateTuplesList(int size, String delimiter, String schema, String table) {
                List<Tuple> tuplesList = new ArrayList<>(size);
                for (int i = 0; i < size; i++) {
                    Tuple tuple = generateTuple(delimiter, schema, table);
                    tuple.addColumn("c1", rawDataString);
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
