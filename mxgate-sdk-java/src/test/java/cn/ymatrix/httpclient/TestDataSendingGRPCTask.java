package cn.ymatrix.httpclient;

import cn.ymatrix.api.*;
import cn.ymatrix.apiserver.SendDataListener;
import cn.ymatrix.apiserver.SendDataResult;
import cn.ymatrix.builder.Utils;
import cn.ymatrix.data.Tuple;
import cn.ymatrix.data.Tuples;
import cn.ymatrix.data.TuplesTarget;
import cn.ymatrix.exception.BrokenTuplesException;
import cn.ymatrix.exception.RetryException;
import cn.ymatrix.faulttolerance.RetryConfiguration;
import cn.ymatrix.faulttolerance.RetryStatistic;
import cn.ymatrix.logger.MxLogger;
import cn.ymatrix.utils.CSVConstructor;
import cn.ymatrix.utils.StrUtil;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;

import java.lang.reflect.Field;
import java.rmi.UnexpectedException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class TestDataSendingGRPCTask {
    private static final Logger l = MxLogger.init(TestHttpTask.class);
    private MxServerBackend gRPCServer;
    private static final CSVConstructor constructor = new CSVConstructor(10);


    @Test
    public void TestCanRetry() {
        String m = "mxgate server not ready";

        int port = 18005;
        int tupleCnt = 5;
        String schema = "public";
        String table = "test";

        Tuples tuples = prepareTuples("TestCanRetry", tupleCnt);
        TuplesTarget target = new TuplesTarget();
        target.setURL(String.format("localhost:%d", port));
        tuples.setTarget(target);
        tuples.setSchema(schema);
        tuples.setTable(table);

        startGRPCClientServer(port, insertFailedWithMsg(MxGrpcClient.GRPC_RESPONSE_CODE_MXGATE_NOT_READY, m));
        DataSendingGRPCTask grpcTask = new DataSendingGRPCTask(tuples, null, constructor) {
            @Override
            public void run() {

            }
        };

        try {
            // With no RetryConfiguration, canRetry() will be false.
            Assert.assertFalse(grpcTask.canRetry());

            RetryConfiguration rc = new RetryConfiguration(3, 3000, RetryException.class);
            grpcTask.withRetry(rc);

            Assert.assertTrue(grpcTask.canRetry());

            Field rsField = DataSendingGRPCTask.class.getDeclaredField("rs");
            Assert.assertNotNull(rsField);
            rsField.setAccessible(true);

            RetryStatistic rs = (RetryStatistic) rsField.get(grpcTask);
            Assert.assertNotNull(rs);
            Assert.assertEquals(rs.actuallyRetryTimes(), 0);
            Assert.assertFalse(rs.exceedMaxRetryTimes());

            rs.increaseRetryTimes();
            Assert.assertTrue(grpcTask.canRetry());
            Assert.assertEquals(rs.actuallyRetryTimes(), 1);
            Assert.assertFalse(rs.exceedMaxRetryTimes());

            rs.increaseRetryTimes();
            Assert.assertTrue(grpcTask.canRetry());
            Assert.assertEquals(rs.actuallyRetryTimes(), 2);
            Assert.assertFalse(rs.exceedMaxRetryTimes());

            rs.increaseRetryTimes();
            Assert.assertFalse(grpcTask.canRetry());
            Assert.assertEquals(rs.actuallyRetryTimes(), 3);
            Assert.assertTrue(rs.exceedMaxRetryTimes());


        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    @Test(expected = NullPointerException.class)
    public void TestNewInstanceNullableTuples() {
        new DataSendingGRPCTask(null, null, constructor) {
            @Override
            public void run() {

            }
        };
    }

    @Test(expected = NullPointerException.class)
    public void TestNewInstanceNullableTuplesTarget() {
        Tuples tuples = prepareTuples("TestNewInstanceNullableTuplesTarget", 5);
        tuples.setTarget(null);
        new DataSendingGRPCTask(tuples, null, constructor) {
            @Override
            public void run() {

            }
        };
    }

    @Test(expected = NullPointerException.class)
    public void TestNewInstanceNullableTuplesTargetURL() {
        Tuples tuples = prepareTuples("TestNewInstanceNullableTuplesTargetURL", 5);
        TuplesTarget target = new TuplesTarget();
        target.setURL(null);
        tuples.setTarget(target);
        new DataSendingGRPCTask(tuples, null, constructor) {
            @Override
            public void run() {

            }
        };
    }

    @Test(expected = NullPointerException.class)
    public void TestNewInstanceEmptyTuplesTargetURL() {
        Tuples tuples = prepareTuples("TestNewInstanceEmptyTuplesTargetURL", 5);
        TuplesTarget target = new TuplesTarget();
        target.setURL("");
        tuples.setTarget(target);
        new DataSendingGRPCTask(tuples, null, constructor) {
            @Override
            public void run() {

            }
        };
    }

    @Test
    public void TestSendTuplesAllSuccess() {
        normalTestFramework(19000, null);
    }

    @Test
    public void TestSendTuplesPartialFailed() {
        Map<Long, String> errorLines = new HashMap<>();
        errorLines.put(new Long(3), "bulabulabula");
        errorLines.put(new Long(8), "bulabulabula again");

        normalTestFramework(19001, errorLines);
    }

    private void normalTestFramework(int port, Map<Long, String> errorLines) {
        int tupleCnt = 10;
        String schema = "public";
        String table = "table";

        Tuples tuples = prepareTuples("normalTestFramework", tupleCnt);
        TuplesTarget target = new TuplesTarget();
        target.setURL(String.format("localhost:%d", port));
        tuples.setTarget(target);
        tuples.setSchema(schema);
        tuples.setTable(table);

        startGRPCClientServer(port, insertNormal(tupleCnt, errorLines));
        DataSendingGRPCTask task = new DataSendingGRPCTask(tuples, null, constructor) {
            @Override
            public void run() {

            }
        };

        final SendDataResult[] callbackResult = {null};
        final CountDownLatch latch = new CountDownLatch(1);
        task.registerListener(new SendDataListener() {
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
        });
        SendDataResult result = null;
        try {
            result = task.sendTuplesBlocking(tuples);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        Assert.assertNotNull(result);

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
            expectMsg = StrUtil.connect("Send data to server with ", String.valueOf(errorLines.size()), " error lines from gRPC response of table", schema, ".", table);
        }
        Assert.assertEquals(expectMsg, result.getMsg());
        Assert.assertEquals(expectMsg, callbackResult[0].getMsg());
    }

    @Test
    public void TestSendTuplesInvalidRequest() {
        String mHeader = "Invalid request of send data gRPC API ";
        String m = "schema required";
        failTestFramework(19002, mHeader, m, insertFailedWithMsg(MxGrpcClient.GRPC_RESPONSE_CODE_INVALID_REQUEST, m), StatusCode.ERROR);
    }

    @Test
    public void TestSendTuplesInvalidConfig() {
        String mHeader = "Invalid request of send data gRPC API ";
        String m = "empty delimiter";
        failTestFramework(19003, mHeader, m, insertFailedWithMsg(MxGrpcClient.GRPC_RESPONSE_CODE_INVALID_CONFIG, m), StatusCode.ERROR);
    }

    @Test
    public void TestSendTuplesInvalidTable() {
        String mHeader = "Invalid request of send data gRPC API ";
        String m = "job not found";
        failTestFramework(19004, mHeader, m, insertFailedWithMsg(MxGrpcClient.GRPC_RESPONSE_CODE_INVALID_TABLE, m), StatusCode.ERROR);
    }

    @Test
    public void TestSendTuplesMxgateNotReady() {
        String m = "mxgate server not ready";

        int port = 19005;
        int tupleCnt = 5;
        String schema = "public";
        String table = "test";

        Tuples tuples = prepareTuples("TestSendTuplesMxgateNotReady", tupleCnt);
        TuplesTarget target = new TuplesTarget();
        target.setURL(String.format("localhost:%d", port));
        tuples.setTarget(target);
        tuples.setSchema(schema);
        tuples.setTable(table);

        startGRPCClientServer(port, insertFailedWithMsg(MxGrpcClient.GRPC_RESPONSE_CODE_MXGATE_NOT_READY, m));
        DataSendingGRPCTask task = new DataSendingGRPCTask(tuples, null, constructor) {
            @Override
            public void run() {

            }
        };
        SendDataResult result = null;
        try {
            result = task.sendTuplesBlocking(tuples);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        Assert.assertNull(result);
    }

    @Test(expected = RetryException.class)
    public void TestSendTuplesTimeout() {
        String m = "whoops...";
        int port = 19006;

        int tupleCnt = 5;
        String schema = "public";
        String table = "test";

        Tuples tuples = prepareTuples("TestSendTuplesTimeout", tupleCnt);
        TuplesTarget target = new TuplesTarget();
        target.setURL(String.format("localhost:%d", port));
        tuples.setTarget(target);
        tuples.setSchema(schema);
        tuples.setTable(table);

        startGRPCClientServer(port, insertFailedWithMsg(MxGrpcClient.GRPC_RESPONSE_CODE_TIMEOUT, m));
        DataSendingGRPCTask task = new DataSendingGRPCTask(tuples, null, constructor) {
            @Override
            public void run() {

            }
        };
        task.sendTuplesBlocking(tuples);
    }

    @Test(expected = RetryException.class)
    public void TestSendTuplesAllFailed() {
        String m = "whoops...";
        int port = 19007;

        int tupleCnt = 5;
        String schema = "public";
        String table = "test";

        Tuples tuples = prepareTuples("TestSendTuplesAllFailed", tupleCnt);
        TuplesTarget target = new TuplesTarget();
        target.setURL(String.format("localhost:%d", port));
        tuples.setTarget(target);
        tuples.setSchema(schema);
        tuples.setTable(table);

        startGRPCClientServer(port, insertFailedWithMsg(MxGrpcClient.GRPC_RESPONSE_CODE_ALL_TUPLES_FAILED, m));
        DataSendingGRPCTask task = new DataSendingGRPCTask(tuples, null, constructor) {
            @Override
            public void run() {

            }
        };
        task.sendTuplesBlocking(tuples);
    }

    @Test
    public void TestSendTuplesUnDefinedError() {
        String mHeader = "Get error from send data gRPC API ";
        String m = "whoops...";
        failTestFramework(19008, mHeader, m, insertFailedWithMsg(MxGrpcClient.GRPC_RESPONSE_CODE_ALL_UNDEFINED_ERROR, m), StatusCode.ALL_TUPLES_FAIL);
    }

    private void failTestFramework(int port, String errMsgHeader, String errMsg, SendDataHook hook, StatusCode expectCode) {
        int tupleCnt = 5;
        String schema = "public";
        String table = "table";

        Tuples tuples = prepareTuples("failTestFramework", tupleCnt);
        TuplesTarget target = new TuplesTarget();
        target.setURL(String.format("localhost:%d", port));
        tuples.setTarget(target);
        tuples.setSchema(schema);
        tuples.setTable(table);

        startGRPCClientServer(port, hook);
        DataSendingGRPCTask task = new DataSendingGRPCTask(tuples, null, constructor) {
            @Override
            public void run() {

            }
        };
        final SendDataResult[] callbackResult = {null};
        final CountDownLatch latch = new CountDownLatch(1);
        task.registerListener(new SendDataListener() {
            @Override
            public void onSuccess(SendDataResult result, Tuples tuples) {}

            @Override
            public void onFailure(SendDataResult result, Tuples tuples) {
                callbackResult[0] = result;
                latch.countDown();
            }
        });
        SendDataResult result = null;
        try {
            result = task.sendTuplesBlocking(tuples);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        Assert.assertNotNull(result);

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

    private Tuples prepareTuples(final String rawDataString, final int tupleSize) {
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

    private void startGRPCClientServer(int port, SendDataHook hook) {
        // mock server
        gRPCServer = MxServerBackend.getServerInstance(port, null, hook);
        l.info("new grpc server.");

        // run mock server
        new Thread() {
            @Override
            public void run() {
                gRPCServer.startBlocking();
            }
        }.start();

        // Sleep some seconds to wait for the server to start.
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            l.warn("Failed to wait server running: {}", e.getMessage());
        }
    }

    private SendDataHook insertNormal(int tupleCnt, Map<Long, String> errorLines) {
        SendDataHook.Builder builder = SendDataHook.newBuilder();
        if (errorLines != null) {
            builder.setErrorLines(errorLines);
        }
        builder.setTupleCount(tupleCnt);
        return builder.build();
    }

    private SendDataHook insertFailedWithMsg(int code, String msg) {
        SendDataHook.Builder builder = SendDataHook.newBuilder();
        builder.setResponseCode(code);
        builder.setMessage(msg);
        return builder.build();
    }

}
