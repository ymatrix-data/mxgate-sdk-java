package cn.ymatrix.worker;

import cn.ymatrix.api.MxServerBackend;
import cn.ymatrix.api.SendDataHook;
import cn.ymatrix.apiserver.SendDataResult;
import cn.ymatrix.builder.RequestType;
import cn.ymatrix.builder.Utils;
import cn.ymatrix.data.Tuple;
import cn.ymatrix.data.Tuples;
import cn.ymatrix.data.TuplesTarget;
import cn.ymatrix.httpclient.MockHttpServer;
import cn.ymatrix.httpclient.SingletonHTTPClient;
import cn.ymatrix.logger.MxLogger;
import cn.ymatrix.utils.CSVConstructor;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class TestTuplesSendingBlockingWorker {
    private static final Logger l = MxLogger.init(TestTuplesSendingBlockingWorker.class);

    @Test
    public void TestSendTuplesSyncHTTP() {
        CountDownLatch latch = new CountDownLatch(1);
        Tuples tuples = getTuples();
        String url = "http://localhost:21001/";
        MockHttpServer.startServerBlocking(21001, new HttpHandler() {
            @Override
            public void handle(HttpExchange he) throws IOException {
                // parse request
                InputStreamReader isr = new InputStreamReader(he.getRequestBody(), StandardCharsets.UTF_8);
                BufferedReader br = new BufferedReader(isr);
                String table = br.readLine();
                String data = br.readLine();
                l.info("request content {}", data);
                Assert.assertEquals("\"0\"|\"0\"|\"0\"|\"0\"|\"0\"", data);
                latch.countDown();

                // send response
                String responseBody = "success";
                he.sendResponseHeaders(200, responseBody.length());
                OutputStream os = he.getResponseBody();
                os.write(responseBody.toString().getBytes());
                os.close();
            }
        });

        TuplesSendingBlockingWorker worker = new TuplesSendingBlockingWorker(RequestType.WithHTTP,
                SingletonHTTPClient.getInstance(10).getClient(), new CSVConstructor(10));

        TuplesTarget target = new TuplesTarget();
        target.setURL(url);
        tuples.setTarget(target);
        worker.sendTuplesBlocking(tuples);

        try {
            Assert.assertTrue(latch.await(3000, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void TestSendTuplesBlockingGRPC() {
        Tuples tuples = getTuples();
        int port = 21002;
        String url = String.format("localhost:%d", port);
        startGRPCClientServer(port, insertNormal(1, null));
        TuplesSendingBlockingWorker worker = new TuplesSendingBlockingWorker(RequestType.WithGRPC, null, new CSVConstructor(10));
        TuplesTarget target = new TuplesTarget();
        target.setURL(url);
        tuples.setTarget(target);
        tuples.setSchema("public");
        tuples.setTable("test_table");
        SendDataResult result = worker.sendTuplesBlocking(tuples);
        Assert.assertNotNull(result);
    }

    @Test
    public void TestSendTuplesBlockingHTTPWithNullResult() {
        TuplesSendingBlockingWorker worker = new TuplesSendingBlockingWorker(RequestType.WithHTTP,
                SingletonHTTPClient.getInstance(10).getClient(), new CSVConstructor(10));
        TuplesTarget target = new TuplesTarget();
        target.setURL("url");
        Tuples tuples = getTuples();
        tuples.setTarget(target);
        SendDataResult result = worker.sendTuplesBlocking(tuples);
        // With invalid url, the result will be null.
        Assert.assertNull(result);
    }

    @Test
    public void TestSendTuplesBlockingGRPCWithNullResult() {
        TuplesSendingBlockingWorker worker = new TuplesSendingBlockingWorker(RequestType.WithGRPC,
                SingletonHTTPClient.getInstance(10).getClient(), new CSVConstructor(10));
        TuplesTarget target = new TuplesTarget();
        target.setURL("url");
        Tuples tuples = getTuples();
        tuples.setTarget(target);
        SendDataResult result = worker.sendTuplesBlocking(tuples);
        // With invalid url, the result will be null.
        Assert.assertNull(result);
    }


    private void startGRPCClientServer(int port, SendDataHook hook) {
        // mock server
        final MxServerBackend gRPCServer = MxServerBackend.getServerInstance(port, null, hook);
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


    private Tuples getTuples() {
        return new Tuples() {

            private TuplesTarget target;

            private static final String delimiter = "|";
            private static final String schema = "public";
            private static final String table = "table";

            private static final int size = 4;

            private static final int csvBatchSize = 10;

            private Tuple generateTuple(String delimiter, String schema, String table) {
                return Utils.generateEmptyTupleLite(delimiter, schema, table);
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
