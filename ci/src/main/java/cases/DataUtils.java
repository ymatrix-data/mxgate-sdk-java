package cases;

import Tools.PrintColor;
import Tools.mxgateHelper;
import cn.ymatrix.apiclient.DataPostListener;
import cn.ymatrix.apiclient.MxClient;
import cn.ymatrix.apiclient.Result;
import cn.ymatrix.data.Tuple;
import com.google.gson.JsonObject;
import org.junit.Assert;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public class DataUtils {
    private AtomicInteger finishFlag = new AtomicInteger(); // for wait data process(insert to DB) finish

    public void waitProcess() {
        // wait data to finish being processed
        do {
            // do nothing to wait sending
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
            }
        } while (finishFlag.get() != 0);

        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {

        }
    }

    // table: (time TIMESTAMP, tagid INT, c1 INT NOT NULL, c2 INT NOT NULL DEFAULT 0, c3 text) DISTRIBUTED BY (tagid)
    public void sendNormal(MxClient client, int sendInterval, int batchSize, int tupleCnt, int batchTupleCnt) {
        assertNotNull("mxclient should not be null", client);
        this.finishFlag.addAndGet(1);

        client.withIntervalToFlushMillis(sendInterval);
        client.withEnoughBytesToFlush(batchSize);

        client.registerDataPostListener(new DataPostListener() {
            @Override
            public void onSuccess(Result result) {

            }

            @Override
            public void onFailure(Result result) {
                System.out.printf("send tuples failed: msg= %s, success_lines= %d, failed_lines= ",
                        result.getMsg(), result.getSucceedLines(), result.getErrorTuplesMap());
                result.getErrorTuplesMap().entrySet().forEach(entry -> {
                    System.out.printf("%s: %s,", entry.getKey(), entry.getValue());
                });
                System.out.println();
            }
        });

        long ts = System.currentTimeMillis();
        int tagID = 0;

        Random rand = new Random();
        List<Tuple> tuples = new ArrayList<>();
        int sendCnt = 0;
        while (sendCnt < tupleCnt) {
            ts++;
            tagID++;

            Tuple tuple = client.generateEmptyTuple();
            tuple.addColumn("\"time\"", ts);
            tuple.addColumn("tagid", tagID);
            tuple.addColumn("c1", rand.nextInt());
            if (rand.nextBoolean()) {
                tuple.addColumn("c2", rand.nextInt());
            }
            tuple.addColumn("c3", this.generateRandomString(20));

            if (batchTupleCnt > 0) {
                tuples.add(tuple);
                if (tuples.size() >= batchTupleCnt) {
                    client.appendTuplesList(tuples);
                    tuples.clear();
                }
            } else {
                client.appendTuple(tuple);
            }
            sendCnt++;
        }
        if (tuples.size() > 0) {
            client.appendTuplesList(tuples);
            tuples.clear();
        }

        this.waitClientFree(client);
    }

    public void syncSendNormal(MxClient client, int lineCnt, int tupleCnt) {
        assertNotNull("mxclient should not be null", client);
        client.withEnoughLinesToFlush(lineCnt);

        client.registerDataPostListener(new DataPostListener() {
            @Override
            public void onSuccess(Result result) {

            }

            @Override
            public void onFailure(Result result) {
                System.out.printf("send tuples failed: msg= %s, success_lines= %d, failed_lines= ",
                        result.getMsg(), result.getSucceedLines(), result.getErrorTuplesMap());
                result.getErrorTuplesMap().entrySet().forEach(entry -> {
                    System.out.printf("%s: %s,", entry.getKey(), entry.getValue());
                });
                System.out.println();
            }
        });

        Random rand = new Random();
        long ts = System.currentTimeMillis();
        boolean catchException = false;
        int flushCnt = 0;
        for (int i = 0; i < tupleCnt; i++) {
            Tuple tuple = client.generateEmptyTuple();
            tuple.addColumn("\"time\"", ts);
            tuple.addColumn("tagid", i);
            tuple.addColumn("c1", rand.nextInt());
            if (rand.nextBoolean()) {
                tuple.addColumn("c2", rand.nextInt());
            } else {
                tuple.addColumn("c2", i);
            }
            String randC3 =  this.generateRandomString(20);
            if (randC3 == null || randC3.isEmpty()) {
                tuple.addColumn("c3", "c3" + i);
            }
            if (client.appendTupleBlocking(tuple)) {
                try {
                    client.flushBlocking();
                    flushCnt++;
                } catch (Exception e) {
                    catchException = true;
                    PrintColor.RedPrint(String.format("[ERROR] catch exception when flushing: %s\n", e.getMessage()));
                    e.printStackTrace();
                    break;
                }
            }
        }
        try {
            client.flushBlocking();
        } catch (Exception e) {
            // do nothing
        }

        assertFalse("should not catch exception, please check log above", catchException);
        assertTrue(flushCnt > 0);
    }

    // multiple type of column: (time TIMESTAMP, tagid SERIAL, c1 BOOLEAN, c2 CHAR(5), c3 NUMERIC(15,5), c4 JSON, c5 JSONB, c6 TIME WITH TIME ZONE, c7 UUID) DISTRIBUTED BY (tagid)
    public void sendMultipleColumnType(MxClient client, int sendInterval, int batchSize, int tupleCnt) {
        assertNotNull("mxclient should not be null", client);
        this.finishFlag.addAndGet(1);

        client.withIntervalToFlushMillis(sendInterval);
        client.withEnoughBytesToFlush(batchSize);

        client.registerDataPostListener(new DataPostListener() {
            @Override
            public void onSuccess(Result result) {

            }

            @Override
            public void onFailure(Result result) {
                System.out.printf("send tuples failed: msg= %s, success_lines= %d, failed_lines= ",
                        result.getMsg(), result.getSucceedLines(), result.getErrorTuplesMap());
                result.getErrorTuplesMap().entrySet().forEach(entry -> {
                    System.out.printf("%s: %s,", entry.getKey(), entry.getValue());
                });
                System.out.println();
            }
        });

        Random rand = new Random();
        long ts = System.currentTimeMillis();
        for (int i = 0; i < tupleCnt; i++) {
            ts++;

            Tuple tuple = client.generateEmptyTuple();

            tuple.addColumn("\"time\"", ts);
            tuple.addColumn("c1", rand.nextBoolean());
            tuple.addColumn("c2", generateRandomString(5));
            tuple.addColumn("c3", new BigDecimal((rand.nextInt()) + rand.nextDouble())
                    .setScale(5, RoundingMode.HALF_UP).doubleValue());

            String rJ = randomJSON().toString();
            tuple.addColumn("c4", rJ);
            tuple.addColumn("c5", rJ);

            tuple.addColumn("c6", new SimpleDateFormat("HH:mm:ss z").format(new Date(ts)));
            tuple.addColumn("c7", UUID.randomUUID().toString());

            client.appendTuple(tuple);
        }

        this.waitClientFree(client);
    }

    // multiple type of column: (time TIMESTAMP, tagid SERIAL, c1 BOOLEAN, c2 CHAR(5), c3 NUMERIC(15,5), c4 JSON, c5 JSONB, c6 TIME WITH TIME ZONE, c7 UUID) DISTRIBUTED BY (tagid)
    public void sendMultipleColumnTypeEmpty(MxClient client, int sendInterval, int batchSize, int tupleCnt) {
        assertNotNull("mxclient should not be null", client);
        this.finishFlag.addAndGet(1);

        client.withIntervalToFlushMillis(sendInterval);
        client.withEnoughBytesToFlush(batchSize);

        client.registerDataPostListener(new DataPostListener() {
            @Override
            public void onSuccess(Result result) {

            }

            @Override
            public void onFailure(Result result) {
                System.out.printf("send tuples failed: msg= %s, success_lines= %d, failed_lines= ",
                        result.getMsg(), result.getSucceedLines(), result.getErrorTuplesMap());
                result.getErrorTuplesMap().entrySet().forEach(entry -> {
                    System.out.printf("%s: %s,", entry.getKey(), entry.getValue());
                });
                System.out.println();
            }
        });

        long ts = System.currentTimeMillis();
        for (int i = 0; i < tupleCnt; i++) {
            ts++;

            Tuple tuple = client.generateEmptyTuple();
            tuple.addColumn("\"time\"", ts);
            client.appendTuple(tuple);
        }

        this.waitClientFree(client);
    }

    // various encoded string: (time TIMESTAMP, tagid int, c1 text, c2 char(1024)) DISTRIBUTED BY (tagid)
    public void sendString(MxClient client, int sendInterval, int batchSize, int tagid, String value) {
        assertNotNull("mxclient should not be null", client);
        this.finishFlag.addAndGet(1);

        client.withIntervalToFlushMillis(sendInterval);
        client.withEnoughBytesToFlush(batchSize);

        client.registerDataPostListener(new DataPostListener() {
            @Override
            public void onSuccess(Result result) {

            }

            @Override
            public void onFailure(Result result) {
                System.out.printf("send tuples failed: msg= %s, success_lines= %d, failed_lines= ",
                        result.getMsg(), result.getSucceedLines(), result.getErrorTuplesMap());
                result.getErrorTuplesMap().entrySet().forEach(entry -> {
                    System.out.printf("%s: %s,", entry.getKey(), entry.getValue());
                });
                System.out.println();
            }
        });

        Tuple tuple = client.generateEmptyTuple();
        tuple.addColumn("\"time\"", System.currentTimeMillis());
        tuple.addColumn("tagid", tagid);
        tuple.addColumn("c1", value);
        tuple.addColumn("c2", value);
        client.appendTuple(tuple);

        this.waitClientFree(client);
    }

    // table: (time TIMESTAMP, tagid INT, c1 INT NOT NULL, c2 INT NOT NULL DEFAULT 0, c3 text) DISTRIBUTED BY (tagid)
    public void sendFailedWrongType1(MxClient client) {
        assertNotNull("mxclient should not be null", client);
        this.finishFlag.addAndGet(1);

        client.withIntervalToFlushMillis(100);
        client.withEnoughBytesToFlush(100);

        client.registerDataPostListener(new DataPostListener() {
            @Override
            public void onSuccess(Result result) {
                System.out.printf("should not success: %s\n", result.getSucceedLines());
            }

            @Override
            public void onFailure(Result result) {
            }
        });

        long ts = System.currentTimeMillis();
        int tagID = 0;
        for (int i = 0; i < 5; i++) {
            ts++;
            tagID++;

            Tuple tuple = client.generateEmptyTuple();
            tuple.addColumn("\"time\"", ts);
            tuple.addColumn("tagid", tagID);
            tuple.addColumn("c1", "wuuuuuuuu");
            tuple.addColumn("c2", this.generateRandomString(3));
            tuple.addColumn("c3", this.generateRandomString(20));

            client.appendTuple(tuple);
        }

        this.waitClientFree(client);
    }

    // table: (time TIMESTAMP, tagid INT, c1 INT NOT NULL, c2 INT NOT NULL DEFAULT 0, c3 text) DISTRIBUTED BY (tagid)
    public void sendFailedOutOfRange1(MxClient client) {
        assertNotNull("mxclient should not be null", client);
        this.finishFlag.addAndGet(1);

        client.withIntervalToFlushMillis(100);
        client.withEnoughBytesToFlush(100);

        client.registerDataPostListener(new DataPostListener() {
            @Override
            public void onSuccess(Result result) {
                System.out.printf("should not success: %s\n", result.getSucceedLines());
            }

            @Override
            public void onFailure(Result result) {
            }
        });

        Random rand = new Random();
        long ts = System.currentTimeMillis();
        int tagID = 0;
        for (int i = 0; i < 5; i++) {
            ts++;
            tagID++;

            Tuple tuple = client.generateEmptyTuple();
            tuple.addColumn("\"time\"", ts);
            tuple.addColumn("tagid", tagID);
            tuple.addColumn("c1", "99999999999999999999999999999");
            tuple.addColumn("c2", rand.nextInt());
            tuple.addColumn("c3", this.generateRandomString(20));

            client.appendTuple(tuple);
        }

        this.waitClientFree(client);

    }

    // table: (time TIMESTAMP, tagid INT, c1 INT NOT NULL, c2 INT NOT NULL DEFAULT 0, c3 text) DISTRIBUTED BY (tagid)
    public void sendFailedShouldNotNull1(MxClient client) {
        assertNotNull("mxclient should not be null", client);
        this.finishFlag.addAndGet(1);

        client.withIntervalToFlushMillis(100);
        client.withEnoughBytesToFlush(100);

        client.registerDataPostListener(new DataPostListener() {
            @Override
            public void onSuccess(Result result) {
                System.out.printf("should not success: %s\n", result.getSucceedLines());
            }

            @Override
            public void onFailure(Result result) {
            }
        });

        Random rand = new Random();
        long ts = System.currentTimeMillis();
        int tagID = 0;
        for (int i = 0; i < 5; i++) {
            ts++;
            tagID++;

            Tuple tuple = client.generateEmptyTuple();
            tuple.addColumn("\"time\"", ts);
            tuple.addColumn("tagid", tagID);
            tuple.addColumn("c2", rand.nextInt());
            tuple.addColumn("c3", this.generateRandomString(20));

            client.appendTuple(tuple);
        }

        this.waitClientFree(client);
    }

    // table: (time TIMESTAMP, tagid INT, c1 INT NOT NULL, c2 INT NOT NULL DEFAULT 0, c3 text) DISTRIBUTED BY (tagid)
    public void sendFailedColumnNotFound1(MxClient client) {
        assertNotNull("mxclient should not be null", client);
        this.finishFlag.addAndGet(1);

        client.withIntervalToFlushMillis(50);
        client.withEnoughBytesToFlush(500);

        client.registerDataPostListener(new DataPostListener() {
            @Override
            public void onSuccess(Result result) {
                System.out.printf("should not success: %s\n", result.getSucceedLines());
            }

            @Override
            public void onFailure(Result result) {
            }
        });

        Random rand = new Random();
        long ts = System.currentTimeMillis();
        int tagID = 0;
        for (int i = 0; i < 5; i++) {
            ts++;
            tagID++;

            Tuple tuple = client.generateEmptyTuple();
            tuple.addColumn("\"time\"", ts);
            tuple.addColumn("tagid", tagID);
            tuple.addColumn("c1", rand.nextInt());
            tuple.addColumn("c2", rand.nextInt());
            tuple.addColumn("c3", this.generateRandomString(20));
            try {
                tuple.addColumn("c4", this.generateRandomString(3));
            } catch (NullPointerException e) {
                // expected
                continue;
            }
            assertNotNull("should not succeed in adding column c4 which not in table meta", null);

            client.appendTuple(tuple);
        }

        this.waitClientFree(client);
    }

    // rate: 60%
    public void sendWithCircuitBreakerOnByFailure(MxClient client, String classPath, String methodName) throws IOException, AssertionError {
        this.finishFlag.addAndGet(1);

        client.registerDataPostListener(new DataPostListener() {
            @Override
            public void onSuccess(Result result) {

            }

            @Override
            public void onFailure(Result result) {
                System.out.printf("send tuples failed: msg= %s, success_lines= %d, failed_lines= ",
                        result.getMsg(), result.getSucceedLines(), result.getErrorTuplesMap());
                result.getErrorTuplesMap().entrySet().forEach(entry -> {
                    System.out.printf("%s: %s,", entry.getKey(), entry.getValue());
                });
                System.out.println();
            }
        });
//        client.withIntervalToFlushMillis(100);
        client.withEnoughBytesToFlush(50);
        client.withEnoughLinesToFlush(2);

        Random rand = new Random();
        long ts = System.currentTimeMillis();
        int tagID = 0;

        Tuple tuple = client.generateEmptyTuple();
        tuple.addColumn("\"time\"", ts);
        tuple.addColumn("tagid", tagID);
        tuple.addColumn("c1", rand.nextInt());
        tuple.addColumn("c2", rand.nextInt());
        tuple.addColumn("c3", this.generateRandomString(20));
        client.appendTuple(tuple);
        // wait for flushing
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            // do nothing
        }

        ChaosBlade blade = new ChaosBlade();
        String bID = blade.makeSendDataException(classPath, methodName);

        tagID++;
        ts++;
        tuple = client.generateEmptyTuple();
        tuple.addColumn("\"time\"", ts);
        tuple.addColumn("tagid", tagID);
        tuple.addColumn("c1", rand.nextInt());
        tuple.addColumn("c2", rand.nextInt());
        tuple.addColumn("c3", this.generateRandomString(20));
        for (int i = 0; i < 5; i++) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                // do nothing
            }

            try {
                client.appendTuple(tuple);
            } catch (Exception e) {
                // expected, do nothing
                break;
            }
        }
        // wait for breaker to open
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            // do nothing
        }

        boolean catchException = false;
        try {
            client.appendTuple(tuple);
        } catch (Exception e) {
            catchException = true;
            System.out.printf("catch expected exception: %s\n", e.getMessage());
            if (!e.getMessage().contains("Circuit has broken")) {
                this.finishFlag.addAndGet(-1);
                throw new RuntimeException("unexpected exception: " + e.getMessage());
            }
        }
        if (!catchException) {
            this.finishFlag.addAndGet(-1);
            throw new RuntimeException("not catch exception from circuit breaker");
        }

        blade.revoke(bID);

        // wait for circuit breaker closed
        try {
            Thread.sleep(40000);
        } catch (InterruptedException e) {
            // do nothing
        }
        client.appendTuple(tuple);

        blade.destroy();
        this.waitClientFree(client);
    }

    // rate: 60%
    public void sendWithCircuitBreakerOnByMxGateDown(mxgateHelper mHelper, String dbName, String cmdCfg,
                                                     MxClient client) throws RuntimeException {
        this.finishFlag.addAndGet(1);

        client.registerDataPostListener(new DataPostListener() {
            @Override
            public void onSuccess(Result result) {

            }

            @Override
            public void onFailure(Result result) {
                System.out.printf("send tuples failed: msg= %s, success_lines= %d, failed_lines= ",
                        result.getMsg(), result.getSucceedLines(), result.getErrorTuplesMap());
                result.getErrorTuplesMap().entrySet().forEach(entry -> {
                    System.out.printf("%s: %s,", entry.getKey(), entry.getValue());
                });
                System.out.println();
            }
        });
//        client.withIntervalToFlushMillis(100);
        client.withEnoughBytesToFlush(50);
        client.withEnoughLinesToFlush(2);

        Random rand = new Random();
        long ts = System.currentTimeMillis();
        int tagID = 0;

        Tuple tuple = client.generateEmptyTuple();
        tuple.addColumn("\"time\"", ts);
        tuple.addColumn("tagid", tagID);
        tuple.addColumn("c1", rand.nextInt());
        tuple.addColumn("c2", rand.nextInt());
        tuple.addColumn("c3", this.generateRandomString(20));
        client.appendTuple(tuple);
        // wait for flushing
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            // do nothing
        }

        try {
            mHelper.foreStop();
        } catch (Exception e) {
            System.out.printf("catch unexpected exception: %s\n", e.getMessage());
            e.printStackTrace();
            throw new RuntimeException("unexpected exception when stop mxgate: " + e.getMessage());
        }

        tagID++;
        ts++;
        tuple = client.generateEmptyTuple();
        tuple.addColumn("\"time\"", ts);
        tuple.addColumn("tagid", tagID);
        tuple.addColumn("c1", rand.nextInt());
        tuple.addColumn("c2", rand.nextInt());
        tuple.addColumn("c3", this.generateRandomString(20));
        for (int i = 0; i < 5; i++) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                // do nothing
            }

            try {
                client.appendTuple(tuple);
            } catch (Exception e) {
                // expected, do nothing
                break;
            }
        }
        // wait for breaker to open
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            // do nothing
        }

        boolean catchException = false;
        try {
            client.appendTuple(tuple);
        } catch (Exception e) {
            catchException = true;
            System.out.printf("catch expected exception: %s\n", e.getMessage());
            if (!e.getMessage().contains("Circuit has broken")) {
                this.finishFlag.addAndGet(-1);
                throw new RuntimeException("unexpected exception: " + e.getMessage());
            }
        }
        if (!catchException) {
            this.finishFlag.addAndGet(-1);
            throw new RuntimeException("not catch exception from circuit breaker");
        }

        try {
            mHelper.start(cmdCfg);
        } catch (Exception e) {
            throw new RuntimeException("failed to start mxgate", e);
        }

        // wait for circuit breaker closed
        try {
            Thread.sleep(40000);
        } catch (InterruptedException e) {
            // do nothing
        }
        client.appendTuple(tuple);

        // Force flush the tuples into the cache.
        client.flush();

        this.waitClientFree(client);
    }

    // rate: 60%
    public void sendWithCircuitBreakerOnByDBDown(MxClient client, mxgateHelper mh) throws IOException {
        this.finishFlag.addAndGet(1);

        client.registerDataPostListener(new DataPostListener() {
            @Override
            public void onSuccess(Result result) {

            }

            @Override
            public void onFailure(Result result) {
                System.out.printf("send tuples failed: msg= %s, success_lines= %d, failed_lines= ",
                        result.getMsg(), result.getSucceedLines(), result.getErrorTuplesMap());
                result.getErrorTuplesMap().entrySet().forEach(entry -> {
                    System.out.printf("%s: %s,", entry.getKey(), entry.getValue());
                });
                System.out.println();
            }
        });
//        client.withIntervalToFlushMillis(100);
        client.withEnoughBytesToFlush(50);
        client.withEnoughLinesToFlush(2);

        Random rand = new Random();
        long ts = System.currentTimeMillis();
        int tagID = 0;

        Tuple tuple = client.generateEmptyTuple();
        tuple.addColumn("\"time\"", ts);
        tuple.addColumn("tagid", tagID);
        tuple.addColumn("c1", rand.nextInt());
        tuple.addColumn("c2", rand.nextInt());
        tuple.addColumn("c3", this.generateRandomString(20));
        client.appendTuple(tuple);
        // wait for flushing
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            // do nothing
        }
        DBHelper dbHelper = new DBHelper(mh);
        dbHelper.shutDown();

        tagID++;
        ts++;
        tuple = client.generateEmptyTuple();
        tuple.addColumn("\"time\"", ts);
        tuple.addColumn("tagid", tagID);
        tuple.addColumn("c1", rand.nextInt());
        tuple.addColumn("c2", rand.nextInt());
        tuple.addColumn("c3", this.generateRandomString(20));
        for (int i = 0; i < 10; i++) {
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                // do nothing
            }

            try {
                client.appendTuple(tuple);
            } catch (Exception e) {
                // expected, do nothing
                break;
            }
        }
        // wait for breaker to open
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            // do nothing
        }

        boolean catchException = false;
        try {
            client.appendTuple(tuple);
        } catch (Exception e) {
            catchException = true;
            System.out.printf("catch expected exception: %s\n", e.getMessage());
            if (!e.getMessage().contains("Circuit has broken")) {
                this.finishFlag.addAndGet(-1);
                dbHelper.launch();
                throw new RuntimeException("unexpected exception: " + e.getMessage());
            }
        }
        if (!catchException) {
            this.finishFlag.addAndGet(-1);
            dbHelper.launch();
            throw new RuntimeException("not catch exception from circuit breaker");
        }

        dbHelper.launch();
        // wait for circuit breaker closed
        try {
            Thread.sleep(40000);
        } catch (InterruptedException e) {
            // do nothing
        }
        client.appendTuple(tuple);

        this.waitClientFree(client);
        dbHelper.close();
    }

    // rate: 80%
    public void sendWithCircuitBreakerOnBySlowCall(MxClient client, String classPath, String methodName, long processTimeMillis) throws IOException {
        this.finishFlag.addAndGet(1);

        client.registerDataPostListener(new DataPostListener() {
            @Override
            public void onSuccess(Result result) {

            }

            @Override
            public void onFailure(Result result) {
                System.out.printf("send tuples failed: msg= %s, success_lines= %d, failed_lines= ",
                        result.getMsg(), result.getSucceedLines(), result.getErrorTuplesMap());
                result.getErrorTuplesMap().entrySet().forEach(entry -> {
                    System.out.printf("%s: %s,", entry.getKey(), entry.getValue());
                });
                System.out.println();
            }
        });
        client.withIntervalToFlushMillis(10);
        client.withEnoughBytesToFlush(50);

        Random rand = new Random();
        long ts = System.currentTimeMillis();
        int tagID = 0;

        Tuple tuple = client.generateEmptyTuple();
        tuple.addColumn("\"time\"", ts);
        tuple.addColumn("tagid", tagID);
        tuple.addColumn("c1", rand.nextInt());
        tuple.addColumn("c2", rand.nextInt());
        tuple.addColumn("c3", this.generateRandomString(20));
        client.appendTuple(tuple);
        client.flush(); // Force flush
        // wait for flushing
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            // do nothing
        }

        ChaosBlade blade = new ChaosBlade();
        String bID = blade.makeSendDataSlow(classPath, methodName, processTimeMillis);

        for (int i = 0; i < 5; i++) {
            tagID++;
            ts++;
            tuple = client.generateEmptyTuple();
            tuple.addColumn("\"time\"", ts);
            tuple.addColumn("tagid", tagID);
            tuple.addColumn("c1", rand.nextInt());
            tuple.addColumn("c2", rand.nextInt());
            tuple.addColumn("c3", this.generateRandomString(20));
            try {
                client.appendTuple(tuple);
                client.flush();
            } catch (Exception e) {
                // expected, do nothing
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                // do nothing
            }
        }
        // wait for breaker to open
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            // do nothing
        }

        boolean catchException = false;
        try {
            client.appendTuple(tuple);
            client.flush();
        } catch (Exception e) {
            catchException = true;
            System.out.printf("catch expected exception: %s\n", e.getMessage());
            if (!e.getMessage().contains("Circuit has broken")) {
                this.finishFlag.addAndGet(-1);
                throw new RuntimeException("unexpected exception: " + e.getMessage());
            }
        }
        if (!catchException) {
            this.finishFlag.addAndGet(-1);
            throw new RuntimeException("not catch exception from circuit breaker");
        }

        blade.revoke(bID);

        // wait for circuit breaker closed
        try {
            Thread.sleep(40000);
        } catch (InterruptedException e) {
            // do nothing
        }
        client.appendTuple(tuple);
        client.flush();

        blade.destroy();
        this.waitClientFree(client);
    }

    private void waitClientFree(MxClient client) {
        try {
            Method isFreeMethod = client.getClass().getDeclaredMethod("isFree");
            isFreeMethod.setAccessible(true);

            do {
                Thread.sleep(500);
            } while (!(boolean) isFreeMethod.invoke(client));
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        } catch (InvocationTargetException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        this.finishFlag.addAndGet(-1);
    }

    public String generateRandomString(int maxLength) {
        int leftLimit = 48; // numeral '0'
        int rightLimit = 122; // letter 'z'

        Random rand = new Random();
        int aimLength = rand.nextInt(maxLength);
        if (aimLength <= 0) {
            return "";
        }

        return rand.ints(leftLimit, rightLimit + 1)
                .filter(i -> (i <= 57 || i >= 65) && (i <= 90 || i >= 97))
                .limit(aimLength)
                .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                .toString();
    }

    private JsonObject randomJSON() {
        /*{
            "info1": "xx"
            "info2": "xxx"
            "info3": {
                "sub_info1": xx,
                "sub_info2": xx,
            }
        }*/
        Random rand = new Random();

        JsonObject jsonContainer = new JsonObject();
        jsonContainer.addProperty("info1", generateRandomString(2));
        jsonContainer.addProperty("info2", generateRandomString(3));

        JsonObject inner = new JsonObject();
        inner.addProperty("sub_info1", rand.nextInt());
        inner.addProperty("sub_info2", rand.nextInt());
        jsonContainer.add("info3", inner);

        return jsonContainer;
    }

    private Object getTuplesImplInstance() throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        String className = "cn.ymatrix.builder.MxClientImpl$TuplesImpl";
        Class<?> innerClazz = Class.forName(className);
        Constructor<?> constructor = innerClazz.getDeclaredConstructor(String.class, String.class);
        constructor.setAccessible(true);
        Object o = constructor.newInstance("public", "test_table");
        Assert.assertEquals(className, o.getClass().getName());
        return o;
    }
}

