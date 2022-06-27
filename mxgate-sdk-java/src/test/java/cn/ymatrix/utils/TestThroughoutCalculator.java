package cn.ymatrix.utils;

import cn.ymatrix.builder.Utils;
import cn.ymatrix.data.Tuple;
import cn.ymatrix.data.Tuples;
import cn.ymatrix.data.TuplesTarget;
import cn.ymatrix.logger.MxLogger;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class TestThroughoutCalculator {
    private static final Logger l = MxLogger.init(TestThroughoutCalculator.class);

    @Test
    public void TestCalculate() {
        // 4 bytes each tuples.
        ThroughoutCalculator calculator = ThroughoutCalculator.getInstance();

        try {
            Field tupleSize = calculator.getClass().getDeclaredField("tupleSize");
            Assert.assertNotNull(tupleSize);
            tupleSize.setAccessible(true);
            AtomicLong tupleSizeAtomic = (AtomicLong) tupleSize.get(calculator);

            Field bandWidth = calculator.getClass().getDeclaredField("bandwidth");
            Assert.assertNotNull(bandWidth);
            bandWidth.setAccessible(true);
            AtomicLong bandWidthAtomic = (AtomicLong) bandWidth.get(calculator);

            int tupleCnt = 25;
            int threads = 4;
            CountDownLatch latch = new CountDownLatch(4);
            // Use 4 threads to add Tuple into the Calculator.
            for (int i = 0; i < threads; i++) {
                Thread thread = new Thread() {
                    @Override
                    public void run() {
                        for (int i = 0; i < tupleCnt; i++) {
                            calculator.add(generateTuples());
                        }
                        latch.countDown();
                    }
                };
                thread.start();
            }
            Assert.assertTrue(latch.await(3000, TimeUnit.SECONDS));

            l.info("tuple size in calculator {}", tupleSizeAtomic.get());
            Assert.assertEquals(threads * tupleCnt * 4, tupleSizeAtomic.get());
            Assert.assertEquals(threads * tupleCnt * 80, bandWidthAtomic.get());

            Method printDiff = calculator.getClass().getDeclaredMethod("printDiff", long.class, long.class, long.class, long.class);
            Assert.assertNotNull(printDiff);
            printDiff.setAccessible(true);
            printDiff.invoke(calculator, 100, 400, 10000, 40000);

            Field tuplesPerSec = calculator.getClass().getDeclaredField("tuplesPerSec");
            Assert.assertNotNull(tuplesPerSec);
            tuplesPerSec.setAccessible(true);
            Field bytesPerSec = calculator.getClass().getDeclaredField("bytesPerSec");
            Assert.assertNotNull(bytesPerSec);
            bytesPerSec.setAccessible(true);
            Field MBPerSec = calculator.getClass().getDeclaredField("MBPerSec");
            Assert.assertNotNull(MBPerSec);
            MBPerSec.setAccessible(true);
            Field KBPerSec = calculator.getClass().getDeclaredField("KBPerSec");
            Assert.assertNotNull(KBPerSec);
            KBPerSec.setAccessible(true);

            calculator.stop();
        } catch (Exception e) {
            l.error("ThroughoutCalculator test reflection exception", e);
            throw new RuntimeException(e);
        }

    }


    /**
     * 4 bytes each Tuples.
     * @return Tuples instance.
     */
    private Tuples generateTuples() {
        return new Tuples() {
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

            }

            @Override
            public TuplesTarget getTarget() {
                return null;
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
