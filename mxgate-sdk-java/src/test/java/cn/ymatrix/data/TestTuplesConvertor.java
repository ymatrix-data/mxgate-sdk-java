package cn.ymatrix.data;

import cn.ymatrix.logger.MxLogger;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;

import java.util.*;

public class TestTuplesConvertor {

    private static final Logger l = MxLogger.init(TestTuplesConvertor.class);

    @Test
    public void TestConvertSucceedTuplesLines() {
        Assert.assertEquals(0, TuplesConsumeResultConvertor.convertSucceedTuplesLines(null, null));

        Tuples tuples = prepareTuples();
        int tupleSize = 5;
        for (int i = 0; i < tupleSize; i++) {
            tuples.append(prepareTuple());
        }
        Assert.assertEquals(tupleSize, TuplesConsumeResultConvertor.convertSucceedTuplesLines(null, tuples));

        Map<Long, String> errorLinesMap = new HashMap<>();
        for (int i = 0; i < tupleSize; i++) {
            errorLinesMap.put((long) i, String.valueOf(i));
        }
        Assert.assertEquals(0, TuplesConsumeResultConvertor.convertSucceedTuplesLines(errorLinesMap, tuples));

        int extraSize = 2;
        Map<Long, String> extraLinesMap = new HashMap<>();
        for (int i = 0; i < extraSize; i++) {
            extraLinesMap.put((long) i, String.valueOf(i));
        }
        Assert.assertEquals(tupleSize - extraSize, TuplesConsumeResultConvertor.convertSucceedTuplesLines(extraLinesMap, tuples));
    }

    @Test
    public void TestConvertSuccessfulSerialNums() {
        List<Long> serialNumList = TuplesConsumeResultConvertor.convertSuccessfulSerialNums(null, null);
        Assert.assertNotNull(serialNumList);
        Assert.assertEquals(0, serialNumList.size());

        Tuples tuples = prepareTuples();
        int tupleSize = 5;
        for (int i = 0; i < tupleSize; i++) {
            Tuple tuple = prepareTuple();
            tuple.setSerialNum(i);
            tuples.append(tuple);
        }
        List<Long> list2 = TuplesConsumeResultConvertor.convertSuccessfulSerialNums(null, tuples);
        Assert.assertEquals(tupleSize, list2.size());
        for (int i = 0; i < tupleSize; i++) {
            l.info("Tuple serial number : {}", tuples.getTupleByIndex(i).getSerialNum());
            Assert.assertEquals((long) i, tuples.getTupleByIndex(i).getSerialNum());
        }

        Map<Long, String> errorLinesMap = new HashMap<>();
        // Line number start from 1;
        for (int i = 1; i < tupleSize + 1; i++) {
            errorLinesMap.put((long) i, String.valueOf(i));
        }
        List<Long> list3 = TuplesConsumeResultConvertor.convertSuccessfulSerialNums(errorLinesMap, tuples);
        Assert.assertEquals(0, list3.size());

        int extraSize = 2;
        Map<Long, String> extraLinesMap = new HashMap<>();
        // Line number start from 1;
        for (int i = 1; i < extraSize + 1; i++) {
            extraLinesMap.put((long) i, String.valueOf(i));
        }
        List<Long> list4 = TuplesConsumeResultConvertor.convertSuccessfulSerialNums(extraLinesMap, tuples);
        Assert.assertEquals(3, list4.size());
        boolean equals2 = false;
        boolean equals3 = false;
        boolean equals4 = false;
        for (Long index : list4) {
            l.info("Tuple serial number last : {}", index);
            if (index == 2) {
                equals2 = true;
            }
            if (index == 3) {
                equals3 = true;
            }
            if (index == 4) {
                equals4 = true;
            }
        }
        Assert.assertTrue(equals2);
        Assert.assertTrue(equals3);
        Assert.assertTrue(equals4);
    }

    @Test
    public void TestConvertErrorTuples() {
        Map<Tuple, String> map1 = TuplesConsumeResultConvertor.convertErrorTuples(null, null, "msg_1");
        Assert.assertNotNull(map1);
        Assert.assertEquals(0, map1.size());

        Tuples tuples = prepareTuples();
        int tupleSize = 5;
        for (int i = 0; i < tupleSize; i++) {
            Tuple tuple = prepareTuple();
            tuple.setSerialNum(i);
            tuples.append(tuple);
        }
        Map<Tuple, String> map2 = TuplesConsumeResultConvertor.convertErrorTuples(null, tuples, "msg_2");
        Assert.assertNotNull(map2);
        Assert.assertEquals(5, map2.size());

        Map<Long, String> errorLinesMap = new HashMap<>();
        for (int i = 0; i < tupleSize; i++) {
            errorLinesMap.put((long) i, String.valueOf(i));
        }
        Map<Tuple, String> map3 = TuplesConsumeResultConvertor.convertErrorTuples(errorLinesMap, tuples, "msg_3");
        Assert.assertNotNull(map3);
        Assert.assertEquals(5, map3.size());

        Map<Long, String> errorLinesMap2 = new HashMap<>();
        for (int i = 1; i < 4; i++) {
            errorLinesMap2.put((long) i, String.valueOf(i));
        }
        Map<Tuple, String> map4 = TuplesConsumeResultConvertor.convertErrorTuples(errorLinesMap2, tuples, "msg_4");
        Assert.assertNotNull(map4);
        Assert.assertEquals(3, map4.size());

    }

    private Tuples prepareTuples() {
        return new Tuples() {
            private List<Tuple> tuplesContainer = new ArrayList<>();

            private static final int csvBatchSize = 10;

            @Override
            public void append(Tuple tuple) {
                tuplesContainer.add(tuple);
            }

            @Override
            public void appendTuples(Tuple... tuples) {
                tuplesContainer.addAll(Arrays.asList(tuples));
            }

            @Override
            public void appendTupleList(List<Tuple> tupleList) {
                tuplesContainer.addAll(tupleList);
            }

            @Override
            public List<Tuple> getTuplesList() {
                return null;
            }

            @Override
            public int size() {
                return tuplesContainer.size();
            }

            @Override
            public void setSchema(String schema) {

            }

            @Override
            public void setTable(String table) {

            }

            @Override
            public String getSchema() {
                return null;
            }

            @Override
            public String getTable() {
                return null;
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
                return tuplesContainer.get(index);
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
                return tuplesContainer.isEmpty();
            }

            @Override
            public void reset() {

            }

            @Override
            public void setDelimiter(String delimiter) {

            }

            @Override
            public String getDelimiter() {
                return null;
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

    private Tuple prepareTuple() {
        return new Tuple() {
            private long serialNum;

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
                return 0;
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
                return null;
            }

            @Override
            public void setSerialNum(long sn) {
                this.serialNum = sn;
            }

            @Override
            public long getSerialNum() {
                return this.serialNum;
            }

            @Override
            public void reset() {

            }
        };
    }

}
