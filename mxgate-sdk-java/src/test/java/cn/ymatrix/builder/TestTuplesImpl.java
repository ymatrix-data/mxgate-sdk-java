package cn.ymatrix.builder;

import cn.ymatrix.api.ColumnMeta;
import cn.ymatrix.api.ColumnMetaWrapper;
import cn.ymatrix.data.Column;
import cn.ymatrix.data.Tuple;
import cn.ymatrix.logger.MxLogger;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestTuplesImpl {
    Logger l = MxLogger.init(TestTuplesImpl.class);
    @Test
    public void TestTuplesImplConstructor() {
        try {
            Object o = getTuplesImplInstance(10);
        } catch (ClassNotFoundException | NoSuchMethodException | InstantiationException | IllegalAccessException |
                 InvocationTargetException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
    @Test
    public void TestTuplesImplAppendTuple() {
        try {
            Object o = getTuplesImplInstance(10);
            Method tuplesSize = o.getClass().getDeclaredMethod("size");
            Assert.assertNotNull(tuplesSize);
            tuplesSize.setAccessible(true);
            // No tuples in the inner list.
            int tupleSize = (int) tuplesSize.invoke(o);
            Assert.assertEquals(tupleSize, 0);
            Method append = o.getClass().getDeclaredMethod("append", Tuple.class);
            append.setAccessible(true);
            int size = 5;
            append.invoke(o, prepareEmptyTuple(size));
            // One tuple has been added into the list.
            tupleSize = (int) tuplesSize.invoke(o);
            Assert.assertEquals(tupleSize, 1);
        } catch (ClassNotFoundException | NoSuchMethodException | InvocationTargetException | InstantiationException |
                 IllegalAccessException e) {
            l.error("TestTuplesImplAppendTuple error ", e);
            throw new RuntimeException(e);
        }
    }
    @Test
    public void TestTuplesImplAppendTuples() {
        try {
            Object o = getTuplesImplInstance(10);
            Method tuplesSize = o.getClass().getDeclaredMethod("size");
            Assert.assertNotNull(tuplesSize);
            tuplesSize.setAccessible(true);
            // No tuples in the inner list.
            int tupleSize = (int) tuplesSize.invoke(o);
            Assert.assertEquals(tupleSize, 0);
            Method appendTuples = o.getClass().getDeclaredMethod("appendTuples", Tuple[].class);
            appendTuples.setAccessible(true);
            int size = 5;
            Tuple[] tuples = new Tuple[] {prepareEmptyTuple(size), prepareEmptyTuple(size)};
            appendTuples.invoke(o, (Object) tuples);
            // Two tuple has been added into the list.
            tupleSize = (int) tuplesSize.invoke(o);
            Assert.assertEquals(tupleSize, 2);
        } catch (ClassNotFoundException | NoSuchMethodException | InvocationTargetException | InstantiationException |
                 IllegalAccessException e) {
            l.error("TestTuplesImplAppendTuples error ", e);
            throw new RuntimeException(e);
        }
    }
    @Test
    public void TestTupleImplAppendTuplesList() {
        try {
            Object o = getTuplesImplInstance(10);
            Method tuplesSize = o.getClass().getDeclaredMethod("size");
            Assert.assertNotNull(tuplesSize);
            tuplesSize.setAccessible(true);
            // No tuples in the inner list.
            int tupleSize = (int) tuplesSize.invoke(o);
            Assert.assertEquals(tupleSize, 0);
            Method appendTuples = o.getClass().getDeclaredMethod("appendTupleList", List.class);
            appendTuples.setAccessible(true);
            int size = 5;
            List<Tuple> list = new ArrayList<>();
            list.add(prepareEmptyTuple(size));
            list.add(prepareEmptyTuple(size));
            appendTuples.invoke(o, list);
            // Two tuple has been added into the list.
            tupleSize = (int) tuplesSize.invoke(o);
            Assert.assertEquals(tupleSize, 2);
        } catch (ClassNotFoundException | NoSuchMethodException | InvocationTargetException | InstantiationException |
                 IllegalAccessException e) {
            l.error("TestTuplesImplAppendTuplesList exception ", e);
            throw new RuntimeException(e);
        }
    }

    @Test
    public void TestTuplesImplReset() {
        try {
            String delimiter = ",";
            Tuple tuple1 = prepareTupleImpl(delimiter);
            tuple1.addColumn("c0", "column0");
            tuple1.addColumn("c2", "column2");
            tuple1.addColumn("c3", "column3");
            tuple1.addColumn("c4", "column4");
            tuple1.addColumn("c1", "CAP(“全容”或者“预测\"或者“简化”请其中一个)");
            Object o = getTuplesImplInstance(10);

            // Append tuple into
            Method append = o.getClass().getDeclaredMethod("append", Tuple.class);
            append.setAccessible(true);
            append.invoke(o, tuple1);

            // Set delimiter
            Method setDelimiter = o.getClass().getDeclaredMethod("setDelimiter", String.class);
            Assert.assertNotNull(setDelimiter);
            setDelimiter.setAccessible(true);
            setDelimiter.invoke(o, delimiter);

            Method size = o.getClass().getDeclaredMethod("size");
            Assert.assertNotNull(size);
            size.setAccessible(true);
            int tupleSize = (int) size.invoke(o);
            Assert.assertEquals(tupleSize, 1);

            // Before reset, the tuples list size == 1.
            Field tuplesList = o.getClass().getDeclaredField("tuples");
            Assert.assertNotNull(tuplesList);
            tuplesList.setAccessible(true);
            List<Tuple> list = (List<Tuple>) tuplesList.get(o);
            Assert.assertNotNull(list);
            Assert.assertEquals(1, list.size());

            Method reset = o.getClass().getDeclaredMethod("reset");
            Assert.assertNotNull(reset);
            reset.setAccessible(true);
            reset.invoke(o);

            int tupleSize2 = (int) size.invoke(o);
            Assert.assertEquals(tupleSize2, 0);
            // After reset, the tuple list size == 0
            Assert.assertEquals(0, list.size());

            Tuple tuple2 = prepareTupleImpl(delimiter);
            tuple2.addColumn("c0", "column_0");
            tuple2.addColumn("c2", "column_2");
            tuple2.addColumn("c3", "column_3");
            tuple2.addColumn("c4", "column_4");
            tuple2.addColumn("c1", "CAP(“全容”或者“预测\"或者“简化”请其中一个)_1");

            append.invoke(o, tuple2);
            int tupleSize3 = (int) size.invoke(o);
            Assert.assertEquals(tupleSize3, 1);
            // After reset, the tuple list size == 0
            Assert.assertEquals(1, list.size());

        } catch (Exception e) {
            l.error("TestTuplesImplReset exception", e);
            throw new RuntimeException(e);
        }
    }

    @Test
    public void TestTuplesImplConstructionWithInvalidParameters() {
        // With negative CSV batch, the InvalidException will be thrown.
        boolean exceptionHappened = false;
        try {
            getTuplesImplInstance(-10);
        } catch (Exception e) {
            exceptionHappened = true;
        }
        Assert.assertTrue(exceptionHappened);
    }

    private Object getTuplesImplInstance(int csvBatch) throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        String className = "cn.ymatrix.builder.MxClientImpl$TuplesImpl";
        Class<?> innerClazz = Class.forName(className);
        Constructor<?> constructor = innerClazz.getDeclaredConstructor(String.class, String.class, int.class);
        constructor.setAccessible(true);
        Object o = constructor.newInstance("public", "test_table", csvBatch);
        Assert.assertEquals(className, o.getClass().getName());
        return o;
    }

    private TupleImpl prepareTupleImpl(String delimiter) {
        String schema = "public";
        String table = "table";
        TupleImpl tuple = new TupleImpl(prepareColumnMetadataMap(), delimiter, schema, table);
        Assert.assertNotNull(tuple);
        Assert.assertNotNull(tuple.getColumns());
        Assert.assertEquals(tuple.getTableName(), schema + "." + table);
        Assert.assertEquals(tuple.getColumns().length, 5);
        for (int i = 0; i < tuple.getColumns().length; i++) {
            Assert.assertEquals(tuple.getColumns()[i].getColumnName(), "c" + i);
            Assert.assertNull(tuple.getColumns()[i].getValue());
        }
        return tuple;
    }

    private Map<String, ColumnMetadata> prepareColumnMetadataMap() {
        Map<String, ColumnMetadata> map = new HashMap<>();
        for (int i = 0; i < 5; i++) {
            ColumnMeta.Builder builder = ColumnMeta.newBuilder();
            builder.setName("c" + i).setType("Text").setNum(i + 1);
            builder.build();
            ColumnMetaWrapper wrapper = ColumnMetaWrapper.wrap(builder.build());
            ColumnMetadata metadata = new ColumnMetadata(wrapper, i);
            map.put("c" + i, metadata);
        }
        return map;
    }

    private Tuple prepareEmptyTuple(final int size) {
        return new Tuple() {
            private String delimiter;
            private String schema;
            private String table;

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
                this.delimiter = delimiter;
            }

            public String getDelimiter() {
                return this.delimiter;
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
}