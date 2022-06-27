package cn.ymatrix.builder;

import cn.ymatrix.api.ColumnMeta;
import cn.ymatrix.api.ColumnMetaWrapper;
import cn.ymatrix.data.Column;
import cn.ymatrix.exception.TupleNotReadyException;
import cn.ymatrix.logger.MxLogger;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;

import java.lang.reflect.Field;
import java.security.InvalidParameterException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class TestTupleImpl {
    Logger l = MxLogger.init(TestTupleImpl.class);

    @Test(expected = NullPointerException.class)
    public void TestTupleImplColumnMetaMapIsNull() {
        TupleImpl tuple = new TupleImpl(null, "|", "public", "test_table");
    }

    @Test(expected = NullPointerException.class)
    public void TestTupleImplColumnMetaMapEmpty() {
        TupleImpl tuple = new TupleImpl(new HashMap<>(), "|", "public", "test_table");
    }

    @Test(expected = InvalidParameterException.class)
    public void TestTupleImplNullDelimiter() {
        TupleImpl tuple = new TupleImpl(prepareColumnMetadataMap(), null, "public", "test_table");
    }

    @Test(expected = InvalidParameterException.class)
    public void TestTupleImplEmptyDelimiter() {
        TupleImpl tuple = new TupleImpl(prepareColumnMetadataMap(), "", "public", "test_table");
    }

    @Test
    public void TestCrateTupleImplNormal() {
        TupleImpl tuple = prepareNormalTuple();
        Assert.assertEquals(tuple.size(), 5);

        // Add columns exists
        tuple.addColumn("c0", 123456);
        tuple.addColumn("c1", 2.34);
        tuple.addColumn("c2", "this is a string value");
        tuple.addColumn("c3", "  ");
        tuple.addColumn("c4", 1234567890123.456789);

        boolean c0Found = false;
        boolean c1Found = false;
        boolean c2Found = false;
        boolean c3Found = false;
        boolean c4Found = false;
        for (Column column : tuple.getColumns()) {
            if (column.getColumnName().equals("c0")) {
                Assert.assertEquals(column.getValue(), 123456);
                c0Found = true;
            }
            if (column.getColumnName().equals("c1")) {
                Assert.assertEquals(column.getValue(), 2.34);
                c1Found = true;
            }
            if (column.getColumnName().equals("c2")) {
                Assert.assertEquals(column.getValue(), "this is a string value");
                c2Found = true;
            }
            if (column.getColumnName().equals("c3")) {
                Assert.assertEquals(column.getValue(), "  ");
                c3Found = true;
            }
            if (column.getColumnName().equals("c4")) {
                Assert.assertEquals(column.getValue(), 1234567890123.456789);
                c4Found = true;
            }
        }
        Assert.assertTrue(c0Found);
        Assert.assertTrue(c1Found);
        Assert.assertTrue(c2Found);
        Assert.assertTrue(c3Found);
        Assert.assertTrue(c4Found);

        String rawInput = tuple.getRawInputString();
        Assert.assertNotNull(rawInput);
        Assert.assertEquals("123456 2.34 this is a string value    1.2345678901234568E12", rawInput);
    }

    @Test(expected = NullPointerException.class)
    public void TestTupleAddColumnNull() {
        TupleImpl tuple = prepareNormalTuple();
        tuple.addColumn(null, "add null column name");
    }

    @Test(expected = NullPointerException.class)
    public void TestTupleAddColumnValueNull() {
        TupleImpl tuple = prepareNormalTuple();
        tuple.addColumn("c1", null);
    }

    @Test(expected = NullPointerException.class)
    public void TestTupleColumnNameNotExists() {
        TupleImpl tuple = prepareNormalTuple();
        tuple.addColumn("not exists column", 12345);
    }

    @Test
    public void TestTupleEmptyColumnSet() throws NoSuchFieldException, IllegalAccessException {
        // Use reflection to get private filed for test.
        TupleImpl tuple = prepareNormalTuple();
        Field emptyColumnSet = TupleImpl.class.getDeclaredField("emptyColumnsSet");
        emptyColumnSet.setAccessible(true);
        Set<Column> set = (Set<Column>) emptyColumnSet.get(tuple);
        Assert.assertEquals(set.size(), 5);

        // Add column, then the c1 will be removed from the emptyColumnSet
        tuple.addColumn("c1", 12345);
        Assert.assertEquals(set.size(), 4);
        boolean c1Found = false;
        for (Column column : set) {
            if (column.getColumnName().equals("c1")) {
                c1Found = true;
            }
        }
        Assert.assertFalse(c1Found);

        tuple.addColumn("c0", 123456);
        tuple.addColumn("c2", "this is a string value");
        tuple.addColumn("c3", "  ");
        tuple.addColumn("c4", 1234567890123.456789);
        Assert.assertTrue(set.isEmpty());
    }

    @Test
    public void TestTupleToString() {
        TupleImpl tuple = prepareNormalTuple();
        String emptyTuple = "||||";
        Assert.assertEquals(tuple.toString(), emptyTuple);

        tuple.addColumn("c0", 123456);
        String oneColumn = "123456||||";
        Assert.assertEquals(tuple.toString(), oneColumn);

        tuple.addColumn("c2", 34563);
        tuple.addColumn("c4", "c4_str");

        String threeColumns = "123456||34563||c4_str";
        Assert.assertEquals(tuple.toString(), threeColumns);
    }

    @Test(expected = NullPointerException.class)
    public void TestTupleToCSVLineString() {
        TupleImpl tuple = prepareTupleDefaultNotNull();
        // c2 has not been set and has no default value, will throw NullPointerException.
        tuple.toCSVLineStr();
    }

    @Test
    public void TestTupleToCSVLineStringDefaultValue() {
        TupleImpl tuple = prepareTupleDefaultNotNull();
        tuple.addColumn("c2", "this is c2 value");
        String expectedCSV = "|default_c1|this is c2 value||"; // For performance no quote of these columns.
        Assert.assertEquals(expectedCSV, tuple.toCSVLineStr());
    }

    @Test(expected = TupleNotReadyException.class)
    public void TestReadinessCheckNotReady() {
        TupleImpl tuple = prepareTupleDefaultNotNull();
        tuple.readinessCheck();
    }

    @Test(expected = TupleNotReadyException.class)
    public void TestReadinessCheckNotReadyC1() {
        TupleImpl tuple = prepareTupleDefaultNotNull();
        tuple.addColumn("c1", 1234223);
        tuple.readinessCheck();
    }

    @Test
    public void TestReadinessCheckNotReadyC2() {
        TupleImpl tuple = prepareTupleDefaultNotNull();
        tuple.addColumn("c2", 1234223);
        tuple.readinessCheck();
        String expectedCSV = "|default_c1|1234223||"; // For performance, no quote of these columns.
        Assert.assertEquals(expectedCSV, tuple.toCSVLineStr());
    }

    @Test(expected = TupleNotReadyException.class)
    public void TestReadinessCheckNotNull() {
        TupleImpl tuple = prepareTupleTemplate(prepareColumnMetadataMapNotNull());
        tuple.readinessCheck();
    }

    @Test
    public void TestReadinessCheckNotNullC2() {
        TupleImpl tuple = prepareTupleTemplate(prepareColumnMetadataMapNotNull());
        tuple.addColumn("c2", 123456);
        tuple.readinessCheck();
        String expectedCSV = "||123456||"; // For performance, no quote of this column.
        Assert.assertEquals(tuple.toCSVLineStr(), expectedCSV);
    }

    @Test
    public void TestReset() {
        TupleImpl tuple = prepareTupleTemplate(prepareColumnMetadataMapNotNull());
        tuple.addColumn("c2", 123456);
        tuple.readinessCheck();
        String expectedCSV = "||123456||"; // For performance, no quote of this column.
        Assert.assertEquals(tuple.toCSVLineStr(), expectedCSV);

        tuple.reset();
        boolean tupleIsNotReady = false;
        try {
            tuple.readinessCheck();
        } catch (TupleNotReadyException e) {
            tupleIsNotReady = true;
            Assert.assertTrue(e.getMessage().contains("the column c2 has no default value and could not be null, So its value must be set"));
        }
        Assert.assertTrue(tupleIsNotReady);

        tuple.addColumn("c2", 12345);
        tuple.addColumn("c1", 123);
        String expectedCSV2 = "|123|12345||";
        tuple.readinessCheck();
        Assert.assertEquals(expectedCSV2, tuple.toCSVLineStr());

        tuple.reset();
        tuple.addColumn("c2", 2);
        tuple.addColumn("c3", 3);
        tuple.addColumn("c4", 4);
        tuple.readinessCheck();
        Assert.assertEquals("||2|3|4", tuple.toCSVLineStr());

        tuple.addColumn("c0", 0);
        tuple.addColumn("c1", 1);
        Assert.assertEquals("0|1|2|3|4", tuple.toCSVLineStr());
    }

    private TupleImpl prepareNormalTuple() {
        String schema = "public";
        String table = "table";
        TupleImpl tuple = new TupleImpl(prepareColumnMetadataMap(), "|", schema, table);
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

    private TupleImpl prepareTupleDefaultNotNull() {
        String schema = "public";
        String table = "table";
        TupleImpl tuple = new TupleImpl(prepareColumnMetadataMapWithDefaultNotNull(), "|", schema, table);
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

    private TupleImpl prepareTupleTemplate(Map<String, ColumnMetadata> map) {
        String schema = "public";
        String table = "table";
        TupleImpl tuple = new TupleImpl(map, "|", schema, table);
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

    private Map<String, ColumnMetadata> prepareColumnMetadataMapWithDefaultNotNull() {
        Map<String, ColumnMetadata> map = new HashMap<>();
        for (int i = 0; i < 5; i++) {
            ColumnMeta.Builder builder = ColumnMeta.newBuilder();
            if (i == 1) {
                // Not null and has default value.
                builder.setName("c" + i).setType("Text").setNum(i + 1).setDefault("default_c1").setIsNotNull(true);
            } else if (i == 2) {
                // Not default value but not null.
                builder.setName("c" + i).setType("Text").setNum(i + 1).setIsNotNull(true);
            } else {
                builder.setName("c" + i).setType("Text").setNum(i + 1);
            }
            builder.build();
            ColumnMetaWrapper wrapper = ColumnMetaWrapper.wrap(builder.build());
            ColumnMetadata metadata = new ColumnMetadata(wrapper, i);
            map.put("c" + i, metadata);
        }
        return map;
    }

    private Map<String, ColumnMetadata> prepareColumnMetadataMapNotNull() {
        Map<String, ColumnMetadata> map = new HashMap<>();
        for (int i = 0; i < 5; i++) {
            ColumnMeta.Builder builder = ColumnMeta.newBuilder();
            if (i == 2) {
                // Not default value but not null.
                builder.setName("c" + i).setType("Text").setNum(i + 1).setIsNotNull(true);
            } else {
                builder.setName("c" + i).setType("Text").setNum(i + 1);
            }
            builder.build();
            ColumnMetaWrapper wrapper = ColumnMetaWrapper.wrap(builder.build());
            ColumnMetadata metadata = new ColumnMetadata(wrapper, i);
            map.put("c" + i, metadata);
        }
        return map;
    }

}
