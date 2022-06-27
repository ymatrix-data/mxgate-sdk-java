package cn.ymatrix.builder;

import cn.ymatrix.data.Column;
import cn.ymatrix.logger.MxLogger;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;

import java.security.InvalidParameterException;

public class TestTupleImplLite {
    private static final String delimiter = "|";
    private static final String schema = "public";
    private static final String table = "table";
    private static final Logger l = MxLogger.init(TestTupleImplLite.class);

    @Test(expected = InvalidParameterException.class)
    public void TestTupleImplLiteCreationWithException() {
        new TupleImplLite(null, schema, table);
    }

    @Test(expected = InvalidParameterException.class)
    public void TestTupleImplLiteCreationWithException2() {
        new TupleImplLite(delimiter, null, table);
    }

    @Test(expected = InvalidParameterException.class)
    public void TestTupleImplLiteCreationWithException3() {
        new TupleImplLite(delimiter, schema, null);
    }

    @Test(expected = InvalidParameterException.class)
    public void TestTupleImplLiteCreationWithException4() {
        new TupleImplLite("", schema, table);
    }

    @Test(expected = InvalidParameterException.class)
    public void TestTupleImplLiteCreationWithException5() {
        new TupleImplLite(delimiter, "", table);
    }

    @Test(expected = InvalidParameterException.class)
    public void TestTupleImplLiteCreationWithException6() {
        new TupleImplLite(delimiter, schema, "");
    }

    @Test(expected = InvalidParameterException.class)
    public void TestTupleAddColumnWithException() {
        TupleImplLite tupleImplLite = new TupleImplLite(delimiter, schema, table);
        tupleImplLite.addColumn(null, "this is a value");
    }

    @Test(expected = InvalidParameterException.class)
    public void TestTupleAddColumnWithException2() {
        TupleImplLite tupleImplLite = new TupleImplLite(delimiter, schema, table);
        tupleImplLite.addColumn("this is a key", null);
    }

    @Test
    public void TestTupleSetGetDelimiter() {
        TupleImplLite tupleImplLite = new TupleImplLite(delimiter, schema, table);
        String newDelimiter = "#";
        tupleImplLite.setDelimiter(newDelimiter);
        Assert.assertEquals(newDelimiter, tupleImplLite.getDelimiter());
    }

    @Test
    public void TestTupleAddColumn() {
        TupleImplLite tupleImplLite = new TupleImplLite(delimiter, schema, table);
        tupleImplLite.addColumn("c1", 1);
        tupleImplLite.addColumn("c2", 2);
        tupleImplLite.addColumn("c3", 3);
        tupleImplLite.addColumn("c4", 4);
        tupleImplLite.addColumn("c5", 5);

        Assert.assertEquals(tupleImplLite.size(), 5);
        Assert.assertEquals(schema + "." + table, tupleImplLite.getTableName());

        Column[] columns = tupleImplLite.getColumns();
        Assert.assertNotNull(columns);
        Assert.assertEquals(columns.length, 5);

        boolean find1 = false;
        boolean find2 = false;
        boolean find3 = false;
        boolean find4 = false;
        boolean find5 = false;

        for (Column c : columns) {
            Assert.assertNotNull(c);
            // For lite TupleImpl, for performance, there are no column names stored.
            Assert.assertNull(c.getColumnName());
            if (c.getValue().equals(1)) {
                find1 = true;
            }
            if (c.getValue().equals(2)) {
                find2 = true;
            }
            if (c.getValue().equals(3)) {
                find3 = true;
            }
            if (c.getValue().equals(4)) {
                find4 = true;
            }
            if (c.getValue().equals(5)) {
                find5 = true;
            }
        }
        Assert.assertTrue(find1);
        Assert.assertTrue(find2);
        Assert.assertTrue(find3);
        Assert.assertTrue(find4);
        Assert.assertTrue(find5);

        // No exception
        tupleImplLite.readinessCheck();
    }

    @Test
    public void TestTupleToCSVLineStr() {
        TupleImplLite tupleImplLite = new TupleImplLite(delimiter, schema, table);
        Assert.assertEquals(tupleImplLite.toCSVLineStr(), "");
        Assert.assertEquals(tupleImplLite.toString(), "");
        tupleImplLite.addColumn("c1", 1);
        tupleImplLite.addColumn("c2", 2);
        tupleImplLite.addColumn("c3", 3);
        tupleImplLite.addColumn("c4", 4);
        tupleImplLite.addColumn("c5", 5);
        Assert.assertEquals(tupleImplLite.toCSVLineStr(), "\"1\"|\"2\"|\"3\"|\"4\"|\"5\"");
        Assert.assertEquals(tupleImplLite.toString(), "\"1\"|\"2\"|\"3\"|\"4\"|\"5\"");
    }

    @Test
    public void TestTupleGetRawInputStr() {
        TupleImplLite tupleImplLite = new TupleImplLite(delimiter, schema, table);
        tupleImplLite.addColumn("c1", 1);
        tupleImplLite.addColumn("c2", 2);
        tupleImplLite.addColumn("c3", 3);
        tupleImplLite.addColumn("c4", 4);
        tupleImplLite.addColumn("c5", 5);
        Assert.assertEquals(tupleImplLite.getRawInputString(), "1 2 3 4 5");
    }

    @Test
    public void TestReset() {
        TupleImplLite tupleImplLite = new TupleImplLite(delimiter, schema, table);
        Assert.assertEquals(tupleImplLite.toCSVLineStr(), "");
        Assert.assertEquals(tupleImplLite.toString(), "");
        tupleImplLite.addColumn("c1", 1);
        tupleImplLite.addColumn("c2", 2);
        tupleImplLite.addColumn("c3", 3);
        tupleImplLite.addColumn("c4", 4);
        tupleImplLite.addColumn("c5", 5);

        Assert.assertEquals(tupleImplLite.size(), 5);
        tupleImplLite.reset();
        Assert.assertEquals(tupleImplLite.size(), 0);
    }

}
