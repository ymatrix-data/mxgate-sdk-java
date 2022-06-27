package cn.ymatrix.utils;

import cn.ymatrix.builder.Utils;
import cn.ymatrix.data.Tuple;
import cn.ymatrix.logger.MxLogger;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class TestCSVConstructor {
    private static final Logger l = MxLogger.init(TestCSVConstructor.class);

    private static final String table = "test_table";

    private static final String schema = "public";

    private static final String delimiter = "|";

    interface checker {
        void checkStringBuilder(StringBuilder sb);
    }

    @Test
    public void TestJoinPoolSize() {
        CSVConstructor constructor = new CSVConstructor(100);
        Assert.assertEquals(constructor.getJoinPoolParallel(), 100);
    }

    @Test(expected = InvalidParameterException.class)
    public void TestCSVConstructionWithInvalidParameterException() {
        new CSVConstructor(0);
    }

    @Test(expected = InvalidParameterException.class)
    public void TestCSVConstructionWithInvalidParameterException2() {
        new CSVConstructor(-1);
    }

    @Test(expected = NullPointerException.class)
    public void TestCSVConstructWithNullPointerException() {
        CSVConstructor constructor = new CSVConstructor(20);
        constructor.constructCSVFromTuplesWithTasks(null, 10, delimiter);
    }

    @Test
    public void TestCSVConstructWithEmptyList() {
        CSVConstructor constructor = new CSVConstructor(20);
        StringBuilder sb = constructor.constructCSVFromTuplesWithTasks(new ArrayList<>(), 10, delimiter);
        Assert.assertNull(sb);
    }

    @Test(expected = InvalidParameterException.class)
    public void TestCSVConstructionWithInvalidDelimiter1() {
        CSVConstructor constructor = new CSVConstructor(20);
        constructor.constructCSVFromTuplesWithTasks(generateTuplesList(100, delimiter, schema, table), 10, "");
    }

    @Test(expected = InvalidParameterException.class)
    public void TestCSVConstructionWithInvalidDelimiter2() {
        CSVConstructor constructor = new CSVConstructor(20);
        constructor.constructCSVFromTuplesWithTasks(generateTuplesList(100, delimiter, schema, table), 10, null);
    }

    @Test(expected = InvalidParameterException.class)
    public void TestCSVConstructionWithInvalidBatch1() {
        CSVConstructor constructor = new CSVConstructor(20);
        constructor.constructCSVFromTuplesWithTasks(generateTuplesList(100, delimiter, schema, table), 0, delimiter);
    }

    @Test(expected = InvalidParameterException.class)
    public void TestCSVConstructionWithInvalidBatch2() {
        CSVConstructor constructor = new CSVConstructor(20);
        constructor.constructCSVFromTuplesWithTasks(generateTuplesList(100, delimiter, schema, table), -1, delimiter);
    }

    @Test
    public void TestBatchSizeIsGreater() {
        CSVConstructor constructor = new CSVConstructor(20);
        StringBuilder csv = constructor.constructCSVFromTuplesWithTasks(generateTuplesList(100, delimiter, schema, table), 200, delimiter);
        Assert.assertEquals(expectedStr, csv.toString());
    }

    @Test
    public void TestCSVConstructionWithSplitTasks() {
        CSVConstructor constructor = new CSVConstructor(50);
        StringBuilder csv = constructor.constructCSVFromTuplesWithTasks(generateTuplesList(100, delimiter, schema, table), 10, "|");
        Assert.assertEquals(expectedStr, csv.toString());
    }

    @Test
    public void TestCSVConstructionWithMultipleTuples() {
        CSVConstructor constructor = new CSVConstructor(50);
        StringBuilder csv1 = constructor.constructCSVFromTuplesWithTasks(generateTuplesList(100, delimiter, schema, table), 10, "|");
        StringBuilder csv2 = constructor.constructCSVFromTuplesWithTasks(generateTuplesList2(100, delimiter, schema, table), 10, "|");
        Assert.assertEquals(expectedStr, csv1.toString());
        Assert.assertEquals(expectedStr2, csv2.toString());
        l.info(csv2.toString());
    }

    @Test
    public void TestCSVConstructionWithMultipleThreads() {
        final CSVConstructor constructor = new CSVConstructor(50);
        CountDownLatch latch = new CountDownLatch(4);
        final boolean[] success = {false, false, false, false};

        new Thread() {
            @Override
            public void run() {
                StringBuilder csv1 = constructor.constructCSVFromTuplesWithTasks(generateTuplesList(100, delimiter, schema, table), 10, "|");
                success[0] = csv1.toString().equals(expectedStr);
                latch.countDown();
            }
        }.start();

        new Thread() {
            @Override
            public void run() {
                StringBuilder csv2 = constructor.constructCSVFromTuplesWithTasks(generateTuplesList2(100, delimiter, schema, table), 10, "|");
                success[1] = csv2.toString().equals(expectedStr2);
            }
        }.start();

        new Thread() {
            @Override
            public void run() {
                StringBuilder csv1 = constructor.constructCSVFromTuplesWithTasks(generateTuplesList(100, delimiter, schema, table), 10, "|");
                success[2] = csv1.toString().equals(expectedStr);
                latch.countDown();
            }
        }.start();

        new Thread() {
            @Override
            public void run() {
                StringBuilder csv2 = constructor.constructCSVFromTuplesWithTasks(generateTuplesList2(100, delimiter, schema, table), 10, "|");
                success[3] = csv2.toString().equals(expectedStr2);
            }
        }.start();

        try {
            latch.await(3, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        Assert.assertTrue(success[0]);
        Assert.assertTrue(success[1]);
        Assert.assertTrue(success[2]);
        Assert.assertTrue(success[3]);
    }

    @Test
    public void TestCSVConstructionWithBrokenLine() {
        final CSVConstructor constructor = new CSVConstructor(50);
        StringBuilder sb = constructor.constructCSVFromTuplesWithTasks(generateTuplesWithBrokenLine(100, delimiter, schema, table), 1, delimiter);
        Assert.assertNull(sb);
    }

    @Test
    public void TestCSVConstruction() {
        csvConstruction(generateTuplesList(100, delimiter, schema, table), new checker() {
            @Override
            public void checkStringBuilder(StringBuilder sb) {
                Assert.assertNotNull(sb);
                Assert.assertEquals(sb.toString(), expectedStr);
            }
        });

        csvConstruction(generateTuplesList2(100, delimiter, schema, table), new checker() {
            @Override
            public void checkStringBuilder(StringBuilder sb) {
                Assert.assertNotNull(sb);
                Assert.assertEquals(sb.toString(), expectedStr2);
            }
        });

        final String es1 = "\"column0\"|\"CAP(“全容”或者“预测\"\"或者“简化”请其中一个)\"|\"column2\"|\"column3\"|\"column4\"\n";
        csvConstruction(generateTuplesList3(1, delimiter, schema, table), new checker() {
            @Override
            public void checkStringBuilder(StringBuilder sb) {
                Assert.assertNotNull(sb);
                Assert.assertEquals(sb.toString(), es1);
            }
        });

        String es2 = "\"abc|d2\"|\"c1\"|\"\"\"abc\"|\"\"\"\"\"\"|\"ab\"\"c,d2\"\n";
        csvConstruction(generateTuplesList4(1, delimiter, schema, table), new checker() {
            @Override
            public void checkStringBuilder(StringBuilder sb) {
                Assert.assertNotNull(sb);
                Assert.assertEquals(sb.toString(), es2);
            }
        });
    }

    @Test
    public void TestCSVConstructionEdgeCases() {
        // With nullable tuples and will generate nullable StringBuilder;
        csvConstruction(generateTuplesListNull(100, delimiter, schema, table), new checker() {
            @Override
            public void checkStringBuilder(StringBuilder sb) {
                Assert.assertNull(sb);
            }
        });

        csvConstruction(generateTuplesWithSkipColumns(100, delimiter, schema, table), new checker() {
            @Override
            public void checkStringBuilder(StringBuilder sb) {
                Assert.assertNotNull(sb);
                Assert.assertEquals(sb.toString(), expectedStrSkipColumns);
            }
        });

        csvConstruction(generateTuplesWithEmptyColumns(100, delimiter, schema, table), new checker() {
            @Override
            public void checkStringBuilder(StringBuilder sb) {
                Assert.assertNotNull(sb);
                Assert.assertEquals(sb.toString(), expectedStrWithEmptyColumns);
            }
        });


    }

    private List<Tuple> generateTuplesListNull(int size, String delimiter, String schema, String table) {
        List<Tuple> tuplesList = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            tuplesList.add(null);
        }
        return tuplesList;
    }

    private List<Tuple> generateTuplesWithSkipColumns(int size, String delimiter, String schema, String table) {
        List<Tuple> tuplesList = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            Tuple tuple = generateTuple(delimiter, schema, table);
            tuple.addColumn("c1", i);
            tuple.addColumn("c2", i);
            tuple.addColumn("c3", i);
            tuple.addColumn("c4", i);
            tuple.addColumn("c5", i);

            for (int j = 0; j < tuple.getColumns().length; j++) {
                if (j == 1) {
                    tuple.getColumns()[j].setSkip();
                }
            }

            tuplesList.add(tuple);
        }
        return tuplesList;
    }

    private List<Tuple> generateTuplesWithEmptyColumns(int size, String delimiter, String schema, String table) {
        List<Tuple> tuplesList = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            Tuple tuple = generateTuple(delimiter, schema, table);
            tuple.addColumn("c1", i);
            tuple.addColumn("c2", "");
            tuple.addColumn("c3", "");
            tuple.addColumn("c4", i);
            tuple.addColumn("c5", i);
            tuplesList.add(tuple);
        }
        return tuplesList;
    }

    private List<Tuple> generateTuplesWithBrokenLine(int size, String delimiter, String schema, String table) {
        List<Tuple> tuplesList = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            // Add a nullable Tuple which i == 2;
            if (i == 2) {
                tuplesList.add(null);
                continue;
            }
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

    private List<Tuple> generateTuplesList2(int size, String delimiter, String schema, String table) {
        List<cn.ymatrix.data.Tuple> tuplesList = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            cn.ymatrix.data.Tuple tuple = generateTuple(delimiter, schema, table);
            tuple.addColumn("c1", i * i);
            tuple.addColumn("c2", i * i);
            tuple.addColumn("c3", i * i);
            tuple.addColumn("c4", i * i);
            tuple.addColumn("c5", i * i);
            tuplesList.add(tuple);
        }
        return tuplesList;
    }

    private List<Tuple> generateTuplesList3(int size, String delimiter, String schema, String table) {
        List<Tuple> tuplesList = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            Tuple tuple = generateTuple(delimiter, schema, table);
            tuple.addColumn("c0", "column0");
            tuple.addColumn("c1", "CAP(“全容”或者“预测\"或者“简化”请其中一个)");
            tuple.addColumn("c2", "column2");
            tuple.addColumn("c3", "column3");
            tuple.addColumn("c4", "column4");
            tuplesList.add(tuple);
        }
        return tuplesList;
    }

    private List<Tuple> generateTuplesList4(int size, String delimiter, String schema, String table) {
        List<Tuple> tuplesList = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            Tuple tuple = generateTuple(delimiter, schema, table);
            tuple.addColumn("c0", "abc|d2");
            tuple.addColumn("c1", "c1");
            tuple.addColumn("c2", "\"abc");
            tuple.addColumn("c3", "\"\"");
            tuple.addColumn("c4", "ab\"c,d2");
            tuplesList.add(tuple);
        }
        return tuplesList;
    }


    private void csvConstruction(List<Tuple> tuples, checker c) {
        try {
            Object subTaskObj = getSubTaskInstance(tuples, 0, tuples.size(), 10, delimiter);
            Assert.assertNotNull(subTaskObj);
            Method runMethod = subTaskObj.getClass().getDeclaredMethod("run", List.class);
            Assert.assertNotNull(runMethod);
            runMethod.setAccessible(true);
            StringBuilder sb = (StringBuilder) runMethod.invoke(subTaskObj, tuples);
            c.checkStringBuilder(sb);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Tuple generateTuple(String delimiter, String schema, String table) {
        return Utils.generateEmptyTupleLite(delimiter, schema, table);
    }

    private Object getSubTaskInstance(List<Tuple> tuples, int startRow, int endRow, int batch, String delimiter) {
        try {
            String className = "cn.ymatrix.utils.CSVConstructor$SubTask";
            Class<?> innerClazz = Class.forName(className);
            Constructor<?> constructor = innerClazz.getDeclaredConstructor(List.class, int.class, int.class, int.class, String.class);
            constructor.setAccessible(true);
            Object o = constructor.newInstance(tuples, startRow, endRow, batch, delimiter);
            Assert.assertEquals(o.getClass().getName(), className);
            return o;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static final String expectedStr =
            "\"0\"|\"0\"|\"0\"|\"0\"|\"0\"\n" +
            "\"1\"|\"1\"|\"1\"|\"1\"|\"1\"\n" +
            "\"2\"|\"2\"|\"2\"|\"2\"|\"2\"\n" +
            "\"3\"|\"3\"|\"3\"|\"3\"|\"3\"\n" +
            "\"4\"|\"4\"|\"4\"|\"4\"|\"4\"\n" +
            "\"5\"|\"5\"|\"5\"|\"5\"|\"5\"\n" +
            "\"6\"|\"6\"|\"6\"|\"6\"|\"6\"\n" +
            "\"7\"|\"7\"|\"7\"|\"7\"|\"7\"\n" +
            "\"8\"|\"8\"|\"8\"|\"8\"|\"8\"\n" +
            "\"9\"|\"9\"|\"9\"|\"9\"|\"9\"\n" +
            "\"10\"|\"10\"|\"10\"|\"10\"|\"10\"\n" +
            "\"11\"|\"11\"|\"11\"|\"11\"|\"11\"\n" +
            "\"12\"|\"12\"|\"12\"|\"12\"|\"12\"\n" +
            "\"13\"|\"13\"|\"13\"|\"13\"|\"13\"\n" +
            "\"14\"|\"14\"|\"14\"|\"14\"|\"14\"\n" +
            "\"15\"|\"15\"|\"15\"|\"15\"|\"15\"\n" +
            "\"16\"|\"16\"|\"16\"|\"16\"|\"16\"\n" +
            "\"17\"|\"17\"|\"17\"|\"17\"|\"17\"\n" +
            "\"18\"|\"18\"|\"18\"|\"18\"|\"18\"\n" +
            "\"19\"|\"19\"|\"19\"|\"19\"|\"19\"\n" +
            "\"20\"|\"20\"|\"20\"|\"20\"|\"20\"\n" +
            "\"21\"|\"21\"|\"21\"|\"21\"|\"21\"\n" +
            "\"22\"|\"22\"|\"22\"|\"22\"|\"22\"\n" +
            "\"23\"|\"23\"|\"23\"|\"23\"|\"23\"\n" +
            "\"24\"|\"24\"|\"24\"|\"24\"|\"24\"\n" +
            "\"25\"|\"25\"|\"25\"|\"25\"|\"25\"\n" +
            "\"26\"|\"26\"|\"26\"|\"26\"|\"26\"\n" +
            "\"27\"|\"27\"|\"27\"|\"27\"|\"27\"\n" +
            "\"28\"|\"28\"|\"28\"|\"28\"|\"28\"\n" +
            "\"29\"|\"29\"|\"29\"|\"29\"|\"29\"\n" +
            "\"30\"|\"30\"|\"30\"|\"30\"|\"30\"\n" +
            "\"31\"|\"31\"|\"31\"|\"31\"|\"31\"\n" +
            "\"32\"|\"32\"|\"32\"|\"32\"|\"32\"\n" +
            "\"33\"|\"33\"|\"33\"|\"33\"|\"33\"\n" +
            "\"34\"|\"34\"|\"34\"|\"34\"|\"34\"\n" +
            "\"35\"|\"35\"|\"35\"|\"35\"|\"35\"\n" +
            "\"36\"|\"36\"|\"36\"|\"36\"|\"36\"\n" +
            "\"37\"|\"37\"|\"37\"|\"37\"|\"37\"\n" +
            "\"38\"|\"38\"|\"38\"|\"38\"|\"38\"\n" +
            "\"39\"|\"39\"|\"39\"|\"39\"|\"39\"\n" +
            "\"40\"|\"40\"|\"40\"|\"40\"|\"40\"\n" +
            "\"41\"|\"41\"|\"41\"|\"41\"|\"41\"\n" +
            "\"42\"|\"42\"|\"42\"|\"42\"|\"42\"\n" +
            "\"43\"|\"43\"|\"43\"|\"43\"|\"43\"\n" +
            "\"44\"|\"44\"|\"44\"|\"44\"|\"44\"\n" +
            "\"45\"|\"45\"|\"45\"|\"45\"|\"45\"\n" +
            "\"46\"|\"46\"|\"46\"|\"46\"|\"46\"\n" +
            "\"47\"|\"47\"|\"47\"|\"47\"|\"47\"\n" +
            "\"48\"|\"48\"|\"48\"|\"48\"|\"48\"\n" +
            "\"49\"|\"49\"|\"49\"|\"49\"|\"49\"\n" +
            "\"50\"|\"50\"|\"50\"|\"50\"|\"50\"\n" +
            "\"51\"|\"51\"|\"51\"|\"51\"|\"51\"\n" +
            "\"52\"|\"52\"|\"52\"|\"52\"|\"52\"\n" +
            "\"53\"|\"53\"|\"53\"|\"53\"|\"53\"\n" +
            "\"54\"|\"54\"|\"54\"|\"54\"|\"54\"\n" +
            "\"55\"|\"55\"|\"55\"|\"55\"|\"55\"\n" +
            "\"56\"|\"56\"|\"56\"|\"56\"|\"56\"\n" +
            "\"57\"|\"57\"|\"57\"|\"57\"|\"57\"\n" +
            "\"58\"|\"58\"|\"58\"|\"58\"|\"58\"\n" +
            "\"59\"|\"59\"|\"59\"|\"59\"|\"59\"\n" +
            "\"60\"|\"60\"|\"60\"|\"60\"|\"60\"\n" +
            "\"61\"|\"61\"|\"61\"|\"61\"|\"61\"\n" +
            "\"62\"|\"62\"|\"62\"|\"62\"|\"62\"\n" +
            "\"63\"|\"63\"|\"63\"|\"63\"|\"63\"\n" +
            "\"64\"|\"64\"|\"64\"|\"64\"|\"64\"\n" +
            "\"65\"|\"65\"|\"65\"|\"65\"|\"65\"\n" +
            "\"66\"|\"66\"|\"66\"|\"66\"|\"66\"\n" +
            "\"67\"|\"67\"|\"67\"|\"67\"|\"67\"\n" +
            "\"68\"|\"68\"|\"68\"|\"68\"|\"68\"\n" +
            "\"69\"|\"69\"|\"69\"|\"69\"|\"69\"\n" +
            "\"70\"|\"70\"|\"70\"|\"70\"|\"70\"\n" +
            "\"71\"|\"71\"|\"71\"|\"71\"|\"71\"\n" +
            "\"72\"|\"72\"|\"72\"|\"72\"|\"72\"\n" +
            "\"73\"|\"73\"|\"73\"|\"73\"|\"73\"\n" +
            "\"74\"|\"74\"|\"74\"|\"74\"|\"74\"\n" +
            "\"75\"|\"75\"|\"75\"|\"75\"|\"75\"\n" +
            "\"76\"|\"76\"|\"76\"|\"76\"|\"76\"\n" +
            "\"77\"|\"77\"|\"77\"|\"77\"|\"77\"\n" +
            "\"78\"|\"78\"|\"78\"|\"78\"|\"78\"\n" +
            "\"79\"|\"79\"|\"79\"|\"79\"|\"79\"\n" +
            "\"80\"|\"80\"|\"80\"|\"80\"|\"80\"\n" +
            "\"81\"|\"81\"|\"81\"|\"81\"|\"81\"\n" +
            "\"82\"|\"82\"|\"82\"|\"82\"|\"82\"\n" +
            "\"83\"|\"83\"|\"83\"|\"83\"|\"83\"\n" +
            "\"84\"|\"84\"|\"84\"|\"84\"|\"84\"\n" +
            "\"85\"|\"85\"|\"85\"|\"85\"|\"85\"\n" +
            "\"86\"|\"86\"|\"86\"|\"86\"|\"86\"\n" +
            "\"87\"|\"87\"|\"87\"|\"87\"|\"87\"\n" +
            "\"88\"|\"88\"|\"88\"|\"88\"|\"88\"\n" +
            "\"89\"|\"89\"|\"89\"|\"89\"|\"89\"\n" +
            "\"90\"|\"90\"|\"90\"|\"90\"|\"90\"\n" +
            "\"91\"|\"91\"|\"91\"|\"91\"|\"91\"\n" +
            "\"92\"|\"92\"|\"92\"|\"92\"|\"92\"\n" +
            "\"93\"|\"93\"|\"93\"|\"93\"|\"93\"\n" +
            "\"94\"|\"94\"|\"94\"|\"94\"|\"94\"\n" +
            "\"95\"|\"95\"|\"95\"|\"95\"|\"95\"\n" +
            "\"96\"|\"96\"|\"96\"|\"96\"|\"96\"\n" +
            "\"97\"|\"97\"|\"97\"|\"97\"|\"97\"\n" +
            "\"98\"|\"98\"|\"98\"|\"98\"|\"98\"\n" +
            "\"99\"|\"99\"|\"99\"|\"99\"|\"99\"\n";

    private static final String expectedStr2 =
            "\"0\"|\"0\"|\"0\"|\"0\"|\"0\"\n" +
            "\"1\"|\"1\"|\"1\"|\"1\"|\"1\"\n" +
            "\"4\"|\"4\"|\"4\"|\"4\"|\"4\"\n" +
            "\"9\"|\"9\"|\"9\"|\"9\"|\"9\"\n" +
            "\"16\"|\"16\"|\"16\"|\"16\"|\"16\"\n" +
            "\"25\"|\"25\"|\"25\"|\"25\"|\"25\"\n" +
            "\"36\"|\"36\"|\"36\"|\"36\"|\"36\"\n" +
            "\"49\"|\"49\"|\"49\"|\"49\"|\"49\"\n" +
            "\"64\"|\"64\"|\"64\"|\"64\"|\"64\"\n" +
            "\"81\"|\"81\"|\"81\"|\"81\"|\"81\"\n" +
            "\"100\"|\"100\"|\"100\"|\"100\"|\"100\"\n" +
            "\"121\"|\"121\"|\"121\"|\"121\"|\"121\"\n" +
            "\"144\"|\"144\"|\"144\"|\"144\"|\"144\"\n" +
            "\"169\"|\"169\"|\"169\"|\"169\"|\"169\"\n" +
            "\"196\"|\"196\"|\"196\"|\"196\"|\"196\"\n" +
            "\"225\"|\"225\"|\"225\"|\"225\"|\"225\"\n" +
            "\"256\"|\"256\"|\"256\"|\"256\"|\"256\"\n" +
            "\"289\"|\"289\"|\"289\"|\"289\"|\"289\"\n" +
            "\"324\"|\"324\"|\"324\"|\"324\"|\"324\"\n" +
            "\"361\"|\"361\"|\"361\"|\"361\"|\"361\"\n" +
            "\"400\"|\"400\"|\"400\"|\"400\"|\"400\"\n" +
            "\"441\"|\"441\"|\"441\"|\"441\"|\"441\"\n" +
            "\"484\"|\"484\"|\"484\"|\"484\"|\"484\"\n" +
            "\"529\"|\"529\"|\"529\"|\"529\"|\"529\"\n" +
            "\"576\"|\"576\"|\"576\"|\"576\"|\"576\"\n" +
            "\"625\"|\"625\"|\"625\"|\"625\"|\"625\"\n" +
            "\"676\"|\"676\"|\"676\"|\"676\"|\"676\"\n" +
            "\"729\"|\"729\"|\"729\"|\"729\"|\"729\"\n" +
            "\"784\"|\"784\"|\"784\"|\"784\"|\"784\"\n" +
            "\"841\"|\"841\"|\"841\"|\"841\"|\"841\"\n" +
            "\"900\"|\"900\"|\"900\"|\"900\"|\"900\"\n" +
            "\"961\"|\"961\"|\"961\"|\"961\"|\"961\"\n" +
            "\"1024\"|\"1024\"|\"1024\"|\"1024\"|\"1024\"\n" +
            "\"1089\"|\"1089\"|\"1089\"|\"1089\"|\"1089\"\n" +
            "\"1156\"|\"1156\"|\"1156\"|\"1156\"|\"1156\"\n" +
            "\"1225\"|\"1225\"|\"1225\"|\"1225\"|\"1225\"\n" +
            "\"1296\"|\"1296\"|\"1296\"|\"1296\"|\"1296\"\n" +
            "\"1369\"|\"1369\"|\"1369\"|\"1369\"|\"1369\"\n" +
            "\"1444\"|\"1444\"|\"1444\"|\"1444\"|\"1444\"\n" +
            "\"1521\"|\"1521\"|\"1521\"|\"1521\"|\"1521\"\n" +
            "\"1600\"|\"1600\"|\"1600\"|\"1600\"|\"1600\"\n" +
            "\"1681\"|\"1681\"|\"1681\"|\"1681\"|\"1681\"\n" +
            "\"1764\"|\"1764\"|\"1764\"|\"1764\"|\"1764\"\n" +
            "\"1849\"|\"1849\"|\"1849\"|\"1849\"|\"1849\"\n" +
            "\"1936\"|\"1936\"|\"1936\"|\"1936\"|\"1936\"\n" +
            "\"2025\"|\"2025\"|\"2025\"|\"2025\"|\"2025\"\n" +
            "\"2116\"|\"2116\"|\"2116\"|\"2116\"|\"2116\"\n" +
            "\"2209\"|\"2209\"|\"2209\"|\"2209\"|\"2209\"\n" +
            "\"2304\"|\"2304\"|\"2304\"|\"2304\"|\"2304\"\n" +
            "\"2401\"|\"2401\"|\"2401\"|\"2401\"|\"2401\"\n" +
            "\"2500\"|\"2500\"|\"2500\"|\"2500\"|\"2500\"\n" +
            "\"2601\"|\"2601\"|\"2601\"|\"2601\"|\"2601\"\n" +
            "\"2704\"|\"2704\"|\"2704\"|\"2704\"|\"2704\"\n" +
            "\"2809\"|\"2809\"|\"2809\"|\"2809\"|\"2809\"\n" +
            "\"2916\"|\"2916\"|\"2916\"|\"2916\"|\"2916\"\n" +
            "\"3025\"|\"3025\"|\"3025\"|\"3025\"|\"3025\"\n" +
            "\"3136\"|\"3136\"|\"3136\"|\"3136\"|\"3136\"\n" +
            "\"3249\"|\"3249\"|\"3249\"|\"3249\"|\"3249\"\n" +
            "\"3364\"|\"3364\"|\"3364\"|\"3364\"|\"3364\"\n" +
            "\"3481\"|\"3481\"|\"3481\"|\"3481\"|\"3481\"\n" +
            "\"3600\"|\"3600\"|\"3600\"|\"3600\"|\"3600\"\n" +
            "\"3721\"|\"3721\"|\"3721\"|\"3721\"|\"3721\"\n" +
            "\"3844\"|\"3844\"|\"3844\"|\"3844\"|\"3844\"\n" +
            "\"3969\"|\"3969\"|\"3969\"|\"3969\"|\"3969\"\n" +
            "\"4096\"|\"4096\"|\"4096\"|\"4096\"|\"4096\"\n" +
            "\"4225\"|\"4225\"|\"4225\"|\"4225\"|\"4225\"\n" +
            "\"4356\"|\"4356\"|\"4356\"|\"4356\"|\"4356\"\n" +
            "\"4489\"|\"4489\"|\"4489\"|\"4489\"|\"4489\"\n" +
            "\"4624\"|\"4624\"|\"4624\"|\"4624\"|\"4624\"\n" +
            "\"4761\"|\"4761\"|\"4761\"|\"4761\"|\"4761\"\n" +
            "\"4900\"|\"4900\"|\"4900\"|\"4900\"|\"4900\"\n" +
            "\"5041\"|\"5041\"|\"5041\"|\"5041\"|\"5041\"\n" +
            "\"5184\"|\"5184\"|\"5184\"|\"5184\"|\"5184\"\n" +
            "\"5329\"|\"5329\"|\"5329\"|\"5329\"|\"5329\"\n" +
            "\"5476\"|\"5476\"|\"5476\"|\"5476\"|\"5476\"\n" +
            "\"5625\"|\"5625\"|\"5625\"|\"5625\"|\"5625\"\n" +
            "\"5776\"|\"5776\"|\"5776\"|\"5776\"|\"5776\"\n" +
            "\"5929\"|\"5929\"|\"5929\"|\"5929\"|\"5929\"\n" +
            "\"6084\"|\"6084\"|\"6084\"|\"6084\"|\"6084\"\n" +
            "\"6241\"|\"6241\"|\"6241\"|\"6241\"|\"6241\"\n" +
            "\"6400\"|\"6400\"|\"6400\"|\"6400\"|\"6400\"\n" +
            "\"6561\"|\"6561\"|\"6561\"|\"6561\"|\"6561\"\n" +
            "\"6724\"|\"6724\"|\"6724\"|\"6724\"|\"6724\"\n" +
            "\"6889\"|\"6889\"|\"6889\"|\"6889\"|\"6889\"\n" +
            "\"7056\"|\"7056\"|\"7056\"|\"7056\"|\"7056\"\n" +
            "\"7225\"|\"7225\"|\"7225\"|\"7225\"|\"7225\"\n" +
            "\"7396\"|\"7396\"|\"7396\"|\"7396\"|\"7396\"\n" +
            "\"7569\"|\"7569\"|\"7569\"|\"7569\"|\"7569\"\n" +
            "\"7744\"|\"7744\"|\"7744\"|\"7744\"|\"7744\"\n" +
            "\"7921\"|\"7921\"|\"7921\"|\"7921\"|\"7921\"\n" +
            "\"8100\"|\"8100\"|\"8100\"|\"8100\"|\"8100\"\n" +
            "\"8281\"|\"8281\"|\"8281\"|\"8281\"|\"8281\"\n" +
            "\"8464\"|\"8464\"|\"8464\"|\"8464\"|\"8464\"\n" +
            "\"8649\"|\"8649\"|\"8649\"|\"8649\"|\"8649\"\n" +
            "\"8836\"|\"8836\"|\"8836\"|\"8836\"|\"8836\"\n" +
            "\"9025\"|\"9025\"|\"9025\"|\"9025\"|\"9025\"\n" +
            "\"9216\"|\"9216\"|\"9216\"|\"9216\"|\"9216\"\n" +
            "\"9409\"|\"9409\"|\"9409\"|\"9409\"|\"9409\"\n" +
            "\"9604\"|\"9604\"|\"9604\"|\"9604\"|\"9604\"\n" +
            "\"9801\"|\"9801\"|\"9801\"|\"9801\"|\"9801\"\n";

    private static final String expectedStrSkipColumns = "\"0\"|\"0\"|\"0\"|\"0\"\n" +
            "\"1\"|\"1\"|\"1\"|\"1\"\n" +
            "\"2\"|\"2\"|\"2\"|\"2\"\n" +
            "\"3\"|\"3\"|\"3\"|\"3\"\n" +
            "\"4\"|\"4\"|\"4\"|\"4\"\n" +
            "\"5\"|\"5\"|\"5\"|\"5\"\n" +
            "\"6\"|\"6\"|\"6\"|\"6\"\n" +
            "\"7\"|\"7\"|\"7\"|\"7\"\n" +
            "\"8\"|\"8\"|\"8\"|\"8\"\n" +
            "\"9\"|\"9\"|\"9\"|\"9\"\n" +
            "\"10\"|\"10\"|\"10\"|\"10\"\n" +
            "\"11\"|\"11\"|\"11\"|\"11\"\n" +
            "\"12\"|\"12\"|\"12\"|\"12\"\n" +
            "\"13\"|\"13\"|\"13\"|\"13\"\n" +
            "\"14\"|\"14\"|\"14\"|\"14\"\n" +
            "\"15\"|\"15\"|\"15\"|\"15\"\n" +
            "\"16\"|\"16\"|\"16\"|\"16\"\n" +
            "\"17\"|\"17\"|\"17\"|\"17\"\n" +
            "\"18\"|\"18\"|\"18\"|\"18\"\n" +
            "\"19\"|\"19\"|\"19\"|\"19\"\n" +
            "\"20\"|\"20\"|\"20\"|\"20\"\n" +
            "\"21\"|\"21\"|\"21\"|\"21\"\n" +
            "\"22\"|\"22\"|\"22\"|\"22\"\n" +
            "\"23\"|\"23\"|\"23\"|\"23\"\n" +
            "\"24\"|\"24\"|\"24\"|\"24\"\n" +
            "\"25\"|\"25\"|\"25\"|\"25\"\n" +
            "\"26\"|\"26\"|\"26\"|\"26\"\n" +
            "\"27\"|\"27\"|\"27\"|\"27\"\n" +
            "\"28\"|\"28\"|\"28\"|\"28\"\n" +
            "\"29\"|\"29\"|\"29\"|\"29\"\n" +
            "\"30\"|\"30\"|\"30\"|\"30\"\n" +
            "\"31\"|\"31\"|\"31\"|\"31\"\n" +
            "\"32\"|\"32\"|\"32\"|\"32\"\n" +
            "\"33\"|\"33\"|\"33\"|\"33\"\n" +
            "\"34\"|\"34\"|\"34\"|\"34\"\n" +
            "\"35\"|\"35\"|\"35\"|\"35\"\n" +
            "\"36\"|\"36\"|\"36\"|\"36\"\n" +
            "\"37\"|\"37\"|\"37\"|\"37\"\n" +
            "\"38\"|\"38\"|\"38\"|\"38\"\n" +
            "\"39\"|\"39\"|\"39\"|\"39\"\n" +
            "\"40\"|\"40\"|\"40\"|\"40\"\n" +
            "\"41\"|\"41\"|\"41\"|\"41\"\n" +
            "\"42\"|\"42\"|\"42\"|\"42\"\n" +
            "\"43\"|\"43\"|\"43\"|\"43\"\n" +
            "\"44\"|\"44\"|\"44\"|\"44\"\n" +
            "\"45\"|\"45\"|\"45\"|\"45\"\n" +
            "\"46\"|\"46\"|\"46\"|\"46\"\n" +
            "\"47\"|\"47\"|\"47\"|\"47\"\n" +
            "\"48\"|\"48\"|\"48\"|\"48\"\n" +
            "\"49\"|\"49\"|\"49\"|\"49\"\n" +
            "\"50\"|\"50\"|\"50\"|\"50\"\n" +
            "\"51\"|\"51\"|\"51\"|\"51\"\n" +
            "\"52\"|\"52\"|\"52\"|\"52\"\n" +
            "\"53\"|\"53\"|\"53\"|\"53\"\n" +
            "\"54\"|\"54\"|\"54\"|\"54\"\n" +
            "\"55\"|\"55\"|\"55\"|\"55\"\n" +
            "\"56\"|\"56\"|\"56\"|\"56\"\n" +
            "\"57\"|\"57\"|\"57\"|\"57\"\n" +
            "\"58\"|\"58\"|\"58\"|\"58\"\n" +
            "\"59\"|\"59\"|\"59\"|\"59\"\n" +
            "\"60\"|\"60\"|\"60\"|\"60\"\n" +
            "\"61\"|\"61\"|\"61\"|\"61\"\n" +
            "\"62\"|\"62\"|\"62\"|\"62\"\n" +
            "\"63\"|\"63\"|\"63\"|\"63\"\n" +
            "\"64\"|\"64\"|\"64\"|\"64\"\n" +
            "\"65\"|\"65\"|\"65\"|\"65\"\n" +
            "\"66\"|\"66\"|\"66\"|\"66\"\n" +
            "\"67\"|\"67\"|\"67\"|\"67\"\n" +
            "\"68\"|\"68\"|\"68\"|\"68\"\n" +
            "\"69\"|\"69\"|\"69\"|\"69\"\n" +
            "\"70\"|\"70\"|\"70\"|\"70\"\n" +
            "\"71\"|\"71\"|\"71\"|\"71\"\n" +
            "\"72\"|\"72\"|\"72\"|\"72\"\n" +
            "\"73\"|\"73\"|\"73\"|\"73\"\n" +
            "\"74\"|\"74\"|\"74\"|\"74\"\n" +
            "\"75\"|\"75\"|\"75\"|\"75\"\n" +
            "\"76\"|\"76\"|\"76\"|\"76\"\n" +
            "\"77\"|\"77\"|\"77\"|\"77\"\n" +
            "\"78\"|\"78\"|\"78\"|\"78\"\n" +
            "\"79\"|\"79\"|\"79\"|\"79\"\n" +
            "\"80\"|\"80\"|\"80\"|\"80\"\n" +
            "\"81\"|\"81\"|\"81\"|\"81\"\n" +
            "\"82\"|\"82\"|\"82\"|\"82\"\n" +
            "\"83\"|\"83\"|\"83\"|\"83\"\n" +
            "\"84\"|\"84\"|\"84\"|\"84\"\n" +
            "\"85\"|\"85\"|\"85\"|\"85\"\n" +
            "\"86\"|\"86\"|\"86\"|\"86\"\n" +
            "\"87\"|\"87\"|\"87\"|\"87\"\n" +
            "\"88\"|\"88\"|\"88\"|\"88\"\n" +
            "\"89\"|\"89\"|\"89\"|\"89\"\n" +
            "\"90\"|\"90\"|\"90\"|\"90\"\n" +
            "\"91\"|\"91\"|\"91\"|\"91\"\n" +
            "\"92\"|\"92\"|\"92\"|\"92\"\n" +
            "\"93\"|\"93\"|\"93\"|\"93\"\n" +
            "\"94\"|\"94\"|\"94\"|\"94\"\n" +
            "\"95\"|\"95\"|\"95\"|\"95\"\n" +
            "\"96\"|\"96\"|\"96\"|\"96\"\n" +
            "\"97\"|\"97\"|\"97\"|\"97\"\n" +
            "\"98\"|\"98\"|\"98\"|\"98\"\n" +
            "\"99\"|\"99\"|\"99\"|\"99\"\n";


    private static final String expectedStrWithEmptyColumns = "\"0\"|||\"0\"|\"0\"\n" +
            "\"1\"|||\"1\"|\"1\"\n" +
            "\"2\"|||\"2\"|\"2\"\n" +
            "\"3\"|||\"3\"|\"3\"\n" +
            "\"4\"|||\"4\"|\"4\"\n" +
            "\"5\"|||\"5\"|\"5\"\n" +
            "\"6\"|||\"6\"|\"6\"\n" +
            "\"7\"|||\"7\"|\"7\"\n" +
            "\"8\"|||\"8\"|\"8\"\n" +
            "\"9\"|||\"9\"|\"9\"\n" +
            "\"10\"|||\"10\"|\"10\"\n" +
            "\"11\"|||\"11\"|\"11\"\n" +
            "\"12\"|||\"12\"|\"12\"\n" +
            "\"13\"|||\"13\"|\"13\"\n" +
            "\"14\"|||\"14\"|\"14\"\n" +
            "\"15\"|||\"15\"|\"15\"\n" +
            "\"16\"|||\"16\"|\"16\"\n" +
            "\"17\"|||\"17\"|\"17\"\n" +
            "\"18\"|||\"18\"|\"18\"\n" +
            "\"19\"|||\"19\"|\"19\"\n" +
            "\"20\"|||\"20\"|\"20\"\n" +
            "\"21\"|||\"21\"|\"21\"\n" +
            "\"22\"|||\"22\"|\"22\"\n" +
            "\"23\"|||\"23\"|\"23\"\n" +
            "\"24\"|||\"24\"|\"24\"\n" +
            "\"25\"|||\"25\"|\"25\"\n" +
            "\"26\"|||\"26\"|\"26\"\n" +
            "\"27\"|||\"27\"|\"27\"\n" +
            "\"28\"|||\"28\"|\"28\"\n" +
            "\"29\"|||\"29\"|\"29\"\n" +
            "\"30\"|||\"30\"|\"30\"\n" +
            "\"31\"|||\"31\"|\"31\"\n" +
            "\"32\"|||\"32\"|\"32\"\n" +
            "\"33\"|||\"33\"|\"33\"\n" +
            "\"34\"|||\"34\"|\"34\"\n" +
            "\"35\"|||\"35\"|\"35\"\n" +
            "\"36\"|||\"36\"|\"36\"\n" +
            "\"37\"|||\"37\"|\"37\"\n" +
            "\"38\"|||\"38\"|\"38\"\n" +
            "\"39\"|||\"39\"|\"39\"\n" +
            "\"40\"|||\"40\"|\"40\"\n" +
            "\"41\"|||\"41\"|\"41\"\n" +
            "\"42\"|||\"42\"|\"42\"\n" +
            "\"43\"|||\"43\"|\"43\"\n" +
            "\"44\"|||\"44\"|\"44\"\n" +
            "\"45\"|||\"45\"|\"45\"\n" +
            "\"46\"|||\"46\"|\"46\"\n" +
            "\"47\"|||\"47\"|\"47\"\n" +
            "\"48\"|||\"48\"|\"48\"\n" +
            "\"49\"|||\"49\"|\"49\"\n" +
            "\"50\"|||\"50\"|\"50\"\n" +
            "\"51\"|||\"51\"|\"51\"\n" +
            "\"52\"|||\"52\"|\"52\"\n" +
            "\"53\"|||\"53\"|\"53\"\n" +
            "\"54\"|||\"54\"|\"54\"\n" +
            "\"55\"|||\"55\"|\"55\"\n" +
            "\"56\"|||\"56\"|\"56\"\n" +
            "\"57\"|||\"57\"|\"57\"\n" +
            "\"58\"|||\"58\"|\"58\"\n" +
            "\"59\"|||\"59\"|\"59\"\n" +
            "\"60\"|||\"60\"|\"60\"\n" +
            "\"61\"|||\"61\"|\"61\"\n" +
            "\"62\"|||\"62\"|\"62\"\n" +
            "\"63\"|||\"63\"|\"63\"\n" +
            "\"64\"|||\"64\"|\"64\"\n" +
            "\"65\"|||\"65\"|\"65\"\n" +
            "\"66\"|||\"66\"|\"66\"\n" +
            "\"67\"|||\"67\"|\"67\"\n" +
            "\"68\"|||\"68\"|\"68\"\n" +
            "\"69\"|||\"69\"|\"69\"\n" +
            "\"70\"|||\"70\"|\"70\"\n" +
            "\"71\"|||\"71\"|\"71\"\n" +
            "\"72\"|||\"72\"|\"72\"\n" +
            "\"73\"|||\"73\"|\"73\"\n" +
            "\"74\"|||\"74\"|\"74\"\n" +
            "\"75\"|||\"75\"|\"75\"\n" +
            "\"76\"|||\"76\"|\"76\"\n" +
            "\"77\"|||\"77\"|\"77\"\n" +
            "\"78\"|||\"78\"|\"78\"\n" +
            "\"79\"|||\"79\"|\"79\"\n" +
            "\"80\"|||\"80\"|\"80\"\n" +
            "\"81\"|||\"81\"|\"81\"\n" +
            "\"82\"|||\"82\"|\"82\"\n" +
            "\"83\"|||\"83\"|\"83\"\n" +
            "\"84\"|||\"84\"|\"84\"\n" +
            "\"85\"|||\"85\"|\"85\"\n" +
            "\"86\"|||\"86\"|\"86\"\n" +
            "\"87\"|||\"87\"|\"87\"\n" +
            "\"88\"|||\"88\"|\"88\"\n" +
            "\"89\"|||\"89\"|\"89\"\n" +
            "\"90\"|||\"90\"|\"90\"\n" +
            "\"91\"|||\"91\"|\"91\"\n" +
            "\"92\"|||\"92\"|\"92\"\n" +
            "\"93\"|||\"93\"|\"93\"\n" +
            "\"94\"|||\"94\"|\"94\"\n" +
            "\"95\"|||\"95\"|\"95\"\n" +
            "\"96\"|||\"96\"|\"96\"\n" +
            "\"97\"|||\"97\"|\"97\"\n" +
            "\"98\"|||\"98\"|\"98\"\n" +
            "\"99\"|||\"99\"|\"99\"\n";
}
