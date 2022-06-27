package cn.ymatrix.builder;

import cn.ymatrix.apiclient.DataPostListener;
import cn.ymatrix.apiclient.MxClient;
import cn.ymatrix.data.Tuple;
import cn.ymatrix.logger.MxLogger;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

public class TestScheduledFlushTask {
    Logger l = MxLogger.init(TestScheduledFlushTask.class);


    private static class TestStateHolder {
        TestStateHolder() {
        }

        boolean invoked;

        public boolean isInvoked() {
            return invoked;
        }

        public void setInvoked(boolean invoked) {
            this.invoked = invoked;
        }
    }

    @Test
    public void NewScheduledFlushTask() {
        try {
            String className = "cn.ymatrix.builder.MxClientImpl$ScheduledFlushTask";
            Class<?> innerClazz = Class.forName(className);
            Constructor<?> constructor = innerClazz.getDeclaredConstructor(MxClient.class);
            constructor.setAccessible(true);
            TestStateHolder holder = new TestStateHolder();
            holder.setInvoked(false);
            Object o = constructor.newInstance(prepareEmptyMxClient(holder));
            Assert.assertEquals(className, o.getClass().getName());
            Method method = o.getClass().getDeclaredMethod("run");
            Assert.assertNotNull(method);
            method.setAccessible(true);
            // Invoke the run method.
            method.invoke(o);
            Assert.assertTrue(holder.isInvoked());
        } catch (ClassNotFoundException | NoSuchMethodException | InstantiationException | IllegalAccessException |
                 InvocationTargetException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    private MxClient prepareEmptyMxClient(final TestStateHolder holder) {
        return new MxClient() {

            public boolean isFree() {
                return false;
            }

            @Override
            public void appendTuple(Tuple tuple) {

            }

            @Override
            public boolean appendTupleBlocking(Tuple tuple) {
                return false;
            }

            @Override
            public void appendTuples(Tuple... tuples) {

            }

            @Override
            public boolean appendTuplesBlocking(Tuple... tuples) {
                return false;
            }

            @Override
            public void flushBlocking() {

            }

            @Override
            public void appendTuplesList(List<Tuple> tuples) {

            }

            @Override
            public boolean appendTuplesListBlocking(List<Tuple> tuples) {
                return false;
            }

            @Override
            public Tuple generateEmptyTuple() {
                return null;
            }

            @Override
            public Tuple generateEmptyTupleLite() {
                return null;
            }

            @Override
            public void flush() {
                holder.setInvoked(true);
            }

            @Override
            public void registerDataPostListener(DataPostListener listener) {

            }

            @Override
            public void withEnoughBytesToFlush(long bytesLimitation) {

            }

            @Override
            public void withIntervalToFlushMillis(long intervalMillis) {

            }

            @Override
            public void withEnoughLinesToFlush(int lines) {

            }

            @Override
            public void withCSVConstructionBatchSize(int size) {

            }

            @Override
            public void useTuplesPool(int size) {

            }

            @Override
            public void setDelimiter(String delimiter) {

            }

            @Override
            public String getDelimiter() {
                return null;
            }

            @Override
            public void setSchema(String schema) {

            }

            @Override
            public String getSchema() {
                return null;
            }

            @Override
            public void setTable(String table) {

            }

            @Override
            public String getTable() {
                return null;
            }

            @Override
            public void withCompress() {

            }

            @Override
            public void withoutCompress() {

            }

            @Override
            public void withBase64Encode4Compress() {

            }

            @Override
            public void withoutBase64EncodeCompress() {

            }

            @Override
            public String getClientName() {
                return null;
            }
        };
    }

}
