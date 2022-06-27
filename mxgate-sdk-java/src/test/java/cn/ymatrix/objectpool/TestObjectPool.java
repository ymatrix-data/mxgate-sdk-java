package cn.ymatrix.objectpool;

import cn.ymatrix.data.Column;
import cn.ymatrix.data.Tuple;
import cn.ymatrix.data.Tuples;
import cn.ymatrix.data.TuplesTarget;
import cn.ymatrix.logger.MxLogger;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class TestObjectPool {
    private static final Logger l = MxLogger.init(TestObjectPool.class);

    @Test
    public void TestTuplePoolBorrowNotEnough() {
        int poolSize = 1;

        // Get tuple from TuplePool.
        cn.ymatrix.objectpool.TuplePool tuplePool = new cn.ymatrix.objectpool.TuplePool(new TupleCreator() {
            @Override
            public Tuple createTuple() {
                return getEmptyTuple();
            }
        }, poolSize);

        CountDownLatch latch = new CountDownLatch(2);
        Thread thread = new Thread() {
            @Override
            public void run() {
                tuplePool.tupleBorrow();
                latch.countDown();
            }
        };
        thread.start();

        Thread thread2 = new Thread() {
            @Override
            public void run() {
                tuplePool.tupleBorrow();
                latch.countDown();
            }
        };
        thread2.start();

        try {
            // Only one thread can get the TuplePool.
            Assert.assertFalse(latch.await(3, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

    }

    @Test
    public void TestTuplesPoolNotEnough() {
        int poolSize = 1;

        // Get tuple from TuplePool.
        cn.ymatrix.objectpool.TuplesPool tuplesPool = new cn.ymatrix.objectpool.TuplesPool(new TuplesCreator() {
            @Override
            public Tuples createTuples() {
                return getEmptyTuples();
            }
        }, poolSize);

        CountDownLatch latch = new CountDownLatch(2);
        Thread thread = new Thread() {
            @Override
            public void run() {
                tuplesPool.tuplesBorrow();
                latch.countDown();
            }
        };
        thread.start();

        Thread thread2 = new Thread() {
            @Override
            public void run() {
                tuplesPool.tuplesBorrow();
                latch.countDown();
            }
        };
        thread2.start();

        try {
            // Only one thread can get the TuplePool.
            Assert.assertFalse(latch.await(3, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }


    @Test
    public void TestTuplePoolEnough() {
        int poolSize = 2;

        // Get tuple from TuplePool.
        cn.ymatrix.objectpool.TuplePool tuplePool = new cn.ymatrix.objectpool.TuplePool(new TupleCreator() {
            @Override
            public Tuple createTuple() {
                return getEmptyTuple();
            }
        }, poolSize);

        CountDownLatch latch = new CountDownLatch(2);
        Thread thread = new Thread() {
            @Override
            public void run() {
                tuplePool.tupleBorrow();
                latch.countDown();
            }
        };
        thread.start();

        Thread thread2 = new Thread() {
            @Override
            public void run() {
                tuplePool.tupleBorrow();
                latch.countDown();
            }
        };
        thread2.start();

        try {
            // All of these two threads can get tuple from the tuple pool.
            Assert.assertTrue(latch.await(3, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void TestTuplesPoolEnough() {
        int poolSize = 2;

        // Get tuple from TuplePool.
        cn.ymatrix.objectpool.TuplesPool tuplesPool = new cn.ymatrix.objectpool.TuplesPool(new TuplesCreator() {
            @Override
            public Tuples createTuples() {
                return getEmptyTuples();
            }
        }, poolSize);

        CountDownLatch latch = new CountDownLatch(2);
        Thread thread = new Thread() {
            @Override
            public void run() {
                tuplesPool.tuplesBorrow();
                latch.countDown();
            }
        };
        thread.start();

        Thread thread2 = new Thread() {
            @Override
            public void run() {
                tuplesPool.tuplesBorrow();
                latch.countDown();
            }
        };
        thread2.start();

        try {
            // All of these two threads can get tuples from the tuples pool.
            Assert.assertTrue(latch.await(3, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void TestGetTargetTupleFromPool() {
        int poolSize = 10;
        int tupleSerialNum = 12345;

        // Get tuple from TuplePool.
        TuplePool tuplePool = new TuplePool(new TupleCreator() {
            @Override
            public Tuple createTuple() {
                Tuple tuple = getEmptyTuple();
                tuple.setSerialNum(tupleSerialNum);
                return tuple;
            }
        }, poolSize);

        CountDownLatch latch = new CountDownLatch(poolSize);
        Thread thread = new Thread() {
            @Override
            public void run() {
                for (int i = 0; i < poolSize; i++) {
                    Tuple tuple = tuplePool.tupleBorrow();
                    if (tuple != null && tuple.getSerialNum() == tupleSerialNum) {
                        latch.countDown();
                    }
                }
            }
        };
        thread.start();

        try {
            // All of these two threads can get tuple from the tuple pool.
            Assert.assertTrue(latch.await(3, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void TestGetTargetTuplesFromPool() {
        int poolSize = 10;
        String senderId = "12345";

        // Get tuple from TuplePool.
        TuplesPool tuplesPool = new TuplesPool(new TuplesCreator() {
            @Override
            public Tuples createTuples() {
                Tuples tuples = getEmptyTuples();
                tuples.setSenderID(senderId);
                return tuples;
            }
        }, poolSize);

        CountDownLatch latch = new CountDownLatch(poolSize);
        Thread thread = new Thread() {
            @Override
            public void run() {
                for (int i = 0; i < poolSize; i++) {
                    Tuples tuples = tuplesPool.tuplesBorrow();
                    if (tuples != null && tuples.getSenderID().equals(senderId)) {
                        latch.countDown();
                    }
                }
            }
        };
        thread.start();

        try {
            // All of these two threads can get tuple from the tuple pool.
            Assert.assertTrue(latch.await(3, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private Tuple getEmptyTuple() {
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

    private Tuples getEmptyTuples() {
        return new Tuples() {
            String senderId;
            private static final int csvBatchSize = 10;

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
                return null;
            }

            @Override
            public int size() {
                return 0;
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
                return null;
            }

            @Override
            public void setSenderID(String senderID) {
                this.senderId = senderID;
            }

            @Override
            public String getSenderID() {
                return this.senderId;
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

}
