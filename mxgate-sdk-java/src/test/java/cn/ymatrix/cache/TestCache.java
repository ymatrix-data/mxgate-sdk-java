package cn.ymatrix.cache;

import cn.ymatrix.data.Tuple;
import cn.ymatrix.data.Tuples;
import cn.ymatrix.data.TuplesTarget;
import cn.ymatrix.exception.EnqueueException;
import cn.ymatrix.logger.MxLogger;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class TestCache {
    Logger l = MxLogger.init(TestCache.class);

    /**
     * Make sure QueueCache instance is not null;
     * Make sure QueueCache is a singleton;
     */

    @Test
    public void TestCacheFactoryGetInstance() {
        Cache cache = CacheFactory.getCacheInstance();
        Assert.assertNotNull(cache);
        cache.clear();

        cache.offer(getTuples(1));
        Assert.assertFalse(cache.isEmpty());

        boolean exceptionHappen = false;
        try {
            Tuples tuples = cache.get();
            Assert.assertNotNull(tuples);
            Assert.assertEquals(tuples.size(), 1);
        } catch (InterruptedException e) {
            exceptionHappen = true;
            e.printStackTrace();
        }
        Assert.assertFalse(exceptionHappen);
        Assert.assertTrue(cache.isEmpty());
        cache.clear();
    }

    @Test
    public void TestCacheOfferTimeout() {
        Cache cache = QueueCache.getInstance(10, 1000);
        for (int i = 0; i < 10; i++) {
            Assert.assertTrue(cache.offer(getTuples(1)));
        }
        Assert.assertFalse(cache.offer(getTuples(1)));
        Assert.assertEquals(cache.size(), 10);
    }

    @Test
    public void TestQueueCache() {
        l.info("Test queue cache");
        QueueCache queueCache = QueueCache.getInstance();
        Assert.assertNotNull(queueCache);
        queueCache.clear();

        queueCache.offer(getTuples(1));
        Assert.assertFalse(queueCache.isEmpty());

        boolean exceptionHappen = false;
        try {
            Tuples tuples = queueCache.get();
            Assert.assertNotNull(tuples);
            Assert.assertEquals(tuples.size(), 1);
        } catch (InterruptedException e) {
            exceptionHappen = true;
            e.printStackTrace();
        }
        Assert.assertFalse(exceptionHappen);
        Assert.assertTrue(queueCache.isEmpty());
        queueCache.clear();
    }

    @Test(timeout = 3000)
    public void TestAddThenFetchSingleThread() {
        l.info("Queue add then fetch");
        QueueCache cache = QueueCache.getInstance();
        // Before use the cache, clear it.
        cache.clear();
        Assert.assertNotNull(cache);

        for (int i = 0; i < 5; i++) {
            cache.offer(getTuples(i));
        }

        int i = 0;
        int[] result = new int[5];
        boolean exceptionHappen = false;
        while (!cache.isEmpty()) {
            try {
                Tuples tuples = cache.get();
                Assert.assertNotNull(tuples);
                result[i] = tuples.size();
                i++;
            } catch (InterruptedException e) {
                exceptionHappen = true;
                e.printStackTrace();
            }
        }
        Assert.assertFalse(exceptionHappen);
        Assert.assertEquals(i, 5);
        for (int j = 0; j < result.length; j++) {
            Assert.assertEquals(j, result[j]);
        }

        // After each test, clear it.
        cache.clear();
    }

    @Test(timeout = 3000)
    public void TestAddThenFetchMultipleThreads() {
        l.info("Queue add then fetch with multiple threads");
        QueueCache cache = QueueCache.getInstance();
        Assert.assertNotNull(cache);
        // Before we use it, clear the cache.
        cache.clear();

        CountDownLatch latch = new CountDownLatch(10);
        AtomicInteger counter = new AtomicInteger();
        counter.set(0);

        int totalSize = 0;
        // Produce
        for (int i = 0; i < 5; i++) {
            CacheProducer producer = new CacheProducer(getTuples(i), latch, cache);
            producer.start();
            totalSize += i;
        }

        // Consume
        for (int i = 0; i < 5; i++) {
            CacheConsumer consumer = new CacheConsumer(counter, latch, cache);
            consumer.start();
        }

        boolean exceptionHappen = false;
        try {
            Assert.assertTrue(latch.await(2, TimeUnit.SECONDS));
            Assert.assertEquals(counter.get(), totalSize);
            l.info("counter : {}", counter.get());
        } catch (InterruptedException e) {
            exceptionHappen = true;
            e.printStackTrace();
        }
        Assert.assertFalse(exceptionHappen);
    }

    @Test
    public void TestRefuse() {
        Cache cache = CacheFactory.getCacheInstance();
        Assert.assertFalse(cache.isRefused());
        cache.setRefuse(true);
        Assert.assertTrue(cache.isRefused());
    }

    @Test
    public void TestCacheClear() {
        Cache cache = CacheFactory.getCacheInstance();
        Tuples tuples1 = getTuples(5);
        Tuples tuples2 = getTuples(5);
        Tuples tuples3 = getTuples(5);
        Tuples tuples4 = getTuples(5);
        Tuples tuples5 = getTuples(5);
        cache.offer(tuples1);
        cache.offer(tuples2);
        cache.offer(tuples3);
        cache.offer(tuples4);
        cache.offer(tuples5);
        Assert.assertEquals(5, cache.size());
        cache.clear();
        Assert.assertEquals(0, cache.size());
        Assert.assertTrue(cache.isEmpty());
    }

    @Test
    public void TestOfferWithException() {
        Cache cache = CacheFactory.getCacheInstance();
        Tuples tuples = getTuples(5);

        // Before set refused to true, this offer will be successful.
        cache.offer(tuples);
        cache.setRefuse(true);

        Tuples tuples2 = getTuples(5);
        boolean enqueueException = false;
        String exceptionMsg = "";
        try {
            cache.offer(tuples2);
        } catch (EnqueueException e) {
            enqueueException = true;
            exceptionMsg = e.getMessage();
        }

        Assert.assertTrue(enqueueException);
        Assert.assertEquals(exceptionMsg, "QueueCache has been set to refused of any new Tuples to enqueue.");
    }

    @Test
    public void TestCacheFactory() {
        Cache cache = CacheFactory.getCacheInstance();
        Assert.assertNotNull(cache);
        cache.offer(getTuples(1));
        Assert.assertEquals(1, cache.size());
    }

    @Test
    public void TestCacheEnqueue() {
        Cache cache = CacheFactory.getCacheInstance(1, 3000);
        Assert.assertTrue(cache.offer(getTuples(1)));
        // Cache is full and could not be added.
        Assert.assertFalse(cache.offer(getTuples(1)));
    }


    private synchronized Tuples getTuples(final int size) {
        return new Tuples() {
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

    private static class CacheConsumer extends Thread {

        Logger l = MxLogger.init(CacheConsumer.class);

        private AtomicInteger counter;
        private Cache cache;
        private CountDownLatch latch;

        public CacheConsumer(AtomicInteger counter, CountDownLatch latch, Cache cache) {
            this.counter = counter;
            this.cache = cache;
            this.latch = latch;
        }

        @Override
        public void run() {
            if (!this.cache.isEmpty()) {
                boolean exceptionHappen = false;
                try {
                    Tuples tuples = this.cache.get();
                    Assert.assertNotNull(tuples);
                    counter.addAndGet(tuples.size());
                    this.latch.countDown();
                } catch (InterruptedException e) {
                    exceptionHappen = true;
                    e.printStackTrace();
                }
                Assert.assertFalse(exceptionHappen);
            }
        }
    }

    private static class CacheProducer extends Thread {
        private Cache cache;
        private CountDownLatch latch;

        private Tuples tuples;

        public CacheProducer(Tuples tuples, CountDownLatch latch, Cache cache) {
            this.latch = latch;
            this.cache = cache;
            this.tuples = tuples;
        }

        @Override
        public void run() {
            cache.offer(tuples);
            latch.countDown();
        }
    }
}
