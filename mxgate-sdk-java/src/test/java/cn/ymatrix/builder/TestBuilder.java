package cn.ymatrix.builder;

import cn.ymatrix.api.JobMetadataHook;
import cn.ymatrix.api.MetadataHook;
import cn.ymatrix.api.MxServerBackend;
import cn.ymatrix.api.SendDataHook;
import cn.ymatrix.apiclient.DataPostListener;
import cn.ymatrix.apiclient.MxClient;
import cn.ymatrix.apiclient.Result;
import cn.ymatrix.apiserver.MxServer;
import cn.ymatrix.cache.Cache;
import cn.ymatrix.cache.CacheFactory;
import cn.ymatrix.data.Tuple;
import cn.ymatrix.data.Tuples;
import cn.ymatrix.data.TuplesTarget;
import cn.ymatrix.exception.AllTuplesFailException;
import cn.ymatrix.exception.ClientClosedException;
import cn.ymatrix.logger.MxLogger;
import cn.ymatrix.utils.CSVConstructor;
import cn.ymatrix.worker.TuplesConsumer;
import cn.ymatrix.worker.TuplesSendingBlockingWorker;
import com.beust.ah.A;
import org.junit.Assert;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.OrderWith;
import org.junit.runners.MethodSorters;
import org.slf4j.Logger;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TestBuilder {
    Logger l = MxLogger.init(TestBuilder.class);
    private static final int CSVConstructParallel = 100;
    private static final int sdkConcurrency = 10;


    @Test(expected = IllegalStateException.class)
    public void Test_A_ABeforeBuild() {
        // Before build, the instance() throws IllegalStateException.
        MxBuilder.instance();
    }

    @Test
    public void Test_A_BuilderBuild() {
        MxBuilder.newBuilder()
                .withConcurrency(sdkConcurrency)
                .withRequestTimeoutMillis(3000)
                .withCircuitBreaker()
                .withMinimumNumberOfCalls(1000)
                .withSlidingWindowSize(100)
                .withFailureRateThreshold(50.0f)
                .withSlowCallDurationThresholdMillis(2000)
                .withSlowCallRateThreshold(75.0f)
                .withCSVConstructionParallel(CSVConstructParallel)
                .withRequestAsync(true)
                .build();
    }

    @Test
    public void Test_B_BuilderConnectWithGroup() {
        // Start a gRPC server for getJobMetadata.
        int port = 9910;
        String schema = "public";
        String table = "table";
        String delimiter = "|";
        String fakeServerUrlDataSending = "http://localhost:8087"; // No use for this test. http://localhost:8087
        String serverURLGRPC = "localhost:" + port;

        try {
            // Get client groups from MxBuilder.
            Field clientGroupsField = MxBuilder.class.getDeclaredField("clientGroups");
            Assert.assertNotNull(clientGroupsField);
            clientGroupsField.setAccessible(true);
            Map<Integer, MxClientGroup> clientGroupMap = (Map<Integer, MxClientGroup>) clientGroupsField.get(MxBuilder.instance());
            Assert.assertNotNull(clientGroupMap);

            // Start backend server.
            this.startGRPCServer(port, this.prepareNormalJobMetadata(schema, table, delimiter), null);

            // Test with MxClient group 1.
            int groupNum1 = 1;
            MxClientGroup group1 = clientGroupMap.get(groupNum1);
            Assert.assertNull(group1); // No MxClient in this group has been initialized.
            testWithGroupNum(fakeServerUrlDataSending, serverURLGRPC, schema, table, groupNum1, clientGroupMap);

            // Test with MxClient group 2.
            int groupNum2 = 2;
            MxClientGroup group2 = clientGroupMap.get(groupNum2);
            Assert.assertNull(group2); // No MxClient in this group has been initialized.
            testWithGroupNum(fakeServerUrlDataSending, serverURLGRPC, schema, table, groupNum2, clientGroupMap);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void Test_B_CacheSize() {
        Set<Cache> cacheSet = getCacheSetFromBuilder();
        // Add new Cache instance into the cacheSet of MxBuilder.
        int tuplesToAdd = 10;
        for (int i = 0; i < tuplesToAdd; i++) {
            Cache cache = CacheFactory.getCacheInstance();
            cache.offer(getTuples());
            cacheSet.add(cache);
        }
        Assert.assertEquals(tuplesToAdd, MxBuilder.instance().getTupleCacheSize());
    }

    private void testWithGroupNum(String fakeServerUrlDataSending, String serverURLGRPC, String schema, String table,
                                  int groupNum1, Map<Integer, MxClientGroup> groupMap) {
        // Connect with group
        MxClient client1 = MxBuilder.instance().connectWithGroup(fakeServerUrlDataSending, serverURLGRPC, schema, table, groupNum1);
        Assert.assertNotNull(client1);

        MxClientGroup clientGroup = groupMap.get(groupNum1);
        Assert.assertNotNull(clientGroup);

        Assert.assertEquals(clientGroup.getGroupNum(), groupNum1);
        Assert.assertNotNull(clientGroup.getClientList());
        Assert.assertTrue(findMxClient(clientGroup.getClientList(), client1));
        MxServerAndCacheValidation(clientGroup, client1);

        // Connect with group configuration.
        MxClient client2 = MxBuilder.instance().connectWithGroup(fakeServerUrlDataSending, serverURLGRPC, schema, table,
                10, 1000, 3000, groupNum1);
        Assert.assertNotNull(client2);
        Assert.assertTrue(findMxClient(clientGroup.getClientList(), client2));
        MxServerAndCacheValidation(clientGroup, client2);

        // Connect with group callback.
        MxClient client3 = connectWithGroupCallback(fakeServerUrlDataSending, serverURLGRPC, schema, table, groupNum1);
        Assert.assertTrue(findMxClient(clientGroup.getClientList(), client3));
        MxServerAndCacheValidation(clientGroup, client3);

        // Connect with group config callback.
        MxClient client4 = connectWithGroupConfigCallback(fakeServerUrlDataSending, serverURLGRPC, schema, table, groupNum1);
        Assert.assertTrue(findMxClient(clientGroup.getClientList(), client4));
        MxServerAndCacheValidation(clientGroup, client4);

        // Skip connect with group.
        MxClient client5 = MxBuilder.instance().skipConnectWithGroup(fakeServerUrlDataSending, serverURLGRPC, schema, table, "|", groupNum1);
        Assert.assertTrue(findMxClient(clientGroup.getClientList(), client5));
        MxServerAndCacheValidation(clientGroup, client5);

        // Skip connect with group config.
        MxClient client6 = MxBuilder.instance().skipConnectWithGroup(fakeServerUrlDataSending, serverURLGRPC, schema, table, "|",
                10, 1000, 3000, groupNum1);
        Assert.assertTrue(findMxClient(clientGroup.getClientList(), client6));
        MxServerAndCacheValidation(clientGroup, client6);
    }

    private MxClient connectWithGroupCallback(String fakeServerUrlDataSending, String serverURLGRPC,
                                              String schema, String table, int groupNum1) {
        final MxClient[] clientRet = {null};
        CountDownLatch latch = new CountDownLatch(1);
        MxBuilder.instance().connectWithGroup(fakeServerUrlDataSending, serverURLGRPC, schema, table,
                new ConnectionListener() {
                    @Override
                    public void onSuccess(MxClient client) {
                          clientRet[0] = client;
                          latch.countDown();
                    }

                    @Override
                    public void onFailure(String failureMsg) {

                    }
                }, groupNum1);
        try {
            latch.await(3, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        Assert.assertNotNull(clientRet[0]);
        return clientRet[0];
    }

    private MxClient connectWithGroupConfigCallback(String fakeServerUrlDataSending, String serverURLGRPC,
                                              String schema, String table, int groupNum1) {
        final MxClient[] clientRet = {null};
        CountDownLatch latch = new CountDownLatch(1);
        MxBuilder.instance().connectWithGroup(fakeServerUrlDataSending, serverURLGRPC, schema, table,
                new ConnectionListener() {
                    @Override
                    public void onSuccess(MxClient client) {
                        clientRet[0] = client;
                        latch.countDown();
                    }

                    @Override
                    public void onFailure(String failureMsg) {

                    }
                }, 10, 1000, 3000, groupNum1);
        try {
            latch.await(3, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        Assert.assertNotNull(clientRet[0]);
        return clientRet[0];
    }

    private void MxServerAndCacheValidation(MxClientGroup group, MxClient client) {
        Assert.assertEquals(fetchMxServerFromMxClient(client), fetchMxServerFromClientGroup(group));
        Assert.assertEquals(fetchCacheFromMxClient(client), fetchCacheFromClientGroup(group));
        Assert.assertTrue(findInCacheSetOfMxBuilder(fetchCacheFromClientGroup(group)));
        Assert.assertTrue(findInCacheSetOfMxBuilder(fetchCacheFromMxClient(client)));
        Assert.assertTrue(findInServerCacheOfMxBuilder(fetchMxServerFromClientGroup(group)));
        Assert.assertTrue(findInServerCacheOfMxBuilder(fetchMxServerFromMxClient(client)));
    }

    private boolean findInCacheSetOfMxBuilder(Cache cache) {
        Set<Cache> cacheSet = getCacheSetFromBuilder();
        for(Cache c : cacheSet) {
            if (c.equals(cache)) {
                return true;
            }
        }
        return false;
    }

    private boolean findInServerCacheOfMxBuilder(MxServer server) {
        Set<MxServer> serverSet = getServerSetFromBuilder();
        for (MxServer s : serverSet) {
            if (s.equals(server)) {
                return true;
            }
        }
        return false;
    }

    private Set<Cache> getCacheSetFromBuilder() {
        try {
            Field cacheFiled = MxBuilder.instance().getClass().getDeclaredField("cacheSet");
            Assert.assertNotNull(cacheFiled);
            cacheFiled.setAccessible(true);
            Set<Cache> cacheSet = (Set<Cache>) cacheFiled.get(MxBuilder.instance());
            Assert.assertNotNull(cacheSet);
            return cacheSet;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Set<MxServer> getServerSetFromBuilder() {
        try {
            Field serverSetFiled = MxBuilder.instance().getClass().getDeclaredField("serverSet");
            Assert.assertNotNull(serverSetFiled);
            serverSetFiled.setAccessible(true);
            Set<MxServer> serverSet = (Set<MxServer>) serverSetFiled.get(MxBuilder.instance());
            Assert.assertNotNull(serverSet);
            return serverSet;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private boolean findMxClient(List<MxClient> clientList, MxClient target) {
        for (MxClient client : clientList) {
            Assert.assertNotNull(client);
            if (client.equals(target)) {
                return true;
            }
        }
        return false;
    }

    private MxServer fetchMxServerFromMxClient(MxClient client) {
        try {
            Field serverField = client.getClass().getDeclaredField("server");
            Assert.assertNotNull(serverField);
            serverField.setAccessible(true);
            MxServer server = (MxServer) serverField.get(client);
            Assert.assertNotNull(server);
            return server;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Cache fetchCacheFromMxClient(MxClient client) {
        try {
            Field cacheField = client.getClass().getDeclaredField("cache");
            Assert.assertNotNull(cacheField);
            cacheField.setAccessible(true);
            Cache cache = (Cache) cacheField.get(client);
            Assert.assertNotNull(cache);
            return cache;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private MxServer fetchMxServerFromClientGroup(MxClientGroup group) {
        try {
            Field serverField = group.getClass().getDeclaredField("server");
            Assert.assertNotNull(serverField);
            serverField.setAccessible(true);
            MxServer server = (MxServer) serverField.get(group);
            Assert.assertNotNull(server);
            return server;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Cache fetchCacheFromClientGroup(MxClientGroup group) {
        try {
            Field cacheField = group.getClass().getDeclaredField("cache");
            Assert.assertNotNull(cacheField);
            cacheField.setAccessible(true);
            Cache cache = (Cache) cacheField.get(group);
            Assert.assertNotNull(cache);
            return cache;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void Test_B_BuilderParameters() {
        try {
            Field requestAsyncField = MxBuilder.instance().getClass().getDeclaredField("withRequestAsync");
            Assert.assertNotNull(requestAsyncField);
            requestAsyncField.setAccessible(true);

            boolean requestAsync = (boolean) requestAsyncField.get(MxBuilder.instance());
            Assert.assertTrue(requestAsync);

            Field serverConfigField = MxBuilder.instance().getClass().getDeclaredField("serverConfig");
            Assert.assertNotNull(serverConfigField);
            serverConfigField.setAccessible(true);

            ServerConfig serverConfig = (ServerConfig) serverConfigField.get(MxBuilder.instance());
            Assert.assertNotNull(serverConfig);
            Assert.assertTrue(serverConfig.useAsyncRequest());
            Assert.assertEquals(serverConfig.getCSVConstructionParallel(), CSVConstructParallel);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void Test_B_BuilderCSVConstructParallel() {
        try {
            Field csvConstructParallel = MxBuilder.instance().getClass().getDeclaredField("csvConstructionParallel");
            Assert.assertNotNull(csvConstructParallel);
            csvConstructParallel.setAccessible(true);
            Assert.assertEquals(csvConstructParallel.get(MxBuilder.instance()), CSVConstructParallel);

            MxClientImpl clientImpl = (MxClientImpl) MxBuilder.instance().skipConnect("server_url", "server_url", "public", "table", "|");
            Assert.assertNotNull(clientImpl);

            MxServer server = clientImpl.getServer();
            Assert.assertNotNull(server);

            Field field = server.getClass().getDeclaredField("serverConfig");
            Assert.assertNotNull(field);
            field.setAccessible(true);

            ServerConfig serverConfig2 = (ServerConfig) field.get(server);
            Assert.assertNotNull(serverConfig2);
            Assert.assertEquals(serverConfig2.getCSVConstructionParallel(), CSVConstructParallel);

            Field blockingSenderField = server.getClass().getDeclaredField("blockingSender");
            Assert.assertNotNull(blockingSenderField);
            blockingSenderField.setAccessible(true);
            TuplesSendingBlockingWorker blockingWorker = (TuplesSendingBlockingWorker) blockingSenderField.get(server);
            Assert.assertNotNull(blockingWorker);
            Field csvConstructorField = blockingWorker.getClass().getDeclaredField("constructor");
            Assert.assertNotNull(csvConstructorField);
            csvConstructorField.setAccessible(true);
            CSVConstructor csvConstructor = (CSVConstructor) csvConstructorField.get(blockingWorker);
            Assert.assertNotNull(csvConstructor);
            Assert.assertEquals(csvConstructor.getJoinPoolParallel(), CSVConstructParallel);

            Field consumersFiled = server.getClass().getDeclaredField("tuplesConsumers");
            Assert.assertNotNull(consumersFiled);
            consumersFiled.setAccessible(true);
            List<TuplesConsumer> consumers = (List<TuplesConsumer>) consumersFiled.get(server);
            Assert.assertNotNull(consumers);
            Assert.assertEquals(consumers.size(), sdkConcurrency);

            for (TuplesConsumer consumer : consumers) {
                Assert.assertNotNull(consumer);
                Field csvConstructorFieldInConsumer = consumer.getClass().getDeclaredField("csvConstructor");
                Assert.assertNotNull(csvConstructorFieldInConsumer);
                csvConstructorFieldInConsumer.setAccessible(true);
                CSVConstructor csvConstructorInConsumer = (CSVConstructor) csvConstructorFieldInConsumer.get(consumer);
                Assert.assertNotNull(csvConstructorInConsumer);
                Assert.assertEquals(csvConstructorInConsumer.getJoinPoolParallel(), CSVConstructParallel);
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void Test_B_BuilderOnlyCanBeBuiltOnce() {
        CountDownLatch latch = new CountDownLatch(2);
        Thread buildThread1 = new Thread() {
            @Override
            public void run() {
                // MxBuilder only can be build once, this will throw an exception.
                try {
                    MxBuilder.newBuilder().withConcurrency(10).build();
                } catch (IllegalStateException e) {
                    latch.countDown();
                }
            }
        };
        buildThread1.start();

        Thread buildThread2 = new Thread() {
            @Override
            public void run() {
                // MxBuilder only can be build once, this will throw an exception.
                try {
                    MxBuilder.newBuilder().withConcurrency(10).build();
                } catch (IllegalStateException e) {
                    latch.countDown();
                }
            }
        };
        buildThread2.start();

        try {
            Assert.assertTrue(latch.await(3, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void Test_B_MxBuilderGetCacheSize() {
        try {
            Field cacheField = MxBuilder.instance().getClass().getDeclaredField("cacheSet");
            Assert.assertNotNull(cacheField);
            cacheField.setAccessible(true);
            Set<Cache> tupleCacheSet = (Set<Cache>) cacheField.get(MxBuilder.instance());
            Assert.assertNotNull(tupleCacheSet);
            int listSize = 0;
            for (Cache cache : tupleCacheSet) {
                Assert.assertNotNull(cache);
                listSize += cache.size();
            }
            Assert.assertEquals(listSize, MxBuilder.instance().getTupleCacheSize());
        } catch (Exception e) {
            l.error("Test MxBuilder get cache size exception", e);
            throw new RuntimeException(e);
        }
    }


    @Test
    public void Test_B_MxBuilderPauseResume() {
        // Start a gRPC server for getJobMetadata.
        int port = 8000;
        String schema = "public";
        String table = "table";
        String delimiter = "|";
        String fakeServerUrlDataSending = "http://localhost:8087"; // No use for this test. http://localhost:8087
        try {
            this.startGRPCServer(port, this.prepareNormalJobMetadata(schema, table, delimiter), null);
            MxClient client = MxBuilder.instance().connect(fakeServerUrlDataSending, "localhost:" + port, schema, table);
            Assert.assertNotNull(client);
            l.info("Get MxClient instance.");

            Tuple tuple1 = client.generateEmptyTuple();
            client.appendTuple(tuple1); // This is an empty tuple.
            Tuple tuple2 = client.generateEmptyTuple();

            MxBuilder.instance().pause();
            Assert.assertFalse(MxBuilder.instance().isShutdown());
            Assert.assertFalse(MxBuilder.instance().isTerminated());

            boolean appendTupleException = false;
            try {
                // Paused client could not be appended any new Tuples.
                client.appendTuple(tuple2);
            } catch (ClientClosedException e) {
                appendTupleException = true;
            }
            Assert.assertTrue(appendTupleException);

            // After pause, appendTuples API will throw exception.
            boolean appendTuplesException = false;
            try {
                client.appendTuples(tuple1, tuple2);
            } catch (ClientClosedException e) {
                appendTuplesException = true;
            }
            Assert.assertTrue(appendTuplesException);

            // After pause, appendTuplesList API will throw exception.
            boolean appendTuplesListException = false;
            List<Tuple> list = new ArrayList<>();
            list.add(tuple1);
            list.add(tuple2);
            try {
                client.appendTuplesList(list);
            } catch (ClientClosedException e) {
                appendTuplesListException = true;
            }
            Assert.assertTrue(appendTuplesListException);

            // After pause, append Tuples Blocking APIs will throw exception.
            boolean appendTupleBlockingException = false;
            try {
                client.appendTupleBlocking(tuple1);
            } catch (ClientClosedException e) {
                appendTupleBlockingException = true;
            }
            Assert.assertTrue(appendTupleBlockingException);

            boolean appendTuplesBlockingException = false;
            try {
                client.appendTuplesBlocking(tuple1, tuple2);
            } catch (ClientClosedException e) {
                appendTuplesBlockingException = true;
            }
            Assert.assertTrue(appendTuplesBlockingException);

            boolean appendTuplesListBlockingException = false;
            try {
                client.appendTuplesListBlocking(list);
            } catch (ClientClosedException e) {
                appendTuplesListBlockingException = true;
            }
            Assert.assertTrue(appendTuplesListBlockingException);

            // After pause, flush APIs will throw exception.
            boolean flushException = false;
            try {
                client.flush();
            } catch (ClientClosedException e) {
                flushException = true;
            }
            Assert.assertTrue(flushException);

            boolean flushBlockingException = false;
            try {
                client.flushBlocking();
            } catch (ClientClosedException e) {
                flushBlockingException = true;
            }
            Assert.assertTrue(flushBlockingException);

            boolean registerListenerException = false;
            try {
                client.registerDataPostListener(new DataPostListener() {
                    @Override
                    public void onSuccess(Result result) {

                    }

                    @Override
                    public void onFailure(Result result) {

                    }
                });
            } catch (ClientClosedException e) {
                registerListenerException = true;
            }
            Assert.assertTrue(registerListenerException);

            boolean withBytesToFlushException = false;
            try {
                client.withEnoughBytesToFlush(40000);
            } catch (ClientClosedException e) {
                withBytesToFlushException = true;
            }
            Assert.assertTrue(withBytesToFlushException);

            boolean withIntervalFlushException = false;
            try {
                client.withIntervalToFlushMillis(3000);
            } catch (ClientClosedException e) {
                withIntervalFlushException = true;
            }
            Assert.assertTrue(withIntervalFlushException);

            boolean getEmptyTuplesException = false;
            try {
                client.generateEmptyTuple();
            } catch (ClientClosedException e) {
                getEmptyTuplesException = true;
            }
            Assert.assertTrue(getEmptyTuplesException);

            MxBuilder.instance().resume();
            Assert.assertFalse(MxBuilder.instance().isShutdown());
            Assert.assertFalse(MxBuilder.instance().isTerminated());

            // After resume, these APIs can be used normally.
            client.withIntervalToFlushMillis(3000);
            client.withEnoughBytesToFlush(40000);
            client.generateEmptyTuple();
            client.appendTuple(tuple2);
            client.appendTuples(tuple1, tuple2);
            client.appendTuplesList(list);
            client.appendTupleBlocking(tuple1);
            client.appendTuplesBlocking(tuple1, tuple2);
            client.appendTuplesListBlocking(list);

            client.flush();

            boolean allTuplesFail = false;
            try {
                client.flushBlocking();
            } catch (IllegalArgumentException e) {
                // Catch this exception for the reason that the data sending URL is fake.
            } catch (AllTuplesFailException e) {
                l.error("AllTupleFailException has happened");
                allTuplesFail = true;
            }
            Assert.assertTrue(allTuplesFail);

        } catch (Exception e) {
            l.error("TestMxBuilderPauseResume exception", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * NOTE: exec this shutdown test in the last Test case for the reason that
     * MxBuilder's shutdown() only can be executed once and could not be restarted.
     */
    @Test
    public void Test_B_MxBuilderShutdown() {
        // Start a gRPC server for getJobMetadata.
        int port = 8001;
        String schema = "public";
        String table = "table_shutdown";
        String delimiter = "|";
        String fakeServerUrlDataSending = "http://localhost:8087"; // No use for this test. http://localhost:8087
        try {
            this.startGRPCServer(port, this.prepareNormalJobMetadata(schema, table, delimiter), null);
            MxClient client = MxBuilder.instance().connect(fakeServerUrlDataSending, "localhost:" + port, schema, table);
            Assert.assertNotNull(client);
            l.info("Get MxClient instance.");

            Tuple tuple1 = client.generateEmptyTuple();
            client.appendTuple(tuple1); // This is an empty tuple.
            Tuple tuple2 = client.generateEmptyTuple();

            List<Tuple> list = new ArrayList<>();
            list.add(tuple1);
            list.add(tuple2);

            // After resume, these APIs can be used normally.
            client.withIntervalToFlushMillis(3000);
            client.withEnoughBytesToFlush(40000);
            client.generateEmptyTuple();
            client.appendTuple(tuple2);
            client.appendTuples(tuple1, tuple2);
            client.appendTuplesList(list);
            client.appendTupleBlocking(tuple1);
            client.appendTuplesBlocking(tuple1, tuple2);
            client.appendTuplesListBlocking(list);

            client.flush();

            boolean allTuplesFailException = false;
            try {
                client.flushBlocking();
            } catch (IllegalArgumentException e) {
                // Catch this exception for the reason that the data sending URL is fake.
            } catch (AllTuplesFailException e) {
                allTuplesFailException = true;
            }
            Assert.assertTrue(allTuplesFailException);

            MxBuilder.instance().shutdownNow();
            Assert.assertTrue(MxBuilder.instance().isShutdown());

            boolean appendTupleException = false;
            try {
                // Paused client could not be appended any new Tuples.
                client.appendTuple(tuple2);
            } catch (ClientClosedException e) {
                appendTupleException = true;
            }
            Assert.assertTrue(appendTupleException);

            // After pause, appendTuples API will throw exception.
            boolean appendTuplesException = false;
            try {
                client.appendTuples(tuple1, tuple2);
            } catch (ClientClosedException e) {
                appendTuplesException = true;
            }
            Assert.assertTrue(appendTuplesException);

            // After pause, appendTuplesList API will throw exception.
            boolean appendTuplesListException = false;
            try {
                client.appendTuplesList(list);
            } catch (ClientClosedException e) {
                appendTuplesListException = true;
            }
            Assert.assertTrue(appendTuplesListException);

            // After pause, append Tuples Blocking APIs will throw exception.
            boolean appendTupleBlockingException = false;
            try {
                client.appendTupleBlocking(tuple1);
            } catch (ClientClosedException e) {
                appendTupleBlockingException = true;
            }
            Assert.assertTrue(appendTupleBlockingException);

            boolean appendTuplesBlockingException = false;
            try {
                client.appendTuplesBlocking(tuple1, tuple2);
            } catch (ClientClosedException e) {
                appendTuplesBlockingException = true;
            }
            Assert.assertTrue(appendTuplesBlockingException);

            boolean appendTuplesListBlockingException = false;
            try {
                client.appendTuplesListBlocking(list);
            } catch (ClientClosedException e) {
                appendTuplesListBlockingException = true;
            }
            Assert.assertTrue(appendTuplesListBlockingException);

            // After pause, flush APIs will throw exception.
            boolean flushException = false;
            try {
                client.flush();
            } catch (ClientClosedException e) {
                flushException = true;
            }
            Assert.assertTrue(flushException);

            boolean flushBlockingException = false;
            try {
                client.flushBlocking();
            } catch (ClientClosedException e) {
                flushBlockingException = true;
            }
            Assert.assertTrue(flushBlockingException);

            boolean registerListenerException = false;
            try {
                client.registerDataPostListener(new DataPostListener() {
                    @Override
                    public void onSuccess(Result result) {

                    }

                    @Override
                    public void onFailure(Result result) {

                    }
                });
            } catch (ClientClosedException e) {
                registerListenerException = true;
            }
            Assert.assertTrue(registerListenerException);

            boolean withBytesToFlushException = false;
            try {
                client.withEnoughBytesToFlush(40000);
            } catch (ClientClosedException e) {
                withBytesToFlushException = true;
            }
            Assert.assertTrue(withBytesToFlushException);

            boolean withIntervalFlushException = false;
            try {
                client.withIntervalToFlushMillis(3000);
            } catch (ClientClosedException e) {
                withIntervalFlushException = true;
            }
            Assert.assertTrue(withIntervalFlushException);

            boolean getEmptyTuplesException = false;
            try {
                client.generateEmptyTuple();
            } catch (ClientClosedException e) {
                getEmptyTuplesException = true;
            }
            Assert.assertTrue(getEmptyTuplesException);

        } catch (Exception e) {
            l.error("TestMxBuilderShutdown exception", e);
            throw new RuntimeException(e);
        }
    }

    private void startGRPCServer(int port, JobMetadataHook hook1, SendDataHook hook2) throws InterruptedException {
        MxServerBackend server = newMxServerBackendWithHook(port, hook1, hook2);
        startRPCServerBlocking(server);
        // Sleep some seconds to wait for the server to start.
        threadSleep(3000);
    }

    private void startRPCServerBlocking(final MxServerBackend gRPCServer) {
        if (gRPCServer != null) {
            new Thread() {
                @Override
                public void run() {
                    gRPCServer.startBlocking();
                }
            }.start();
        }
    }

    private void threadSleep(long millis) throws InterruptedException {
        Thread.sleep(millis);
    }

    private MxServerBackend newMxServerBackendWithHook(int port, JobMetadataHook hook1, SendDataHook hook2) {
        MxServerBackend gRPCServer = MxServerBackend.getServerInstance(port, hook1, hook2);
        l.info("new grpc server.");
        return gRPCServer;
    }

    private JobMetadataHook prepareNormalJobMetadata(String schema, String table, String delimiter) {
        JobMetadataHook.Builder builder = JobMetadataHook.newBuilder();
        builder.setDelimiter(delimiter);
        builder.setSchemaTable(schema, table);
        builder.addJobMetadataHook(MetadataHook.newBuilder().setNum(1).setType("timestamp").setName("ts").Build());
        builder.addJobMetadataHook(MetadataHook.newBuilder().setNum(2).setType("int").setName("tag").Build());
        builder.addJobMetadataHook(MetadataHook.newBuilder().setNum(3).setType("float").setName("c1").Build());
        builder.addJobMetadataHook(MetadataHook.newBuilder().setNum(4).setType("double precision").setName("c2").Build());
        builder.addJobMetadataHook(MetadataHook.newBuilder().setNum(5).setType("text").setName("c3").Build());
        builder.setResponseCode(0);
        return builder.build();
    }

    private Tuples getTuples() {
        return new Tuples() {

            private TuplesTarget target;

            private static final String delimiter = "|";
            private static final String schema = "public";
            private static final String table = "table";

            private static final int size = 4;

            private static final int csvBatchSize = 10;

            private Tuple generateTuple(String delimiter, String schema, String table) {
                return cn.ymatrix.builder.Utils.generateEmptyTupleLite(delimiter, schema, table);
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
                this.target = target;
            }

            @Override
            public TuplesTarget getTarget() {
                return this.target;
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
