package cn.ymatrix.load;

import cn.ymatrix.Main;
import cn.ymatrix.apiclient.DataPostListener;
import cn.ymatrix.apiclient.MxClient;
import cn.ymatrix.apiclient.Result;
import cn.ymatrix.builder.ConnectionListener;
import cn.ymatrix.builder.MxBuilder;
import cn.ymatrix.builder.RequestType;
import cn.ymatrix.data.Tuple;
import cn.ymatrix.logger.LoggerLevel;
import cn.ymatrix.logger.MxLogger;
import org.slf4j.Logger;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class TestDataLoading {
    private static Logger l;
    private static final String gRPCHostSDW2 = "172.16.100.12:8087";
    private static final String httpHostSDW2 = "http://172.16.100.12:8086/";
    private static final String CUSTOMER_LOG_TAG = "[>>>CUSTOMER<<<] ";
//    private static MxBuilder mxBuilder;
    private static int httpConcurrency;
    private static int tupleProducerConcurrency;
    private static int tupleProduceDelayMillis;
    private static int enoughLines = 40;
    private static int enoughBytes = 40000;
    private static int flushMode = 1; // Lines mode;
    private static int requestType = 1; // By default, HTTP type.
    private static int maxTuplePoolSize = 0;

    public static void main(String[] args) {
        httpConcurrency = Integer.parseInt(args[0]);
        tupleProducerConcurrency = Integer.parseInt(args[1]);
        tupleProduceDelayMillis = Integer.parseInt(args[2]);
        enoughLines = Integer.parseInt(args[3]);
        enoughBytes = Integer.parseInt(args[4]);
        flushMode = Integer.parseInt(args[5]);
        if (flushMode != 1 && flushMode != 2) {
            System.out.println("Invalid flush mode, must be 1 or 2, 1 for lines and 2 for bytes");
        }
        requestType = Integer.parseInt(args[6]);
        if (requestType != 1 && requestType != 2) { // 1 for http, 2 for grpc.
            System.out.println("Invalid request type, must be 1 or 2, 1 for http and 2 for grpc");
        }
        maxTuplePoolSize = Integer.parseInt(args[7]);

        MxBuilder.instance();

        initBuilder(httpConcurrency);
        dataLoad();
    }

    public static void dataLoad() {
        Thread thread = new Thread() {
            @Override
            public void run() {
                startClientsBlocking(gRPCHostSDW2, "public", "test_table_transfer", tupleProducerConcurrency, 10000000);
            }
        };
        thread.start();
    }

    private static void initBuilder(int concurrency) {
        MxLogger.loggerLevel(LoggerLevel.INFO);
        MxLogger.writeToFile("./sdk_log/mxgate_sdk_java.log");
        l = MxLogger.init(Main.class);
        MxBuilder.Builder builder = MxBuilder.newBuilder()
                .withDropAll(false)
                .withCacheCapacity(1000000)
                .withCacheEnqueueTimeout(10000)
                .withConcurrency(concurrency)
                .withRequestTimeoutMillis(1000000)
                .withMaxRequestQueued(4096)
                .withMaxRetryAttempts(3)
                .withRequestAsync(true)
                .withCSVConstructionParallel(100)
                .withRetryWaitDurationMillis(3000);
        switch (requestType) {
            case 1:
                builder.withRequestType(RequestType.WithHTTP);
                break;
            case 2:
                builder.withRequestType(RequestType.WithGRPC);
                break;
            default:
                l.error("Invalid request type, must be 1 or 2");
                return;
        }
        builder.build();
    }

    private static void startClientsBlocking(final String host, final String schema, final String table, int threads, int cycleTimes) {
        for (int i = 0; i < threads; i++) {
            Thread thread = new Thread() {
                @Override
                public void run() {
                    super.run();
                    try {
                        if (requestType == 1) {
                            sendData(generateMxClientBlocking(httpHostSDW2, host, schema, table, "|"), cycleTimes);
                        } else if (requestType == 2) {
                            sendData(generateMxClientBlocking(gRPCHostSDW2, host, schema, table, "|"), cycleTimes);
                        }
                    } catch (Exception e) {
                        l.error("MxClient init error: {}", e.getMessage());
                        e.printStackTrace();
                    }
                }
            };
            thread.start();
        }
    }

    private static MxClient generateMxClientAsync(String dataSendingHost, String metadataHost, String schema, String table, String delimiter) {
        CountDownLatch latch = new CountDownLatch(1);
        final MxClient[] retClient = {null};
//        mxBuilder.connect(dataSendingHost, metadataHost, schema, table, new ConnectionListener() {
//            @Override
//            public void onSuccess(MxClient client) {
//                retClient[0] = client;
//                latch.countDown();
//            }
//
//            @Override
//            public void onFailure(String failureMsg) {
//
//            }
//        });

//        mxBuilder.connect(dataSendingHost, metadataHost, schema, table, new ConnectionListener() {
//            @Override
//            public void onSuccess(MxClient client) {
//                retClient[0] = client;
//                latch.countDown();
//            }
//
//            @Override
//            public void onFailure(String failureMsg) {
//
//            }
//        }, 1, 1000, 3000);

//        mxBuilder.connectWithGroup(dataSendingHost, metadataHost, schema, table, new ConnectionListener() {
//            @Override
//            public void onSuccess(MxClient client) {
//                retClient[0] = client;
//                latch.countDown();
//            }
//
//            @Override
//            public void onFailure(String failureMsg) {
//
//            }
//        }, 1, 1000, 3000, 1);

        MxBuilder.instance().connectWithGroup(dataSendingHost, metadataHost, schema, table, new ConnectionListener() {
            @Override
            public void onSuccess(MxClient client) {
                retClient[0] = client;
                latch.countDown();
            }

            @Override
            public void onFailure(String failureMsg) {

            }
        }, 1);

        try {
            latch.await(3, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        return retClient[0];
    }

    private static MxClient generateMxClientBlocking(String dataSendingHost, String metadataHost, String schema, String table, String delimiter) {
//        MxClient client = mxBuilder.connect(dataSendingHost, metadataHost, schema, table);
//        MxClient client = mxBuilder.connect(dataSendingHost, metadataHost, schema, table, 10, 1000, 3000);
//        MxClient client = mxBuilder.connectWithGroup(dataSendingHost, metadataHost, schema, table, 1, 1000, 3000, 10);
//        MxClient client = mxBuilder.connectWithGroup(dataSendingHost, metadataHost, schema, table, 10);
//        MxClient client = mxBuilder.skipConnect(dataSendingHost, metadataHost, schema, table, delimiter);
//        MxClient client = mxBuilder.skipConnect(dataSendingHost, metadataHost, schema, table, delimiter, 1, 1000, 3000);
//        MxClient client = mxBuilder.skipConnectWithGroup(dataSendingHost, metadataHost, schema, table, delimiter, 1, 1000, 3000, 1);
        MxClient client = MxBuilder.instance().skipConnectWithGroup(dataSendingHost, metadataHost, schema, table, delimiter,1);
        return client;
    }

    private static void sendData(MxClient client, int cycleTimes) {
        l.info("Get MxClient success, begin to send data.");
        if (client != null) {
            // client.withIntervalToFlushMillis(5000);
            switch (flushMode) {
                case 1:
                    client.withEnoughLinesToFlush(enoughLines);
                    break;
                case 2:
                    client.withEnoughBytesToFlush(enoughBytes);
                    break;
                default:
                    l.error("Invalid flush mode: {}", flushMode);
                    return;
            }
            if (maxTuplePoolSize > 0) {
                client.useTuplesPool(maxTuplePoolSize);
            }
            client.withCompress();
//            client.withBase64Encode4Compress();
            client.withCSVConstructionBatchSize(10);
            client.registerDataPostListener(new DataPostListener() {
                @Override
                public void onSuccess(Result result) {

                }

                @Override
                public void onFailure(Result result) {
                    l.error(CUSTOMER_LOG_TAG + "Send tuples fail msg: {} lines: {}", result.getMsg(), result.getSucceedLines());
                    l.error(CUSTOMER_LOG_TAG + "Send tuples fail error tuples: {}", result.getErrorTuplesMap());
                    for (Map.Entry<Tuple, String> entry : result.getErrorTuplesMap().entrySet()) {
                        l.error(CUSTOMER_LOG_TAG + "error tuple of table={}, tuple={}, reason={}", entry.getKey().getTableName(), entry.getKey(), entry.getValue());
                    }
                }
            });

            for (int i = 0; i < cycleTimes; i++) {
//                Tuple tuple1 = client.generateEmptyTuple();

                Tuple tuple1 = client.generateEmptyTupleLite();

                tuple1.addColumn("ts", nowDataTime());
                tuple1.addColumn("tag", randInt(20));
                tuple1.addColumn("c1", randDouble());
//                tuple1.addColumn("c1", "this is not double");
//                tuple1.addColumn("c2", "this is not double");
//                tuple1.addColumn("c3", "this is not double");
//                tuple1.addColumn("c4", "this is not double");
                tuple1.addColumn("c2", randDouble());
                tuple1.addColumn("c3", randDouble());
                tuple1.addColumn("c4", randDouble());
                tuple1.addColumn("c5", "CAP(“全容”或者“预测\"或者“简化”请其中一个)" + randString(20));
                tuple1.addColumn("c6", randString(20) + i);
                tuple1.addColumn("c7", randInt(30) + i);
                tuple1.addColumn("c8", randInt(40) + i);

                client.appendTuple(tuple1);

//                Tuple tuple2 = client.generateEmptyTuple();
//                tuple2.addColumn("ts", nowDataTime());
//                tuple2.addColumn("tag", randInt(20));
//                tuple2.addColumn("c1", randDouble());
//                tuple2.addColumn("c2", randDouble());
//                tuple2.addColumn("c3", randDouble());
//                tuple2.addColumn("c4", randDouble());
//                tuple2.addColumn("c5", "CAP(“全容”或者“预测\"或者“简化”请其中一个)" + randString(20));
//                tuple2.addColumn("c6", randInt(20));
//                tuple2.addColumn("c7", randDouble());
//                tuple2.addColumn("c8", randString(30));
//
//                Tuple tuple3 = client.generateEmptyTuple();
//                tuple3.addColumn("ts", nowDataTime());
//                tuple3.addColumn("tag", randInt(20));
//                tuple3.addColumn("c1", randDouble());
//                tuple3.addColumn("c2", randDouble());
//                tuple3.addColumn("c3", randDouble());
//                tuple3.addColumn("c4", randDouble());
//                tuple3.addColumn("c5", "CAP(“全容”或者“预测\"或者“简化”请其中一个)" + randString(20));
//                tuple3.addColumn("c6", randInt(20));
//                tuple3.addColumn("c7", randInt(30));
//                tuple3.addColumn("c8", randDouble());
//
//                Tuple tuple4 = client.generateEmptyTuple();
//                tuple4.addColumn("ts", nowDataTime());
//                tuple4.addColumn("tag", 102020030);
//                tuple4.addColumn("c1", randDouble());
//                tuple4.addColumn("c2", randDouble());
//                tuple4.addColumn("c3", randDouble());
//                tuple4.addColumn("c4", randDouble());
//                tuple4.addColumn("c5", "CAP(“全容”或者“预测\"或者“简化”请其中一个)" + randString(20));
//                tuple4.addColumn("c6", randString(20));
//                tuple4.addColumn("c7", randString(30));
//                tuple4.addColumn("c8", randString(40));

//                client.appendTuples(tuple1, tuple2, tuple3, tuple4);

//                try {
//                    if (client.appendTuplesBlocking(tuple1, tuple2, tuple3, tuple4)) {
//                        l.info("append tuples enough");
//                        client.flushBlocking();
//                    }
//                } catch (AllTuplesFailException e) {
//                    l.error("Tuples fail and catch the exception return.", e);
//                    return;
//                } catch (PartiallyTuplesFailException e) {
//                    l.error("Tuples fail and catch the exception continue.", e);
//                }

                try {
                    Thread.sleep(tupleProduceDelayMillis);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }

            }
        }
    }

    private static String randString(int length) {
        String str = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        Random random = new Random();
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < length; i++) {
            int number = random.nextInt(62);
            sb.append(str.charAt(number));
        }
        return sb.toString();
    }

    private static int randInt(int length) {
        return (int) (Math.random() * length);
    }

    private static double randDouble() {
        Random random = new Random();
        return random.nextDouble();
    }

    private static String nowDataTime() {
        Date date = new Date();
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return dateFormat.format(date);
    }


}
