package cn.ymatrix.load;

import cn.ymatrix.Main;
import cn.ymatrix.apiclient.DataPostListener;
import cn.ymatrix.apiclient.MxClient;
import cn.ymatrix.apiclient.Result;
import cn.ymatrix.builder.MxBuilder;
import cn.ymatrix.builder.RequestType;
import cn.ymatrix.data.Tuple;
import cn.ymatrix.exception.AllTuplesFailException;
import cn.ymatrix.exception.PartiallyTuplesFailException;
import cn.ymatrix.logger.LoggerLevel;
import cn.ymatrix.logger.MxLogger;
import org.slf4j.Logger;

import java.util.Map;

public class TestExclude {
    private static Logger l;
    private static final String gRPCHostSDW2 = "172.16.100.12:8087";
    private static final String httpHostSDW2 = "http://172.16.100.12:8086/";
    private static final String CUSTOMER_LOG_TAG = "[>>>CUSTOMER<<<] ";
    private static MxBuilder mxBuilder;
    private static int httpConcurrency;
    private static int tupleProducerConcurrency;
    private static int tupleProduceDelayMillis;
    private static int enoughLines = 40;
    private static int enoughBytes = 40000;
    private static int mode = 1; // Lines mode;
    private static int requestType = 1; // By default, HTTP type.

    private static int maxTuplePoolSize = 200000;
    private static int maxTuplesPoolSize = 20000;

    public static void main(String[] args) {
        httpConcurrency = Integer.parseInt(args[0]);
        tupleProducerConcurrency = Integer.parseInt(args[1]);
        tupleProduceDelayMillis = Integer.parseInt(args[2]);
        enoughLines = Integer.parseInt(args[3]);
        enoughBytes = Integer.parseInt(args[4]);
        mode = Integer.parseInt(args[5]);
        if (mode != 1 && mode != 2) {
            System.out.println("Invalid flush mode, must be 1 or 2");
        }
        requestType = Integer.parseInt(args[6]);
        if (requestType != 1 && requestType != 2) {
            System.out.println("Invalid request type, must be 1 or 2");
        }
        maxTuplePoolSize = Integer.parseInt(args[7]);
        maxTuplesPoolSize = Integer.parseInt(args[8]);
        dataLoad();
    }

    public static void dataLoad() {
        Thread thread = new Thread() {
            @Override
            public void run() {
                initBuilder(httpConcurrency);
                startClientsBlocking(gRPCHostSDW2, "public", "test_exclude", tupleProducerConcurrency, 10);
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

        mxBuilder = builder.build();
    }

    private static void startClientsBlocking(final String host, final String schema, final String table, int threads, int cycleTimes) {
        for (int i = 0; i < threads; i++) {
            Thread thread = new Thread() {
                @Override
                public void run() {
                    super.run();
                    try {
                        if (requestType == 1) {
                            MxClient client = mxBuilder.connect(httpHostSDW2, host, schema, table);
                            sendData(client, cycleTimes);
                        } else if (requestType == 2) {
                            MxClient client = mxBuilder.connect(gRPCHostSDW2, host, schema, table);
                            sendData(client, cycleTimes);
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

    private static void sendData(MxClient client, int cycleTimes) {
        l.info("Get MxClient success, begin to send data.");
        if (client != null) {
//            client.withIntervalToFlushMillis(5000);
            switch (mode) {
                case 1:
                    client.withEnoughLinesToFlush(enoughLines);
                    break;
                case 2:
                    client.withEnoughBytesToFlush(enoughBytes);
                    break;
                default:
                    l.error("Invalid flush mode: {}", mode);
                    return;
            }
            client.useTuplesPool(1000);
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
                Tuple tuple1 = client.generateEmptyTuple();
                tuple1.addColumn("id", i);
//                client.appendTuples(tuple1);

                try {
                    if (client.appendTuplesBlocking(tuple1)) {
                        l.info("append tuples enough");
                        client.flushBlocking();
                    }
                } catch (AllTuplesFailException e) {
                    l.error("Tuples fail and catch the exception return.", e);
                    return;
                } catch (PartiallyTuplesFailException e) {
                    l.error("Tuples fail and catch the exception continue.", e);
                }

                try {
                    Thread.sleep(tupleProduceDelayMillis);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }

            }
        }
    }



}
