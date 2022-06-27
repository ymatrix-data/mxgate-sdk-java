package cn.ymatrix.api;

import cn.ymatrix.logger.MxLogger;
import com.google.protobuf.ByteString;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * This is a gRPC Server which can hook the real backend server for Unit Test.
 * <p>
 * CREATE TABLE test_table(
 * ts timestamp
 * ,tag int NOT NULL
 * ,c1 float
 * ,c2 double precision
 * ,c3 text
 * )
 */
public class MxServerBackend {
    private static final Logger l = MxLogger.init(MxServerBackend.class);
    private final Server server;

    public static MxServerBackend getServerInstance(int port, JobMetadataHook hook1, SendDataHook hook2) {
        return new MxServerBackend(port, hook1, hook2);
    }

    public void startBlocking() {
        try {
            this.start();
            this.blockUntilShutdown();
        } catch (IOException e) {
            l.error("MxServerBackend start error 1 {}", e.getMessage());
            e.printStackTrace();
        } catch (InterruptedException e) {
            l.error("MxServerBackend start error 2 {}", e.getMessage());
            e.printStackTrace();
        }
    }

    private MxServerBackend(int port, JobMetadataHook hook1, SendDataHook hook2) {
        ServerBuilder<?> serverBuilder = ServerBuilder.forPort(port);
        InnerService service = new InnerService();
        if (hook1 != null) {
            service.registerJobMetadataHook(hook1);
        }
        if (hook2 != null) {
            service.registerSendDataHook(hook2);
        }
        server = serverBuilder.addService(service).build();
    }

    public void start() throws IOException {
        server.start();
        l.info("Server start, listening on {}", server.getPort());
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                l.error("*** shutting down gRPC server since JVM is shutting down");
                try {
                    MxServerBackend.this.stop();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                l.error("*** server shut down");
            }
        });
    }

    public void stop() throws InterruptedException {
        if (server != null) {
            server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
        }
    }

    private void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    private static class InnerService extends MXGateGrpc.MXGateImplBase {
        private JobMetadataHook hookMetadata;
        private SendDataHook hookSendData;

        InnerService() {

        }

        public void registerJobMetadataHook(JobMetadataHook hook) {
            this.hookMetadata = hook;
        }

        public void registerSendDataHook(SendDataHook hook) {
            this.hookSendData = hook;
        }

        @Override
        public void getJobMetadata(GetJobMetadata.Request request, StreamObserver<GetJobMetadata.Response> responseObserver) {
            responseObserver.onNext(this.getMetadata());
            responseObserver.onCompleted();
        }

        @Override
        public void sendData(SendData.Request request, StreamObserver<SendData.Response> responseObserver) {
            try {
                SendData.Response resp = this.sendData(request);
                responseObserver.onNext(resp);
                responseObserver.onCompleted();
            } catch (Exception e) {
                responseObserver.onError(Status.ABORTED.withDescription(e.getMessage()).withCause(e).asException());
                responseObserver.onError(e);
            }
        }

        private GetJobMetadata.Response getMetadata() {
            if (this.hookMetadata != null) {
                int responseCode = this.hookMetadata.getResponseCode();
                JobMetadata.Builder builder = JobMetadata.newBuilder();
                if (this.hookMetadata.getDelimiter() != null) {
                    builder.setDelimiter(this.hookMetadata.getDelimiter());
                }
                if (this.hookMetadata.getSchema() != null) {
                    builder.setSchema(this.hookMetadata.getSchema());
                }
                if (this.hookMetadata.getTable() != null) {
                    builder.setTable(this.hookMetadata.getTable());
                }
                for (int i = 0; i < this.hookMetadata.getMetadataHookList().size(); i++) {
                    MetadataHook metadataHook = this.hookMetadata.getMetadataHookList().get(i);
                    ColumnMeta.Builder columnMetaBuilder = ColumnMeta.newBuilder();
                    columnMetaBuilder.setNum(metadataHook.getNum());
                    if (metadataHook.getName() != null) {
                        columnMetaBuilder.setName(metadataHook.getName());
                    }
                    if (metadataHook.getType() != null) {
                        columnMetaBuilder.setType(metadataHook.getType());
                    }
                    ColumnMeta columnMeta = columnMetaBuilder.build();
                    builder.addColumns(columnMeta);
                }
                return GetJobMetadata.Response.newBuilder()
                        .setCode(responseCode)
                        .setMetadata(builder.build())
                        .build();
            }
            return getMetadataDefault();
        }

        private GetJobMetadata.Response getMetadataDefault() {
            ColumnMeta columnMeta1 = ColumnMeta.newBuilder().setNum(1).setName("ts").setType("timestamp").build();
            ColumnMeta columnMeta2 = ColumnMeta.newBuilder().setNum(2).setName("tag").setType("int").build();
            ColumnMeta columnMeta3 = ColumnMeta.newBuilder().setNum(3).setName("c1").setType("float").build();
            ColumnMeta columnMeta4 = ColumnMeta.newBuilder().setNum(4).setName("c2").setType("double precision").build();
            ColumnMeta columnMeta5 = ColumnMeta.newBuilder().setNum(5).setName("c3").setType("text").build();
            JobMetadata metadata = JobMetadata.newBuilder()
                    .setSchema("public")
                    .setTable("test_table")
                    .setDelimiter("|")
                    .addColumns(columnMeta1)
                    .addColumns(columnMeta2)
                    .addColumns(columnMeta3)
                    .addColumns(columnMeta4)
                    .addColumns(columnMeta5)
                    .build();
            GetJobMetadata.Response response = GetJobMetadata.Response.newBuilder().setCode(0).setMetadata(metadata).build();
            return response;
        }

        private SendData.Response sendData(SendData.Request request) {
            if (this.hookSendData == null) {
                return SendData.Response.newBuilder().setCode(MxGrpcClient.GRPC_RESPONSE_CODE_OK).build();
            }

            if (this.hookSendData.getSleepMillis() > 0 && request.getData().contains("slow")) {
                try {
                    Thread.sleep(this.hookSendData.getSleepMillis());
                } catch (Exception e) {
                    // do nothing
                }
            }

            if (this.hookSendData.getException() != null && request.getData().contains("exception")) {
                throw this.hookSendData.getException();
            }

            if (this.hookSendData.getDecoder() != null) {
                this.hookSendData.getDecoder().decode(request.getData());
            }

            SendData.Response.Builder builder = SendData.Response.newBuilder();
            if (this.hookSendData.getMessage() != null) {
                builder.setMsg(this.hookSendData.getMessage());
                builder.setMsgBytes(ByteString.copyFromUtf8(this.hookSendData.getMessage()));
            }
            if (this.hookSendData.getErrorLines() != null) {
                builder.putAllErrorLines(this.hookSendData.getErrorLines());
            }

            return builder.setCode(this.hookSendData.getResponseCode())
                    .setTupleCount(this.hookSendData.getTupleCount())
                    .build();
        }
    }

}
