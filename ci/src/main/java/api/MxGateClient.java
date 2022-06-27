package api;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import api.matrix.mxgate.Mxgate.*;
import java.util.concurrent.TimeUnit;

public class MxGateClient {
    private final ManagedChannel channel;

    private final MXGateGrpc.MXGateBlockingStub blockingStub;
    private final int timeoutMillis;

    public MxGateClient(String host, int port, int timeoutMillis) {
        this.channel = ManagedChannelBuilder.forTarget(String.format("%s:%d", host, port)).usePlaintext().build();
        this.blockingStub = MXGateGrpc.newBlockingStub(this.channel);
        this.timeoutMillis = timeoutMillis;
    }

    public GetMxGateStatus.Response getStatus() throws Exception {
        try {
            MXGateGrpc.MXGateBlockingStub s = this.blockingStub;
            if (this.timeoutMillis > 0) {
                s = s.withDeadlineAfter(this.timeoutMillis, TimeUnit.MILLISECONDS);
            }
            return s.getMxGateStatus(com.google.protobuf.Empty.getDefaultInstance());
        } catch (Exception e) {
            throw new Exception("error calling grpc registerProgram", e);
        }
    }
}
