package api;

import api.matrix.supervisor.Program;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import api.matrix.supervisor.Program.*;

import java.util.concurrent.TimeUnit;

public class ProgramClient {
    private final ManagedChannel channel;

    private final ProgramGrpc.ProgramBlockingStub blockingStub;
    private final int timeoutMillis;

    public ProgramClient(String host, String port, int timeoutMillis) {
        this.channel = ManagedChannelBuilder.forTarget(String.format("%s:%s", host, port)).usePlaintext().build();
        this.blockingStub = ProgramGrpc.newBlockingStub(this.channel);
        this.timeoutMillis = timeoutMillis;
    }

    public void registerProgram(String user, String name, String cmd) throws Exception {
        try {
            RegisterProgram.Request request = RegisterProgram.Request.newBuilder()
                    .setName(name)
                    .setConfig(
                            ProgramConfig.newBuilder()
                                    .setUser(user)
                                    .setDirectory("%(ENV_MXHOME)s/bin")
                                    .setCommand("%(ENV_MXHOME)s/bin/"+cmd).build()
                    )
                    .setDaemon(DaemonConfig.newBuilder().setAutoStart(AutoStart.AUTO_START_YES)).
                    build();

            ProgramGrpc.ProgramBlockingStub s = this.blockingStub;
            if (this.timeoutMillis > 0) {
                s = s.withDeadlineAfter(this.timeoutMillis, TimeUnit.MILLISECONDS);
            }
            s.registerProgram(request);
        } catch (Exception e) {
            throw new Exception("error calling grpc registerProgram", e);
        }
    }

    public void unregisterProgram(String name) throws Exception {
        try {
            UnregisterProgram.Request request = UnregisterProgram.Request.newBuilder()
                    .setName(name).build();

            ProgramGrpc.ProgramBlockingStub s = this.blockingStub;
            if (this.timeoutMillis > 0) {
                s = s.withDeadlineAfter(this.timeoutMillis, TimeUnit.MILLISECONDS);
            }
            s.unregisterProgram(request);
        } catch (Exception e) {
            throw new Exception("error calling grpc unregisterProgram", e);
        }
    }
}
