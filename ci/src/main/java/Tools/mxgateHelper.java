package Tools;

import api.*;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.ServerSocket;

public class mxgateHelper {
    final String masterHost;

    final String dbPort;
    final String dbUser;

    final int timeoutMillis;

    final String mxgateTag;

    final int mxgateRpcPort;
    final String mxgateHttpPort;

    final ProgramClient client;

    public mxgateHelper(String host, String dbPort, String dbUser, String supervisorRpcPort, String mxgateHttpPort, int timeoutMillis) {
        this.masterHost = host;
        this.dbPort = dbPort;
        this.dbUser = dbUser;
        this.mxgateRpcPort = getAvailablePort();
        this.mxgateHttpPort = mxgateHttpPort;
        this.timeoutMillis = timeoutMillis;

        String pid = ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
        this.mxgateTag = String.format("mxgate-%s", pid);
        this.client = new ProgramClient(this.masterHost, supervisorRpcPort, this.timeoutMillis);
    }

    private int getAvailablePort() {
        try {
            ServerSocket serverSocket = new ServerSocket(0);
            int localPort = serverSocket.getLocalPort();

            serverSocket.close();
            return localPort;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return 8087; // default
    }

    public String getMasterHost() {return masterHost;}
    public String getDbPort() {return dbPort;}

    public String getDbUser() {return dbUser;}

    public int getMxgateRpcPort() {
        return mxgateRpcPort;
    }

    public String getMxgateHttpPort() {
        return mxgateHttpPort;
    }

    public void start(String cfgStr) throws Exception {
        this.client.registerProgram(getDbUser(), this.mxgateTag, String.format("mxgated %s", cfgStr));
    }

    public void startHttp(final String dbName, final String schema, final String tableName) throws Exception {
        this.client.registerProgram(getDbUser(), this.mxgateTag, String.format("mxgated %s", generateHTTPCmdCfg(dbName, schema, tableName)));
    }

    public void startHttp(final String dbName, final String schema, final String tableName, final String timeFmt) throws Exception {
        this.client.registerProgram(getDbUser(), this.mxgateTag, String.format("mxgated %s", generateHTTPCmdCfg(dbName, schema, tableName, timeFmt)));
    }

    public void startHttp(final String dbName, final String schema, final String tableName, int maxBody, int reqTimeout, final String timeFmt) throws Exception {
        this.client.registerProgram(getDbUser(), this.mxgateTag, String.format("mxgated %s", generateHTTPCmdCfg(dbName, schema, tableName, maxBody, reqTimeout, timeFmt)));
    }

    public void startGrpc(final String dbName, final String schema, final String tableName) throws Exception {
        this.client.registerProgram(getDbUser(), this.mxgateTag, String.format("mxgated %s", generateGRPCCmdCfg(dbName, schema, tableName)));
    }

    public void startGrpc(final String dbName, final String schema, final String tableName, final String timeFmt) throws Exception {
        this.client.registerProgram(getDbUser(), this.mxgateTag, String.format("mxgated %s", generateGRPCCmdCfg(dbName, schema, tableName, timeFmt)));
    }

    public void startGrpc(final String dbName, final String schema, final String tableName, final int reqTimeout) throws Exception {
        this.client.registerProgram(getDbUser(), this.mxgateTag, String.format("mxgated %s", generateGRPCCmdCfg(dbName, schema, tableName, reqTimeout)));
    }

    public void startGrpc(final String dbName, final String schema, final String tableName, int maxBody, int reqTimeout, final String timeFmt) throws Exception {
        this.client.registerProgram(getDbUser(), this.mxgateTag, String.format("mxgated %s", generateGRPCCmdCfg(dbName, schema, tableName, maxBody, reqTimeout, timeFmt)));
    }

    public api.matrix.mxgate.Mxgate.GetMxGateStatus.Response getStatus() throws Exception {
        return new MxGateClient(this.masterHost, this.mxgateRpcPort, this.timeoutMillis).getStatus();
    }

    public void foreStop() throws Exception{
        this.client.unregisterProgram(this.mxgateTag);
    }

    public String generateHTTPCmdCfg(final String dbName, final String schema, final String tableName) {
        return String.format("--db-database %s --db-master-host %s --db-master-port %s " +
                "--source http --http-port %s --max-body-bytes 3000000 --request-timeout 500 " +
                "--format csv --time-format raw --target %s.%s --grpc-port %d --allow-dynamic --debug",
                dbName, this.masterHost, this.dbPort,
                getMxgateHttpPort(), schema, tableName, getMxgateRpcPort());
    }

    public String generateHTTPCmdCfg(final String dbName, final String schema, final String tableName, final String timeFmt) {
        return String.format("--db-database %s --db-master-host %s --db-master-port %s " +
                        "--source http --http-port %s --max-body-bytes 3000000 --request-timeout 500 " +
                        "--format csv --time-format %s --target %s.%s --grpc-port %d --allow-dynamic --debug",
                dbName, this.masterHost, this.dbPort,
                getMxgateHttpPort(),
                timeFmt, schema, tableName, getMxgateRpcPort());
    }

    public String generateHTTPCmdCfg(final String dbName, final String schema, final String tableName, int maxBody, int reqTimeout, final String timeFmt) {
        return String.format("--db-database %s --db-master-host %s --db-master-port %s " +
                        "--source http --http-port %s --max-body-bytes %d --request-timeout %d " +
                        "--format csv --time-format %s --target %s.%s --grpc-port %d --allow-dynamic --debug",
                dbName, this.masterHost, this.dbPort,
                getMxgateHttpPort(), maxBody, reqTimeout,
                timeFmt, schema, tableName, getMxgateRpcPort());
    }

    public String generateGRPCCmdCfg(final String dbName, final String schema, final String tableName) {
        return String.format("--db-database %s --db-master-host %s --db-master-port %s " +
                        "--source grpc --max-request-bytes 3000000 --request-timeout 500 " +
                        "--format csv --time-format raw --target %s.%s --grpc-port %d --allow-dynamic --debug",
                dbName, this.masterHost, this.dbPort,
                schema, tableName, getMxgateRpcPort());
    }

    public String generateGRPCCmdCfg(final String dbName, final String schema, final String tableName, final int reqTimeout) {
        return String.format("--db-database %s --db-master-host %s --db-master-port %s " +
                        "--source grpc --max-request-bytes 3000000 --request-timeout %d " +
                        "--format csv --time-format raw --target %s.%s --grpc-port %d --allow-dynamic --debug",
                dbName, this.masterHost, this.dbPort,
                reqTimeout,
                schema, tableName, getMxgateRpcPort());
    }

    public String generateGRPCCmdCfg(final String dbName, final String schema, final String tableName, final String timeFmt) {
        return String.format("--db-database %s --db-master-host %s --db-master-port %s " +
                        "--source grpc --max-request-bytes 3000000 --request-timeout 500 " +
                        "--format csv --time-format %s --target %s.%s --grpc-port %d --allow-dynamic --debug",
                dbName, this.masterHost, this.dbPort,
                timeFmt, schema, tableName, getMxgateRpcPort());
    }

    public String generateGRPCCmdCfg(final String dbName, final String schema, final String tableName, int maxBody, int reqTimeout, final String timeFmt) {
        return String.format("--db-database %s --db-master-host %s --db-master-port %s " +
                        "--source grpc --max-request-bytes %d --request-timeout %d " +
                        "--format csv --time-format %s --target %s.%s --grpc-port %d --allow-dynamic --debug",
                dbName, this.masterHost, this.dbPort,
                maxBody, reqTimeout,
                timeFmt, schema, tableName, getMxgateRpcPort());
    }
}
