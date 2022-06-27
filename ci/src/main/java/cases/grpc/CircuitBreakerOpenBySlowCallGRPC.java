package cases.grpc;

import Tools.PrintColor;
import api.matrix.mxgate.Mxgate;
import cases.DBHelper;
import cases.DataUtils;
import runner.E2ECase;
import cn.ymatrix.apiclient.MxClient;
import cn.ymatrix.builder.ConnectionListener;
import cn.ymatrix.builder.MxBuilder;
import junit.framework.AssertionFailedError;

import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public class CircuitBreakerOpenBySlowCallGRPC implements E2ECase {
    private final Tools.mxgateHelper mxgateHelper;
    private DBHelper dbHelper;
    private final DataUtils dataMaker = new DataUtils();

    final private MxBuilder builder;

    public CircuitBreakerOpenBySlowCallGRPC(MxBuilder builder, Tools.mxgateHelper mxgateHelper) {
        this.mxgateHelper = mxgateHelper;
        this.builder = builder;
    }

    @Override
    public String getName() {
        return this.getClass().getName();
    }
    @Override
    public void execute() throws SQLException, IOException {
        dbHelper = new DBHelper(this.mxgateHelper);

        String schema = "public";
        String tableName = String.format("%s_table_0001", this.getClass().getSimpleName()).toLowerCase();

        // prepare
        dbHelper.createTable(String.format("CREATE TABLE IF NOT EXISTS %1$s.%2$s (time TIMESTAMP, tagid INT, c1 INT NOT NULL, c2 INT NOT NULL DEFAULT 0, c3 text) DISTRIBUTED BY (tagid);TRUNCATE %1$s.%2$s", schema, tableName));


        // start mxgate
        try {
            mxgateHelper.startGrpc(dbHelper.getDBName(), schema, tableName, "unix-ms");
        } catch (Exception e) {
            throw new RuntimeException("failed to start mxgate", e);
        }

        Mxgate.GetMxGateStatus.Response resp;
        try {
            resp = mxgateHelper.getStatus();
        } catch (Exception e) {
            throw new RuntimeException("failed to get mxgate status", e);
        }
        assertTrue(resp.getCode() == 0);
        String bp = resp.getStatus().getBinaryPath();
        assertTrue(bp != null && bp.length() > 0);

        final String[] failMsgs = new String[1];
        String grpcURL = String.format("%s:%d", this.mxgateHelper.getMasterHost(), this.mxgateHelper.getMxgateRpcPort());
        AtomicInteger failedCnt = new AtomicInteger(); // for wait data process(insert to DB) finish
        this.builder.connect(grpcURL, grpcURL, schema, tableName, new ConnectionListener() {
            @Override
            public void onFailure(String failureMsg) {
                failMsgs[0] =String.format("failed to connect to {}: {}", tableName, failureMsg);
            }

            @Override
            public void onSuccess(MxClient client) {
                try {
                    dataMaker.sendWithCircuitBreakerOnBySlowCall(client, "cn.ymatrix.api.MxGrpcClient", "sendDataBlockingCore", 1100);
                } catch (Exception e){
                    failedCnt.addAndGet(1);
                    PrintColor.RedPrint(String.format("[%s] unexpected exception: %s", getName(),e.getMessage()));
                }
            }
        });
        for (String m: failMsgs) {
            assertNull(String.format("unexpected error: %s", m), m);
        }

        dataMaker.waitProcess();
        try {
            assertTrue(String.format("should count a minimum of  inserted tuples in %s.%s", schema, tableName), dbHelper.countTuple(tableName)> 2);
        } catch (SQLException e) {
            throw new AssertionFailedError(String.format("should not catch SQLException when counting tuples: %s", e.getMessage()));
        }
        assertEquals(0, failedCnt.get());
    }
}
