package cases.grpc;

import Tools.TestLogger;
import api.matrix.mxgate.Mxgate;
import cases.DBHelper;
import cases.DataUtils;
import Tools.mxgateHelper;
import runner.E2ECase;
import cn.ymatrix.apiclient.MxClient;
import cn.ymatrix.builder.ConnectionListener;
import cn.ymatrix.builder.MxBuilder;
import junit.framework.AssertionFailedError;
import org.slf4j.Logger;

import java.io.IOException;
import java.sql.SQLException;

import static org.junit.Assert.*;


public class SimpleConcurrencyGRPC implements E2ECase {
    private final Logger l = TestLogger.init(this.getClass());

    private final Tools.mxgateHelper mxgateHelper;
    private final DBHelper dbHelper;
    private final DataUtils dataMaker = new DataUtils();

    private final MxBuilder builder;

    public SimpleConcurrencyGRPC(MxBuilder builder, mxgateHelper mxgateHelper) {
        this.mxgateHelper = mxgateHelper;
        this.builder = builder;
        this.dbHelper = new DBHelper(this.mxgateHelper);
    }

    @Override
    public String getName() {
        return this.getClass().getName();
    }
    @Override
    public void execute() throws AssertionError, IOException, SQLException {
        String schema = "public";
        String tableName1 = String.format("%s_table_0001", this.getClass().getSimpleName()).toLowerCase();
        String tableName2 = String.format("%s_table_0002", this.getClass().getSimpleName()).toLowerCase();

        // prepare
        dbHelper.createTable(String.format("CREATE TABLE IF NOT EXISTS %1$s.%2$s (time TIMESTAMP, tagid INT, c1 INT NOT NULL, c2 INT NOT NULL DEFAULT 0, c3 text) DISTRIBUTED BY (tagid);TRUNCATE %1$s.%2$s", schema, tableName1));
        dbHelper.createTable(String.format("CREATE TABLE IF NOT EXISTS %1$s.%2$s (time TIMESTAMP, tagid INT, c1 INT NOT NULL, c2 INT NOT NULL DEFAULT 0, c3 text) DISTRIBUTED BY (tagid);TRUNCATE %1$s.%2$s", schema, tableName2));

        // start mxgate
        try {
            this.mxgateHelper.startGrpc(dbHelper.getDBName(), schema, tableName1, "unix-ms");
        } catch (Exception e) {
            throw new RuntimeException("failed to start mxgate", e);
        }

        Mxgate.GetMxGateStatus.Response resp;
        try {
            resp = this.mxgateHelper.getStatus();
        } catch (Exception e) {
            throw new RuntimeException("failed to get mxgate status", e);
        }
        assertTrue(resp.getCode() == 0);
        String bp = resp.getStatus().getBinaryPath();
        assertTrue(bp != null && bp.length() > 0);

        // call sdk
        final String[] failMsgs = new String[2];
        int tableCnt1 = 1000;
        int tableCnt2 = 500;
        String grpcURL = String.format("%s:%d", this.mxgateHelper.getMasterHost(), this.mxgateHelper.getMxgateRpcPort());
        this.builder.connect(grpcURL, grpcURL, schema, tableName1, new ConnectionListener() {
            @Override
            public void onFailure(String failureMsg) {
                failMsgs[0] =String.format("failed to connect to {}: {}", tableName1, failureMsg);
            }

            @Override
            public void onSuccess(MxClient client) {
                dataMaker.sendNormal(client, 100, 100, tableCnt1, 50);
            }
        });

        this.builder.connect(grpcURL, grpcURL, schema, tableName2, new ConnectionListener() {
            @Override
            public void onFailure(String failureMsg) {
                failMsgs[1] =String.format("failed to connect to {}: {}", tableName2, failureMsg);
            }

            @Override
            public void onSuccess(MxClient client) {
                dataMaker.sendNormal(client, 100, 100, tableCnt2, 50);
            }
        });
        for (String m: failMsgs) {
            assertNull(String.format("unexpected error: %s", m), m);
        }

        dataMaker.waitProcess();
        try {
            assertEquals(String.format("should count all inserted tuples in %s.%s", schema, tableName1), tableCnt1, dbHelper.countTuple(tableName1));
            assertEquals(String.format("should count all inserted tuples in %s.%s", schema, tableName2), tableCnt2, dbHelper.countTuple(tableName2));
        } catch (SQLException e) {
            throw new AssertionFailedError(String.format("should not catch SQLException when counting tuples: %s", e.getMessage()));
        } catch (AssertionError e) {
            throw e;
        }
    }
}
