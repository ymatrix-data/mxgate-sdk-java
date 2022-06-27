package cases.http;

import Tools.TestLogger;
import api.matrix.mxgate.Mxgate;
import cases.DBHelper;
import cases.DataUtils;
import runner.E2ECase;
import cn.ymatrix.apiclient.MxClient;
import cn.ymatrix.builder.ConnectionListener;
import cn.ymatrix.builder.MxBuilder;
import junit.framework.AssertionFailedError;
import org.slf4j.Logger;

import java.io.IOException;
import java.sql.SQLException;

import static org.junit.Assert.*;

public class MultipleColumnTypeHTTP implements E2ECase {
    private final Logger l = TestLogger.init(this.getClass());

    private final Tools.mxgateHelper mxgateHelper;
    final private DBHelper dbHelper;
    private final DataUtils dataMaker = new DataUtils();

    final private MxBuilder builder;

    public MultipleColumnTypeHTTP(MxBuilder builder, Tools.mxgateHelper mxgateHelper) {
        this.mxgateHelper = mxgateHelper;
        this.dbHelper = new DBHelper(this.mxgateHelper);
        this.builder = builder;
    }

    @Override
    public String getName() {
        return this.getClass().getName();
    }

    @Override
    public void execute() throws AssertionError, IOException, SQLException {
        String schema = "public";
        String tableName = String.format("%s_table_0001", this.getClass().getSimpleName()).toLowerCase();

        // prepare
        dbHelper.createTable(String.format("CREATE TABLE IF NOT EXISTS %1$s.%2$s (time TIMESTAMP, tagid SERIAL, c1 BOOLEAN, c2 CHAR(5), c3 NUMERIC(15,5), c4 JSON, c5 JSONB, c6 TIME WITH TIME ZONE, c7 UUID) DISTRIBUTED BY (tagid); TRUNCATE %1$s.%2$s", schema, tableName));

        // start mxgate
        try {
            mxgateHelper.startHttp(dbHelper.getDBName(), schema, tableName, "unix-ms");
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

        // call sdk
        int tableCnt = 10;
        final String[] failMsgs = new String[2];
        String grpcURL = String.format("%s:%d", this.mxgateHelper.getMasterHost(), this.mxgateHelper.getMxgateRpcPort());
        String httpURL = String.format("http://%s:%s/", this.mxgateHelper.getMasterHost(), this.mxgateHelper.getMxgateHttpPort());
        this.builder.connect(httpURL, grpcURL, schema, tableName, new ConnectionListener() {
            @Override
            public void onFailure(String failureMsg) {
                failMsgs[0] =String.format("failed to connect to {}: {}", tableName, failureMsg);
            }

            @Override
            public void onSuccess(MxClient client) {
                dataMaker.sendMultipleColumnType(client, 100, 100, tableCnt);
            }
        });

        // send empty tuple
        this.builder.connect(httpURL, grpcURL, schema, tableName, new ConnectionListener() {
            @Override
            public void onFailure(String failureMsg) {
                failMsgs[1] =String.format("failed to connect to {}: {}", tableName, failureMsg);
            }

            @Override
            public void onSuccess(MxClient client) {
                dataMaker.sendMultipleColumnTypeEmpty(client, 100, 100, tableCnt);
            }
        });
        for (String m: failMsgs) {
            assertNull(String.format("unexpected error: %s", m), m);
        }

        dataMaker.waitProcess();
        try {
            assertEquals(String.format("should count all inserted tuples in %s.%s", schema, tableName), tableCnt*2, dbHelper.countTuple(tableName));
        } catch (SQLException e) {
            throw new AssertionFailedError(String.format("should not catch SQLException when counting tuples: %s", e.getMessage()));
        } catch (AssertionError e) {
            throw e;
        }
    }
}
