package cases.http;

import api.matrix.mxgate.Mxgate;
import cases.DBHelper;
import cases.DataUtils;
import runner.E2ECase;
import cn.ymatrix.apiclient.MxClient;
import cn.ymatrix.builder.MxBuilder;
import junit.framework.AssertionFailedError;

import java.io.IOException;
import java.sql.SQLException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SimpleSyncHTTP implements E2ECase {
    private final Tools.mxgateHelper mxgateHelper;
    final private DBHelper dbHelper;
    private final DataUtils dataMaker = new DataUtils();

    final private MxBuilder builder;

    public SimpleSyncHTTP(MxBuilder builder, Tools.mxgateHelper mxgateHelper) {
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
        dbHelper.createTable(String.format("CREATE TABLE IF NOT EXISTS %1$s.%2$s (time TIMESTAMP, tagid INT, c1 INT NOT NULL, c2 INT NOT NULL DEFAULT 0, c3 text) DISTRIBUTED BY (tagid);TRUNCATE %1$s.%2$s", schema, tableName));

        // start mxgate
        try {
            mxgateHelper.startHttp(dbHelper.getDBName(), schema, tableName, 50000, 1000, "unix-ms");
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
        int tableCnt = 1000;
        String grpcURL = String.format("%s:%d", this.mxgateHelper.getMasterHost(), this.mxgateHelper.getMxgateRpcPort());
        String httpURL = String.format("http://%s:%s/", this.mxgateHelper.getMasterHost(), this.mxgateHelper.getMxgateHttpPort());

        MxClient client = this.builder.connect(httpURL, grpcURL, schema, tableName);
        dataMaker.syncSendNormal(client, 10, tableCnt);

        try {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {

            }

            assertEquals(String.format("should count all inserted tuples in %s.%s", schema, tableName), tableCnt, dbHelper.countTuple(tableName));
        } catch (SQLException e) {
            throw new AssertionFailedError(String.format("should not catch SQLException when counting tuples: %s", e.getMessage()));
        }
    }
}
