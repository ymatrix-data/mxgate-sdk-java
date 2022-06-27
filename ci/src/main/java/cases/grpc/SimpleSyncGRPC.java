package cases.grpc;

import Tools.TestLogger;
import api.matrix.mxgate.Mxgate;
import cases.DBHelper;
import cases.DataUtils;
import runner.E2ECase;
import cn.ymatrix.apiclient.MxClient;
import cn.ymatrix.builder.MxBuilder;
import junit.framework.AssertionFailedError;
import org.slf4j.Logger;

import java.io.IOException;
import java.sql.SQLException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SimpleSyncGRPC implements E2ECase {
    private final Logger l = TestLogger.init(this.getClass());

    private final Tools.mxgateHelper mxgateHelper;
    private final DBHelper dbHelper;
    private final DataUtils dataMaker = new DataUtils();

    private final MxBuilder builder;

    public SimpleSyncGRPC(MxBuilder builder, Tools.mxgateHelper mxgateHelper) {
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
            this.mxgateHelper.startGrpc(dbHelper.getDBName(), schema, tableName, 3000000, 1000, "unix-ms");
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
        int tableCnt = 1000;
        String grpcURL = String.format("%s:%d", this.mxgateHelper.getMasterHost(), this.mxgateHelper.getMxgateRpcPort());

        MxClient client = this.builder.connect(grpcURL, grpcURL, schema, tableName);
        dataMaker.syncSendNormal(client, 10, tableCnt);

        try {

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {

            }

            assertEquals(String.format("should count all inserted tuples in %s.%s", schema, tableName), tableCnt, dbHelper.countTuple(tableName));
        } catch (SQLException e) {
            e.printStackTrace();
            throw new AssertionFailedError(String.format("should not catch SQLException when counting tuples: %s", e.getMessage()));
        }
    }
}
