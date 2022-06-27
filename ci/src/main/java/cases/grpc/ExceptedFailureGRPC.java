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

public class ExceptedFailureGRPC implements E2ECase {
    private final Logger l = TestLogger.init(this.getClass());

    private final Tools.mxgateHelper mxgateHelper;
    private final DBHelper dbHelper;
    private final DataUtils dataMaker = new DataUtils();

    final private MxBuilder builder;

    public ExceptedFailureGRPC(MxBuilder builder, mxgateHelper mxgateHelper) {
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
        String tableName = String.format("%s_table_0001", this.getClass().getSimpleName()).toLowerCase();

        // prepare
        dbHelper.createTable(String.format("CREATE TABLE IF NOT EXISTS %1$s.%2$s (time TIMESTAMP, tagid INT, c1 INT NOT NULL, c2 INT NOT NULL DEFAULT 0, c3 text) DISTRIBUTED BY (tagid);TRUNCATE %1$s.%2$s", schema, tableName));

        // start mxgate
        try {
            this.mxgateHelper.startGrpc(dbHelper.getDBName(), schema, tableName, "unix-ms");
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
        final String[] failMsgs = new String[4];
        String grpcURL = String.format("%s:%d", this.mxgateHelper.getMasterHost(), this.mxgateHelper.getMxgateRpcPort());
        // should failed: (1) wrong format (2) out of range (3) violate constraint (4) column not found
        builder.connect(grpcURL, grpcURL, schema, tableName, new ConnectionListener() {
            @Override
            public void onFailure(String failureMsg) {
                failMsgs[0] =String.format("failed to connect to {}: {}", tableName, failureMsg);
            }

            @Override
            public void onSuccess(MxClient client) {
                l.info("Get MxClient success, begin to send data to {}.");

                dataMaker.sendFailedWrongType1(client);
            }
        });

        builder.connect(grpcURL, grpcURL, schema, tableName, new ConnectionListener() {
            @Override
            public void onFailure(String failureMsg) {
                failMsgs[1] =String.format("failed to connect to {}: {}", tableName, failureMsg);
            }

            @Override
            public void onSuccess(MxClient client) {
                l.info("Get MxClient success, begin to send data to {}.");

                dataMaker.sendFailedOutOfRange1(client);
            }
        });

        builder.connect(grpcURL, grpcURL, schema, tableName, new ConnectionListener() {
            @Override
            public void onFailure(String failureMsg) {
                failMsgs[2] =String.format("failed to connect to {}: {}", tableName, failureMsg);
            }

            @Override
            public void onSuccess(MxClient client) {
                l.info("Get MxClient success, begin to send data to {}.");

                dataMaker.sendFailedShouldNotNull1(client);
            }
        });

        builder.connect(grpcURL, grpcURL, schema, tableName, new ConnectionListener() {
            @Override
            public void onFailure(String failureMsg) {
                failMsgs[3] =String.format("failed to connect to {}: {}", tableName, failureMsg);
            }

            @Override
            public void onSuccess(MxClient client) {
                l.info("Get MxClient success, begin to send data to {}.");

                dataMaker.sendFailedColumnNotFound1(client);
            }
        });
        for (String m: failMsgs) {
            assertNull(String.format("unexpected error: %s", m), m);
        }

        dataMaker.waitProcess();
        try {
            assertEquals(String.format("should count 0 in %s.%s", schema, tableName), 0, dbHelper.countTuple(tableName));
        } catch (SQLException e) {
            throw new AssertionFailedError(String.format("should not catch SQLException when counting tuples: %s", e.getMessage()));
        } catch (AssertionError e) {
            throw e;
        }
    }
}
