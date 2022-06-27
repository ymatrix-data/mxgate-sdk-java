package cases.grpc;

import api.matrix.mxgate.Mxgate;
import cases.DBHelper;
import runner.E2ECase;
import cn.ymatrix.apiclient.MxClient;
import cn.ymatrix.builder.MxBuilder;
import cn.ymatrix.logger.MxLogger;
import org.junit.Assert;
import org.slf4j.Logger;

import static org.junit.Assert.assertTrue;

public class WriterIsNilGRPC implements E2ECase  {

    private static final Logger l = MxLogger.init(WriterIsNilGRPC.class);
    private final Tools.mxgateHelper mxgateHelper;
    private final DBHelper dbHelper;
    private final MxBuilder builder;

    public WriterIsNilGRPC(MxBuilder builder, Tools.mxgateHelper helper) {
        this.mxgateHelper = helper;
        this.dbHelper = new DBHelper(this.mxgateHelper);
        this.builder = builder;
    }

    @Override
    public void execute() throws Exception {
        String schema = "public";
        String tableName = String.format("%s_table_0001", this.getClass().getSimpleName()).toLowerCase();
        dbHelper.createTable(String.format("CREATE TABLE IF NOT EXISTS %1$s.%2$s (c1 INT NOT NULL, c2 INT NOT NULL, c3 text); TRUNCATE %1$s.%2$s", schema, tableName));


        // start mxgate
        try {
            this.mxgateHelper.startGrpc(dbHelper.getDBName(), schema, tableName, 1000);
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

        String grpcURL = String.format("%s:%d", this.mxgateHelper.getMasterHost(), this.mxgateHelper.getMxgateRpcPort());
        MxClient client = this.builder.connect(grpcURL, grpcURL, schema, tableName);
        Assert.assertNotNull(client);
    }

    @Override
    public String getName() {
        return this.getClass().getName();
    }


}
