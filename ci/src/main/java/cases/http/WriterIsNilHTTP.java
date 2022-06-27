package cases.http;

import api.matrix.mxgate.Mxgate;
import cases.DBHelper;
import runner.E2ECase;
import cn.ymatrix.apiclient.MxClient;
import cn.ymatrix.builder.MxBuilder;
import cn.ymatrix.logger.MxLogger;
import org.junit.Assert;
import org.slf4j.Logger;

import static org.junit.Assert.assertTrue;

public class WriterIsNilHTTP implements E2ECase {

    private static final Logger l = MxLogger.init(WriterIsNilHTTP.class);
    private final Tools.mxgateHelper mxgateHelper;
    private final DBHelper dbHelper;
    private final MxBuilder builder;

    public WriterIsNilHTTP(MxBuilder builder, Tools.mxgateHelper helper) {
        this.builder = builder;
        this.mxgateHelper = helper;
        this.dbHelper = new DBHelper(this.mxgateHelper);
    }

    @Override
    public void execute() throws Exception {
        String schema = "public";
        String tableName1 = String.format("%s_table_0001", this.getClass().getSimpleName()).toLowerCase();
        dbHelper.createTable(String.format("CREATE TABLE IF NOT EXISTS %1$s.%2$s (c1 INT NOT NULL, c2 INT NOT NULL, c3 text); TRUNCATE %1$s.%2$s", schema, tableName1));

        // start mxgate
        try {
            mxgateHelper.startHttp(dbHelper.getDBName(), schema, tableName1);
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

        String grpcURL = String.format("%s:%d", this.mxgateHelper.getMasterHost(), this.mxgateHelper.getMxgateRpcPort());
        String httpURL = String.format("http://%s:%s/", this.mxgateHelper.getMasterHost(), this.mxgateHelper.getMxgateHttpPort());
        MxClient client = this.builder.connect(httpURL, grpcURL, schema, tableName1);
        Assert.assertNotNull(client);
    }

    @Override
    public String getName() {
        return this.getClass().getName();
    }
}
