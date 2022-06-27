package cases.http;

import Tools.PrintColor;
import api.matrix.mxgate.Mxgate;
import cases.DBHelper;
import cases.DataUtils;
import runner.E2ECase;
import cn.ymatrix.apiclient.MxClient;
import cn.ymatrix.builder.MxBuilder;
import junit.framework.AssertionFailedError;
import org.junit.Assert;

import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public class CircuitBreakerOpenByFailureHTTP implements E2ECase {
    private final Tools.mxgateHelper mxgateHelper;
    private DBHelper dbHelper;
    private final DataUtils dataMaker = new DataUtils();

    final private MxBuilder builder;

    public CircuitBreakerOpenByFailureHTTP(MxBuilder builder, Tools.mxgateHelper mxgateHelper) {
        this.mxgateHelper = mxgateHelper;
        this.dbHelper = new DBHelper(this.mxgateHelper);
        this.builder = builder;
    }

    @Override
    public String getName() {
        return this.getClass().getName();
    }

    @Override
    public void execute() throws SQLException, IOException {
        By("Prepare");
        String schema = "public";
        String tableName = String.format("%s_table_0001", this.getClass().getSimpleName()).toLowerCase();

        // prepare
        dbHelper.createTable(String.format("CREATE TABLE IF NOT EXISTS %1$s.%2$s (time TIMESTAMP, tagid INT, c1 INT NOT NULL, c2 INT NOT NULL DEFAULT 0, c3 text) DISTRIBUTED BY (tagid);TRUNCATE %1$s.%2$s", schema, tableName));


        // start mxgate
        String cmdCfg = this.mxgateHelper.generateHTTPCmdCfg(dbHelper.getDBName(), schema, tableName, "unix-ms");
        System.out.println(cmdCfg);
        try {
            mxgateHelper.start(cmdCfg);
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

        By("Request exception");
        String grpcURL = String.format("%s:%d", this.mxgateHelper.getMasterHost(), this.mxgateHelper.getMxgateRpcPort());
        String httpURL = String.format("http://%s:%s/", this.mxgateHelper.getMasterHost(), this.mxgateHelper.getMxgateHttpPort());AtomicInteger failedCnt = new AtomicInteger(); // for wait data process(insert to DB) finish
        MxClient client = this.builder.connect(httpURL, grpcURL, schema, tableName);
        Assert.assertNotNull(client);
        try {
            dataMaker.sendWithCircuitBreakerOnByFailure(client, "cn.ymatrix.httpclient.HttpTask", "requestBlockingCore");
        } catch (Exception e) {
            failedCnt.addAndGet(1);
            PrintColor.RedPrint(String.format("[%s] unexpected exception: %s", getName(), e.getMessage()));
        }

        dataMaker.waitProcess();
        try {
            int expectCnt = 2;
            int actualCnt = dbHelper.countTuple(tableName);
            assertTrue(String.format("expect at least %d tuples in %s.%s, but get %d", expectCnt, schema, tableName, actualCnt), actualCnt >= expectCnt);
        } catch (SQLException e) {
            throw new AssertionFailedError(String.format("should not catch SQLException when counting tuples: %s", e.getMessage()));
        }
        assertEquals(0, failedCnt.get());

        normalSend(httpURL, grpcURL, schema, tableName, 10);

        By("Mxgate broken");

        MxClient client1 = this.builder.connect(httpURL, grpcURL, schema, tableName);
        Assert.assertNotNull(client1);
        try {
            dataMaker.sendWithCircuitBreakerOnByMxGateDown(mxgateHelper, dbHelper.getDBName(), cmdCfg, client1);
        } catch (Exception e) {
            failedCnt.addAndGet(1);
            PrintColor.RedPrint(String.format("[%s] unexpected exception: %s", getName(), e.getMessage()));
        }

        dataMaker.waitProcess();
        try {
            int expectCnt = 2;
            int actualCnt = dbHelper.countTuple(tableName);
            assertTrue(String.format("expect at least %d tuples in %s.%s, but get %d", expectCnt, schema, tableName, actualCnt), actualCnt >= expectCnt);
        } catch (SQLException e) {
            throw new AssertionFailedError(String.format("should not catch SQLException when counting tuples: %s", e.getMessage()));
        }
        assertEquals(0, failedCnt.get());

        normalSend(httpURL, grpcURL, schema, tableName, 10);

        By("DB broken");
        MxClient client2 = this.builder.connect(httpURL, grpcURL, schema, tableName);
        Assert.assertNotNull(client2);

        try {
            dataMaker.sendWithCircuitBreakerOnByDBDown(client2, this.mxgateHelper);
        } catch (Exception e) {
            failedCnt.addAndGet(1);
            PrintColor.RedPrint(String.format("[%s] unexpected exception: %s", getName(), e.getMessage()));
        }

        dataMaker.waitProcess();
        dbHelper = new DBHelper(this.mxgateHelper);
        try {
            int expectCnt = 2;
            int actualCnt = dbHelper.countTuple(tableName);
            assertTrue(String.format("expect at least %d tuples in %s.%s, but get %d", expectCnt, schema, tableName, actualCnt), actualCnt >= expectCnt);
        } catch (SQLException e) {
            throw new AssertionFailedError(String.format("should not catch SQLException when counting tuples: %s", e.getMessage()));
        }
        assertEquals(0, failedCnt.get());
    }

    private void normalSend(String httpURL, String grpcURL, String schema, String tableName, int cnt) {
        By("make breaker closed");
        MxClient client = this.builder.connect(httpURL, grpcURL, schema, tableName);
        Assert.assertNotNull(client);
        dataMaker.sendNormal(client, 100, 10, cnt, 0);

        dataMaker.waitProcess();
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            // do nothing
        }
        dbHelper.truncateTable(schema + "." + tableName);
    }

    private void By(String msg) {
        System.out.println("At:\t" + msg);
    }
}
