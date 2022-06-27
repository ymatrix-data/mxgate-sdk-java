package cases.objectpool;

import api.matrix.mxgate.Mxgate;
import cases.DBHelper;
import cases.DataUtils;
import Tools.mxgateHelper;
import runner.E2ECase;
import cn.ymatrix.apiclient.MxClient;
import cn.ymatrix.builder.MxBuilder;
import cn.ymatrix.data.Tuple;
import cn.ymatrix.logger.MxLogger;
import org.junit.Assert;
import org.slf4j.Logger;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ObjectPoolTestCase implements E2ECase {
    private static final Logger l = MxLogger.init(ObjectPoolTestCase.class);
    private final Tools.mxgateHelper mxgateHelper;
    private final DBHelper dbHelper;
    private final MxBuilder builder;

    public ObjectPoolTestCase(MxBuilder builder, mxgateHelper helper) {
        this.mxgateHelper = helper;
        this.dbHelper = new DBHelper(this.mxgateHelper);
        this.builder = builder;
    }

    @Override
    public void execute() throws AssertionError, SQLException, IOException, NullPointerException, InterruptedException {
        String schema = "public";
        String tableName = String.format("%s_table_0001", this.getClass().getSimpleName()).toLowerCase();
        dbHelper.createTable(String.format("CREATE TABLE IF NOT EXISTS %1$s.%2$s (c1 INT NOT NULL, c2 INT NOT NULL, c3 text); TRUNCATE %1$s.%2$s", schema, tableName));

        // start mxgate
        try {
            this.mxgateHelper.startHttp(dbHelper.getDBName(), schema, tableName);
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
        String httpURL = String.format("http://%s:%s/", this.mxgateHelper.getMasterHost(), this.mxgateHelper.getMxgateHttpPort());

        MxClient client = this.builder.connect(httpURL, grpcURL, schema, tableName);
        Assert.assertNotNull(client);

        client.useTuplesPool(200);
        client.withEnoughLinesToFlush(10);

        int tableCnt = 1000;
        List<ObjectPoolTestTableBean> beans = prepareTableData(tableCnt);
        if (beans == null) {
            throw new NullPointerException("list of test beans is null");
        }

        for (ObjectPoolTestTableBean bean : beans) {
            Tuple tuple = client.generateEmptyTuple();
            tuple.addColumn("c1", bean.getC1());
            tuple.addColumn("c2", bean.getC2());
            tuple.addColumn("c3", bean.getC3());
            if (client.appendTupleBlocking(tuple)) {
                client.flushBlocking();
            }
        }

        // Wait the data to be inserted into DB.
        sleep(3000);

        assertEquals(String.format("should count all inserted tuples in %s.%s", schema, tableName), tableCnt, dbHelper.countTuple(tableName));

        ResultSet rs = dbHelper.selectAll(tableName);

        while (rs.next()) {
            int c1 = rs.getInt("c1");
            ObjectPoolTestTableBean bean = find(beans, c1);
            Assert.assertNotNull(bean);
            int c2 = rs.getInt("c2");
            String c3 = rs.getString("c3");
            l.info("Expect Bean.c1({}) = {}, Bean.c2({}) = {}, Bean.c3({}) = {}", bean.getC1(), c1, bean.getC2(), c2, bean.getC3(), c3);
            Assert.assertEquals(c2, bean.getC2());
            if (c3 != null) {
                Assert.assertEquals(c3, bean.getC3());
            } else {
                Assert.assertEquals(0, bean.getC3().length());
            }
        }

    }

    @Override
    public String getName() {
        return this.getClass().getSimpleName();
    }

    private List<ObjectPoolTestTableBean> prepareTableData(int lines) {
        DataUtils utils = new DataUtils();
        List<ObjectPoolTestTableBean> list = new ArrayList<>();
        for (int i = 0; i < lines; i++) {
            ObjectPoolTestTableBean bean = new ObjectPoolTestTableBean();
            bean.setC1(i);
            bean.setC2(i + 1);
            bean.setC3(utils.generateRandomString(30));
            list.add(bean);
        }
        return list;
    }

    private ObjectPoolTestTableBean find(List<ObjectPoolTestTableBean> beans, int targetC1) {
        for (ObjectPoolTestTableBean bean : beans) {
            if (bean.getC1() == targetC1) {
                return bean;
            }
        }
        return null;
    }

    private void sleep(int millis) throws InterruptedException {
        Thread.sleep(millis);
    }
}











