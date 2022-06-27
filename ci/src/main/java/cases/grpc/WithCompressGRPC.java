package cases.grpc;

import api.matrix.mxgate.Mxgate;
import cases.DBHelper;
import cases.DataUtils;
import runner.E2ECase;
import cn.ymatrix.apiclient.MxClient;
import cn.ymatrix.builder.MxBuilder;
import cn.ymatrix.data.Tuple;
import cn.ymatrix.logger.MxLogger;
import org.junit.Assert;
import org.slf4j.Logger;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class WithCompressGRPC implements E2ECase {
    private static final Logger l = MxLogger.init(WithCompressGRPC.class);
    private final Tools.mxgateHelper mxgateHelper;
    private final DBHelper dbHelper;
    private final MxBuilder builder;

    public WithCompressGRPC(MxBuilder builder, Tools.mxgateHelper helper) {
        this.mxgateHelper = helper;
        this.builder = builder;
        this.dbHelper = new DBHelper(this.mxgateHelper);
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

        client.withEnoughLinesToFlush(10);
        client.withCompress();
        client.withBase64Encode4Compress();

        int tableCnt = 1000;
        List<WithCompressGRPCBean> beans = prepareTableData(tableCnt);
        if (beans == null) {
            throw new NullPointerException("list of test beans is null");
        }

        for (WithCompressGRPCBean bean : beans) {
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
            WithCompressGRPCBean bean = find(beans, c1);
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

    private List<WithCompressGRPCBean> prepareTableData(int lines) {
        DataUtils utils = new DataUtils();
        List<WithCompressGRPCBean> list = new ArrayList<>();
        for (int i = 0; i < lines; i++) {
            WithCompressGRPCBean bean = new WithCompressGRPCBean();
            bean.setC1(i);
            bean.setC2(i + 1);
            bean.setC3(utils.generateRandomString(30));
            list.add(bean);
        }
        return list;
    }

    private WithCompressGRPCBean find(List<WithCompressGRPCBean> beans, int targetC1) {
        for (WithCompressGRPCBean bean : beans) {
            if (bean.getC1() == targetC1) {
                return bean;
            }
        }
        return null;
    }

    private void sleep(int millis) throws InterruptedException {
        Thread.sleep(millis);
    }

    @Override
    public String getName() {
        return this.getClass().getName();
    }


    private static class WithCompressGRPCBean {
        private int c1;

        private int c2;

        private String c3;


        public int getC1() {
            return c1;
        }

        public void setC1(int c1) {
            this.c1 = c1;
        }

        public int getC2() {
            return c2;
        }

        public void setC2(int c2) {
            this.c2 = c2;
        }

        public String getC3() {
            return c3;
        }

        public void setC3(String c3) {
            this.c3 = c3;
        }
    }
}
