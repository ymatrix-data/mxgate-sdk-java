package cases.grpc;

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
import java.nio.charset.Charset;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.*;
import static org.junit.Assert.assertTrue;

public class VariousEncodedStringGRPC implements E2ECase {
    private final Logger l = TestLogger.init(this.getClass());

    private final Tools.mxgateHelper mxgateHelper;
    private final DBHelper dbHelper;
    private final DataUtils dataMaker = new DataUtils();

    private final MxBuilder builder;

    private int grpcPort;

    public VariousEncodedStringGRPC(MxBuilder builder, Tools.mxgateHelper mxgateHelper) {
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
        dbHelper.createTable(String.format("CREATE TABLE IF NOT EXISTS %1$s.%2$s (time TIMESTAMP, tagid int, c1 text, c2 char(1024)) DISTRIBUTED BY (tagid);TRUNCATE %1$s.%2$s RESTART IDENTITY;", schema, tableName));

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

        this.grpcPort = this.mxgateHelper.getMxgateRpcPort();
        decodeAndEncodeToTest(schema, tableName, 0, "\\//1234测试测试\"2234；。……¥#&…*%@$^~`～·");
        decodeAndEncodeToTest(schema, tableName, 10, "CAPAC(“全容量”或者“预测全容量\"或者“简化容量”请选择其中一个)");
    }

    private void decodeAndEncodeToTest(String schema, String tableName, int tagidBase, String originalS) throws AssertionError, IOException, SQLException {
        HashMap<Integer, Charset> decodeCharsetMap = new HashMap<>();
        HashMap<Integer, Charset> encodeCharsetMap = new HashMap<>();
        {
            decodeCharsetMap.put(1, Charset.forName("gbk"));
            encodeCharsetMap.put(1, UTF_8);

            decodeCharsetMap.put(2, UTF_8);
            encodeCharsetMap.put(2, Charset.forName("gbk"));

            decodeCharsetMap.put(3, US_ASCII);
            encodeCharsetMap.put(3, Charset.forName("gbk"));

            decodeCharsetMap.put(4, Charset.forName("gbk"));
            encodeCharsetMap.put(4, US_ASCII);

            decodeCharsetMap.put(5, UTF_8);
            encodeCharsetMap.put(5, US_ASCII);

            decodeCharsetMap.put(6, US_ASCII);
            encodeCharsetMap.put(6, UTF_8);
        }


        int beforeCnt = dbHelper.countTuple(tableName);

        final String[] failMsgs = new String[encodeCharsetMap.size()];
        String grpcURL = String.format("%s:%d", this.mxgateHelper.getMasterHost(), this.grpcPort);
        final int[] idx = {0};
        for (Map.Entry<Integer, Charset> entry: decodeCharsetMap.entrySet()) {
            String reEncodedV = new String(originalS.getBytes(entry.getValue()), encodeCharsetMap.get(entry.getKey()));

            this.builder.connect(grpcURL, grpcURL, schema, tableName, new ConnectionListener() {
                @Override
                public void onFailure(String failureMsg) {
                    failMsgs[idx[0]]=String.format("failed to connect to {}: {}", tableName, failureMsg);
                    idx[0] +=1;
                }

                @Override
                public void onSuccess(MxClient client) {
                    idx[0] +=1;
                    dataMaker.sendString(client, 100, 100, entry.getKey()+tagidBase, reEncodedV);
                }
            });
        }
        for (String m: failMsgs) {
            assertNull(String.format("unexpected error: %s", m), m);
        }
        dataMaker.waitProcess();

        int afterCnt = dbHelper.countTuple(tableName);
        assertEquals("all re-encoded string value should be inserted", decodeCharsetMap.size(), afterCnt-beforeCnt);

        Statement stmt = dbHelper.getConn().createStatement();
        for (Map.Entry<Integer, Charset> entry: decodeCharsetMap.entrySet()) {
            String expectedV = new String(originalS.getBytes(entry.getValue()), encodeCharsetMap.get(entry.getKey()));

            ResultSet rs = stmt.executeQuery(String.format("SELECT c1 AS c1,c2 AS c2 FROM %s WHERE tagid=%d", tableName, entry.getKey()+tagidBase));
            if (rs.next()) {
                assertEquals("c1 should equal to insert when tagid="+entry.getKey(), expectedV, rs.getString("c1"));
                assertEquals("c2 should equal to insert when tagid="+entry.getKey(), expectedV, rs.getString("c2").trim());
            }
        }
    }
}


