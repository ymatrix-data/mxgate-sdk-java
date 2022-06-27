package cases;

import Tools.TestLogger;
import Tools.CmdExecutor;
import Tools.mxgateHelper;
import org.slf4j.Logger;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DBHelper {
    private final Logger l = TestLogger.init(DBHelper.class);

    private final static String DBName = "mxgate_sdk_java";
    private final String DBUser;
    private Connection conn;

    private String stopCmd = "gpstop";
    private String startCmd = "gpstart";

    public DBHelper(mxgateHelper mh) {
        DBUser = initDBUser(mh);
        try {
            Class.forName("org.postgresql.Driver");

            String ConnURLTpl = "jdbc:postgresql://%s:%s/%s?user=%s";
            String krbsrvname = System.getenv("PGKRBSRVNAME");
            if (krbsrvname != null && krbsrvname.length() > 0) {
                ConnURLTpl += ("&krbsrvname="+krbsrvname);
            }
            String url = String.format(ConnURLTpl,
                    mh.getMasterHost(), mh.getDbPort(), DBName, DBUser);
            this.conn = DriverManager.getConnection(url);

        } catch (Exception e) {
            l.error("failed to connect database: db_name= {}, db_user= {}, e= {}", DBName, DBUser, e.getMessage());
            System.exit(1);
        }

        try {
            Statement stmt = this.conn.createStatement();
            ResultSet rs = stmt.executeQuery("select version() as version");
            if (rs.next()) {
                String outputStr = rs.getString("version");

                Pattern pattern = Pattern.compile("\\(MatrixDB\\s(?<version>\\d+\\.\\d+\\.\\d+).*\\)");
                Matcher m = pattern.matcher(outputStr);
                if (m.find()) {
                    String v = m.group("version");
                    System.out.printf("MatrixDB version: %s\n", v);
                    String[] vParts = v.split("\\.");
                    try {
                        if (Integer.parseInt(vParts[0]) >= 5) {
                            this.startCmd = "mxstart";
                            this.stopCmd = "mxstop";
                        }
                    } catch (NumberFormatException e) {
                        System.out.printf("Cannot parse MatrixDB version invalid: %s\n", v);
                        System.exit(1);
                    }
                } else {
                    System.out.printf("Cannot parse MatrixDB version: %s\n", outputStr);
                    System.exit(1);
                }
            } else {
                System.out.println("get nothing from 'select version()'");
                System.exit(1);
            }
        } catch (SQLException e) {
            l.error("failed to select version: db_name= {}, db_user= {}, e= {}", DBName, DBUser, e.getMessage());
            System.exit(1);
        }
    }

    private static String initDBUser(mxgateHelper mh) {
        String DBUser = mh.getDbUser();
        if (DBUser== null ||DBUser.length() <= 0) {
            DBUser = System.getProperty("user.name");
        }
        return DBUser;
    }

    public String getDBName() {
        return this.DBName;
    }

    public Connection getConn() {
        return this.conn;
    }

    public void createTable(final String sql) throws SQLException {
        Statement stmt = this.conn.createStatement();
        stmt.execute(sql);
    }

    public void truncateTable(final String tableName) {
        try {
            Statement stmt = this.conn.createStatement();
            stmt.execute(String.format("TRUNCATE TABLE %s", tableName));
        } catch (SQLException e) {
            System.out.printf("failed to truncate table(%s): %s", tableName, e.getMessage());
        }
    }

    public void dropTable(final String tableName) throws SQLException {
        Statement stmt = this.conn.createStatement();
        boolean ok = stmt.execute(String.format("DROP TABLE %s", tableName));
        if (!ok) {
            throw new SQLException("cannot drop table");
        }
    }

    public int countTuple(final String tableName) throws SQLException {
        Statement stmt = this.conn.createStatement();
        ResultSet rs = stmt.executeQuery(String.format("SELECT count(*) AS total FROM %s", tableName));
        if (rs.next()) {
            return rs.getInt("total");
        }
        return 0;
    }

    public ResultSet selectAll(final String tableName) throws SQLException {
        Statement stmt = this.conn.createStatement();
        return stmt.executeQuery(String.format("SELECT * FROM %s", tableName));
    }

    public void shutDown() throws IOException {
        ArrayList cmdArgs = new ArrayList<>(Arrays.asList(new String[]{this.stopCmd, "-af"}));
        CmdExecutor.run("gpstop", cmdArgs, 5000, 10);
    }

    public void launch() throws IOException {
        ArrayList cmdArgs = new ArrayList<>(Arrays.asList(new String[]{this.startCmd, "-a"}));
        CmdExecutor.run("gpstop", cmdArgs, 5000, 10);
    }

    public void close() {
        try {
            this.conn.close();
        } catch (SQLException e) {
            System.out.printf("Failed to close connection to DB: %s\n", e.getMessage());
        }
    }
}
