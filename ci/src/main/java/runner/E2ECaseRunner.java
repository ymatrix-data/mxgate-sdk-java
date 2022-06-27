package runner;

import Tools.PrintColor;
import Tools.mxgateHelper;
import cn.ymatrix.utils.StrUtil;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;

public class E2ECaseRunner {
    private final String TAG = StrUtil.logTagWrap(this.getClass().getName());

    final Tools.mxgateHelper mxgateHelper = getMxgateHelper();
    private static ArrayList<E2ECase> e2ECases = new ArrayList<>();

    private static E2ECaseRunner runner = new E2ECaseRunner();

    E2ECaseRunner() {
    }

    private mxgateHelper getMxgateHelper() {
        String masterHost = System.getenv().getOrDefault("MATRIXDB_MASTER_HOST", "localhost");
        String masterPort = System.getenv().getOrDefault("MATRIXDB_MASTER_PORT", "5432");
        String dbUser = System.getenv().getOrDefault("MATRIXDB_USER", "mxadmin");
        String supervisorPort = System.getenv().getOrDefault("SUPERVISOR_PORT", "4617");
        String mxgateHttpPort = System.getenv().getOrDefault("MXGATE_HTTP_PORT", "8086");

        return new mxgateHelper(masterHost, masterPort, dbUser, supervisorPort, mxgateHttpPort, 2000);
    }

    public static E2ECaseRunner getInstance() {
        return runner;
    }

    void addTestCase(E2ECase t) {
        e2ECases.add(t);
    }

    void execute() {
        int successCnt = 0;
        int failedCnt = 0;
        for (E2ECase t : e2ECases) {
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
            }

            try {
                mxgateHelper.foreStop();
            } catch (Exception e) {
                Throwable causeE = e.getCause();
                if (causeE.getClass().equals(io.grpc.StatusRuntimeException.class)) {
                    if (!causeE.getMessage().toLowerCase().contains("notfound")) {
                        PrintColor.YellowPrint(String.format("Unexpected status: %s", e.getMessage()));
                        e.printStackTrace();
                    }
                } else {
                    PrintColor.RedPrint(String.format("Failed to stop mxgate ahead: %s", e.getMessage()));
                    e.printStackTrace();
                    break;
                }
            }

            boolean caseFailed = false;
            PrintColor.BluePrint(String.format("Ready to execute: %s", t.getName()));
            try {
                t.execute();
                successCnt++;
            } catch (AssertionError | Exception e) {
                failedCnt++;
                caseFailed = true;
                PrintColor.RedPrint(String.format("[%s] failed to execute: %s", t.getName(), e.getMessage()));
                e.printStackTrace();
            }
            if (!caseFailed) {
                PrintColor.BluePrint(String.format("Succeed execute: %s", t.getName()));
            }

            try {
                mxgateHelper.foreStop();
            } catch (Exception e) {
                PrintColor.RedPrint(String.format("Failed to stop mxgate: %s", e.getMessage()));
                e.printStackTrace();
                break;
            }
        }

        int allCnt = e2ECases.size();
        int remain = allCnt - successCnt - failedCnt;
        if (failedCnt > 0 || remain > 0) {
            System.out.println(TAG+" -- "+PrintColor.ANSI_RED+"FAILED"+PrintColor.ANSI_RESET
                    +" | "+PrintColor.ANSI_BLUE+"Total: "+allCnt+PrintColor.ANSI_RESET
                    +" | "+PrintColor.ANSI_GREEN+"Success: "+successCnt+PrintColor.ANSI_RESET
                    +" | "+PrintColor.ANSI_RED+"Failed: "+failedCnt+PrintColor.ANSI_RESET
                    +" | "+PrintColor.ANSI_YELLOW+"Remain: "+remain+PrintColor.ANSI_RESET);
            System.exit(1);
        } else {
            System.out.println(TAG+" -- "+PrintColor.ANSI_GREEN+"PASS"+PrintColor.ANSI_RESET
                    +" | "+PrintColor.ANSI_BLUE+"Total: "+allCnt+PrintColor.ANSI_RESET
                    +" | "+PrintColor.ANSI_GREEN+"Success: "+successCnt+PrintColor.ANSI_RESET);
        }

        System.exit(0);
    }
}
