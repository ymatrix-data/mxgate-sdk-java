package cases;

import Tools.FindExecutable;
import Tools.CmdExecutor;
import com.google.gson.Gson;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Arrays;

// https://github.com/chaosblade-io/chaosblade/blob/master/README_CN.md
public class ChaosBlade {
    private String pid;
    private String jvmID;
    private static final String cmd = "blade";
    private static String bladePath = new FindExecutable().lookPath(cmd);

    public ChaosBlade() throws IOException {
        if (bladePath == null) {
            throw new FileNotFoundException(String.format("%s not found in PATH", cmd));
        }
        pid = ManagementFactory.getRuntimeMXBean().getName().split("@")[0];

        System.out.printf("PID: %s\n", pid);
        prepareJVM();
    }

    public void prepareJVM() throws IOException {
        ArrayList cmdArgs =  new ArrayList<>(Arrays.asList(new String[]{this.bladePath, "prepare", "jvm", "--pid", this.pid}));
        String stdOut = CmdExecutor.run("blade-prepare-jvm", cmdArgs, 60000, 5);
        BladeResult r = new BladeResult(stdOut);
        jvmID = r.result;
    }
    
    public String makeSendDataException(String classPath, String methodName) throws IOException {
        ArrayList cmdArgs = new ArrayList<>(Arrays.asList(new String[]{this.bladePath, "create", "jvm", "throwCustomException"
                ,"--exception", "java.lang.RuntimeException", "--exception-message", "mock exception"
                , "--classname", classPath, "--methodname", methodName
                , "--pid", this.pid}));

        String stdOut = CmdExecutor.run("blade-send-data-exception", cmdArgs, 10000, 5);
        BladeResult r = new BladeResult(stdOut);
        return r.result;
    }

    public String makeSendDataSlow(String classPath, String methodName, long t) throws IOException {
        ArrayList cmdArgs = new ArrayList<>(Arrays.asList(new String[]{this.bladePath, "create", "jvm", "delay"
                ,"--time", String.valueOf(t)
                , "--classname", classPath, "--methodname", methodName,
                "--pid", this.pid}));

        String stdOut = CmdExecutor.run("blade-send-data-slow", cmdArgs, 10000, 5);
        BladeResult r = new BladeResult(stdOut);
        return r.result;
    }

    public void revoke(String id) throws IOException {
        ArrayList cmdArgs = new ArrayList<>(Arrays.asList(new String[]{this.bladePath, "d", id}));
        CmdExecutor.run("blade-revoke", cmdArgs, 10000, 5);
    }

    public void destroy() throws IOException {
        ArrayList cmdArgs = new ArrayList<>(Arrays.asList(new String[]{this.bladePath, "r", this.jvmID}));
        CmdExecutor.run("blade-destroy", cmdArgs, 10000, 5);
    }

    private class BladeResult {
        private int code;
        private Boolean success;
        private String result;
        private BladeResult(String output) {
            Gson gson = new Gson();
            BladeResult r = gson.fromJson(output, BladeResult.class);
            this.code = r.code;
            this.success = r.success;
            this.result = r.result;
        }
    }
}
