package Tools;

import org.slf4j.Logger;

import java.io.*;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

public class CmdExecutor {
    private static final Logger l = TestLogger.init(CmdExecutor.class);

    public static String run(String cmdName, ArrayList<String> args, long waitTimeMillis, int stdOutLineCnt) throws IOException {
        Instant t1 = Instant.now();

        ProcessBuilder builder = new ProcessBuilder(args);
        Process process = builder.start();

        try {
            process.waitFor(waitTimeMillis, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            l.error("wait for {} get interrupted: {}", cmdName, e.getMessage());
        }

        String stdOut;
        String stdErr;
        try {
            stdOut = readFixedLines(new BufferedReader(new InputStreamReader(process.getInputStream())), stdOutLineCnt);
            stdErr = readFixedLines(new BufferedReader(new InputStreamReader(process.getErrorStream())), stdOutLineCnt);
        } catch (IOException e) {
            l.error("failed to read output and error of {}: {}", cmdName, e.getMessage());
            throw e;
        }

        int exitCode = 0;
        try {
            exitCode = process.exitValue();
        } catch (IllegalThreadStateException e) {
            // expected, nothing
        }
        if (exitCode != 0 || stdErr.length() > 0) {
            throw new RuntimeException(String.format("%s failed: exit %d\n stdout:\n\t%s\nstderr:\n\t%s", cmdName, exitCode, stdOut, stdErr));
        }

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd hh:mm:ss:SSS")
                .withZone(ZoneId.systemDefault());
        l.info("==== {} Duration ==== {}, since {}", cmdName, Duration.between(t1, Instant.now()), formatter.format(t1));
        l.info("====  STDOUT  ====\n{}", stdOut);
        l.info("====  STDERR  ====\n{}", stdErr);
        return stdOut;
    }

    private static String readFixedLines(BufferedReader r, final int lineCnt) throws IOException {
        int curCnt = 0;
        StringBuilder outputStr = new StringBuilder();

        String line;
        try {
            while((line = r.readLine()) != null){
                if (curCnt>=lineCnt) {
                    break;
                }
                outputStr.append(line+System.lineSeparator());
                curCnt ++;
            }
        } catch (IOException e) {
            throw e;
        }

        return outputStr.toString().trim();
    }
}
