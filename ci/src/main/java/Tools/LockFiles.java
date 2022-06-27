package Tools;

import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class LockFiles {
    private final Logger l = TestLogger.init(LockFiles.class);

    final static private String tmpDir = "/tmp";
    private ArrayList<LockFile> lockFiles= new ArrayList<>();

    private static class LockFile {
        private final Logger l = TestLogger.init(LockFile.class);

        private boolean hidden;
        private String filePath;
        private String logFilePath;
        private String serverIdentity;
        private String cfgFilePath;

        private Integer pid;
        private Integer perfPort;
        private Integer grpcPort;
        private Integer httpPort;

        private  boolean invalid; // something wrong with lock file

        private LockFile(final Path filePath) {
            List<String> lines;
            try {
                lines = Files.readAllLines(filePath);
            } catch (IOException e) {
                l.warn("failed to read file {}: {}", filePath, e.getMessage());
                return;
            }

            if (lines.isEmpty()) {
                return;
            }

            if (lines.get(0).startsWith("#")) {
                this.hidden = true;
                return;
            }
            try {
                this.pid = Integer.parseInt(lines.get(0));
            } catch (NumberFormatException e) {
                this.invalid = true;
                l.warn("invalid pid in lockfile: line 1 {}", lines.get(0));
                return;
            }
            if (this.pid < 1) {
                this.invalid = true;
                l.warn("invalid pid: line 1 {}", lines.get(0));
                return;
            }

            if (lines.size() < 2) {
                return;
            }
            this.logFilePath = lines.get(1);

            if (lines.size() < 3) {
                return;
            }
            try {
                this.perfPort = Integer.parseInt(lines.get(2));
            } catch (NumberFormatException e) {
                this.invalid = true;
                l.warn("invalid perf_port in lockfile: line 3 {}", lines.get(2));
                return;
            }

            if (lines.size() < 4) {
                return;
            }
            this.serverIdentity= lines.get(3);

            String[] parts = this.serverIdentity.split(Pattern.quote("."));
            if (parts.length == 2 && parts[0].length() > 0) {
                try {
                    this.httpPort = Integer.parseInt(parts[0]);
                } catch (NumberFormatException e){
                    l.warn("invalid http_port in lockfile: line 4 {}", this.serverIdentity);
                }
            }

            if (lines.size() < 5) {
                return;
            }
            try {
                this.grpcPort = Integer.parseInt(lines.get(4));
            } catch (NumberFormatException e) {
                this.invalid = true;
                l.warn("invalid grpc_port in lockfile: line 5 {}", lines.get(2));
                return;
            }

            if (lines.size() < 6) {
                return;
            }
            this.cfgFilePath = lines.get(5);
        }
    }

    public LockFiles() {
//        System.setProperty("user.dir", tmpDir);

        final File tmpDir = new File(LockFiles.tmpDir);
        final File[] tmpFiles = tmpDir.listFiles();

        for (File f: tmpFiles) {
            if (f.isDirectory()) {
                continue;
            }

            if (!f.getName().startsWith(".s.MXGATED.")) {
                continue;
            }
            if (!f.getName().endsWith(".lock")) {
                continue;
            }

            LockFile lockFile = new LockFile(f.toPath());
            if (lockFile.hidden) {
                continue;
            }
            if (lockFile.invalid) {
                continue;
            }
            lockFile.filePath = f.getAbsolutePath();
            this.lockFiles.add(lockFile);
        }

        if (this.lockFiles.size() <= 0) {
            l.error("mxgate daemon not running");
            System.out.printf("mxgate daemon not running");
            System.exit(1);
        }
    }

    public int getOneGrpcPort() {
        for(LockFile f: this.lockFiles) {
            if (f.grpcPort > 0) {
                return f.grpcPort;
            }
        }

        return -1;
    }

    public int getOneHttpPort() {
        for(LockFile f: this.lockFiles) {
            if (f.httpPort != null && f.httpPort > 0) {
                return f.httpPort;
            }
        }

        return 8086; // default
    }
}
