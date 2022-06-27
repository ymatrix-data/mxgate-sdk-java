package Tools;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;

public class FindExecutable {

    public FindExecutable() {}

    public String lookPath(String fileName) {
        String systemPath = System.getenv("PATH");
        String[] pathDirs = systemPath.split(File.pathSeparator);

        String filePath = null;
        for (String pathDir : pathDirs) {
            Path onePath = Paths.get(pathDir, fileName);

            if (canExecute(onePath.toString())) {
                filePath = onePath.toString();
                break;
            }
        }
        return filePath;
    }

    public boolean canExecute(String filePath) {
        File file = new File(filePath);
        return file.isFile() && file.canExecute();
    }
}
