package cn.ymatrix.utils;

import org.apache.commons.lang3.StringUtils;

import java.sql.Date;
import java.util.List;

public class StrUtil {
    public static boolean isNullOrEmpty(String target) {
        return target == null || target.isEmpty();
    }

    public static String logTagWrap(String tag) {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        sb.append(tag);
        sb.append("]");
        return sb.toString();
    }

    public static String connect(List<String> stringList) {
        StringBuilder sb = new StringBuilder();
        for (String str : stringList) {
            sb.append(str);
        }
        return sb.toString();
    }

    public static String connect(String... strings) {
        StringBuilder sb = new StringBuilder();
        for (String str : strings) {
            sb.append(str);
        }
        return sb.toString();
    }

    public static String quote(String str) {
        StringBuilder sb = new StringBuilder();
        sb.append("\"");
        sb.append(StringUtils.replace(str, "\"", "\"\""));
        sb.append("\"");
        return sb.toString();
    }

    /**
     * This method is in bad performance, deprecated.
     * Only used for benchmark with quote().
     * @param str
     * @return
     */
    @Deprecated
    public static String quoteSlow(String str) {
        StringBuilder sb = new StringBuilder();
        sb.append("\"");
        sb.append(str.replaceAll("\"", "\"\""));
        sb.append("\"");
        return sb.toString();
    }

    // For CSV string, the empty one will contain "" which length will be 2.
    public static boolean isCSVStrEmpty(String csv) throws NullPointerException {
        if (csv == null) {
            return true;
        }
        return csv.length() <= 2;
    }
}
