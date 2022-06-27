package cn.ymatrix.benchmark;

import cn.ymatrix.logger.MxLogger;
import cn.ymatrix.utils.StrUtil;
import org.slf4j.Logger;

public class Benchmark {
    private static final Logger l = MxLogger.init(Benchmark.class);
    private static final String originalStr = "CAP(“全容”或者“预测\"或者“简化”请其中一个)";

    public static void main(String[] args) {
        long start1 = System.currentTimeMillis();
        for (int i = 0; i < 10000000; i++) {
            String qs = quoteSlowBenchmark();
        }
        long end1 = System.currentTimeMillis();
        l.info("quote slow time cost = {}", end1 - start1);

        long start = System.currentTimeMillis();
        for (int i = 0; i < 10000000; i++) {
            String q = quoteBenchmark();
        }
        long end = System.currentTimeMillis();
        l.info("quote time cost = {}", end - start);
    }

    private static String quoteBenchmark() {
        return StrUtil.quote(originalStr);
    }

    private static String quoteSlowBenchmark() {
        return StrUtil.quoteSlow(originalStr);
    }


}
