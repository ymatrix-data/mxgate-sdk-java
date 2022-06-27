package cn.ymatrix.utils;

import cn.ymatrix.concurrencycontrol.WorkerPool;
import cn.ymatrix.concurrencycontrol.WorkerPoolFactory;
import cn.ymatrix.data.Tuples;
import cn.ymatrix.logger.MxLogger;
import org.slf4j.Logger;

import java.text.DecimalFormat;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class ThroughoutCalculator {
    private static final Logger l = MxLogger.init(ThroughoutCalculator.class);

    private static final String TAG = StrUtil.logTagWrap(ThroughoutCalculator.class.getName());
    private final AtomicLong tupleSize; // Input tuples per second.

    private final AtomicLong bandwidth; // Input bytes size of the tuples per second.

    private final AtomicBoolean stop;

    private final int intervalMillis;

    private final WorkerPool pool;

    private final DecimalFormat df;

    private final CSVConstructor constructor;

    public static ThroughoutCalculator getInstance() {
        return InnerClass.instance;
    }

    private static class InnerClass {
        private static final ThroughoutCalculator instance = new ThroughoutCalculator();
    }

    private ThroughoutCalculator() {
        this.stop = new AtomicBoolean(false);
        this.tupleSize = new AtomicLong(0);
        this.bandwidth = new AtomicLong(0);
        this.intervalMillis = 3000;
        this.pool = WorkerPoolFactory.initFixedSizeWorkerPool(1);
        this.df = new DecimalFormat("#.00");
        this.constructor = new CSVConstructor(Runtime.getRuntime().availableProcessors());
        this.run();
    }

    private void run() {
        this.pool.join(new Runnable() {
            @Override
            public void run() {
                ThroughoutCalculator.this.calculate();
            }
        });
    }
    private long tuplesPerSec;
    private long bytesPerSec;
    private double MBPerSec;
    private double KBPerSec;

    private void calculate() {
        while (!this.stop.get()) {
            calculateOnce();
        }
    }

    private void calculateOnce() {
        long startTupleSize = this.tupleSize.get();
        long startBandwidth = this.bandwidth.get();
        try {
            Thread.sleep(intervalMillis);
        } catch (InterruptedException e) {
            l.error("{} calculate cycle thread sleep exception ", TAG, e);
        }
        printDiff(startTupleSize, startBandwidth, tupleSize.get(), bandwidth.get());
    }

    private void printDiff(long startTupleSize, long endTupleSize, long startBandwidth, long endBandWidth) {
        tuplesPerSec = (endTupleSize - startTupleSize) / (intervalMillis / 1000); // Transform from millis to second.
        bytesPerSec = (endBandWidth - startBandwidth) / (intervalMillis / 1000); // Transform from millis to second.
        MBPerSec = (double) bytesPerSec / 1000 / 1000;
        KBPerSec = (double) bytesPerSec / 1000;
        if (MBPerSec > 1) {
            l.info("{} Input {} Tuples / sec , {} MB / sec", TAG, tuplesPerSec, df.format(MBPerSec));
        } else if (KBPerSec > 1) {
            l.info("{} Input {} Tuples / sec , {} KB / sec", TAG, tuplesPerSec, df.format(KBPerSec));
        } else {
            l.info("{} Input {} Tuples / sec , {} bytes / sec", TAG, tuplesPerSec, bytesPerSec);
        }
    }

    public void add(Tuples tuples) {
        if (tuples != null) {
            this.tupleSize.addAndGet(tuples.size());
            StringBuilder rawCSVData = constructor.constructCSVFromTuplesWithTasks(tuples.getTuplesList(), tuples.getCSVBatchSize(), tuples.getDelimiter());
            if (rawCSVData == null) {
                l.error("Get null CSV Data from constructor.");
                return;
            }
            this.bandwidth.addAndGet(rawCSVData.toString().getBytes().length);
        }
    }

    public void stop() {
        this.stop.set(true);
        this.pool.shutdown();
    }


}
