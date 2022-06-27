package cn.ymatrix.utils;

import cn.ymatrix.data.Column;
import cn.ymatrix.data.Tuple;
import cn.ymatrix.logger.MxLogger;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.slf4j.Logger;

import java.io.IOException;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class CSVConstructor {
    private static final String TAG = StrUtil.logTagWrap(CSVConstructor.class.getName());
    private static final Logger l = MxLogger.init(CSVConstructor.class);
    private final ForkJoinPool joinPool;
    private final int joinPoolParallel;

    public CSVConstructor(int parallel) throws InvalidParameterException {
        if (parallel <= 0) {
            l.error("{} ForkJoinPool parallel must be positive.", TAG);
            throw new InvalidParameterException("ForkJoinPool parallel must be positive.");
        }
        this.joinPool = new ForkJoinPool(parallel);
        this.joinPoolParallel = parallel;
        l.info("{} Init CSV Constructor with parallel == {} in ForkJoinPool", TAG, this.joinPool.getParallelism());
    }

    public int getJoinPoolParallel() {
        return this.joinPoolParallel;
    }

    private static CSVPrinter generateCSVPrinter(StringBuilder sb, CSVFormat csvFormat) {
        try {
            return new CSVPrinter(sb, csvFormat);
        } catch (IOException e) {
            l.error("{} CSVPrinter creation exception ", TAG, e);
            return null;
        }
    }

    public StringBuilder constructCSVFromTuplesWithTasks(List<Tuple> tuplesList, int batch, String delimiter) throws NullPointerException, InvalidParameterException {
        if (tuplesList == null) {
            l.error("{} Tuples list is null in CSV construction.", TAG);
            throw new NullPointerException("Tuples list is null");
        }

        // No need to construct the CSV.
        if (tuplesList.size() == 0) {
            return null;
        }

        if (StrUtil.isNullOrEmpty(delimiter)) {
            l.error("{} Delimiter is empty in CSV construction", TAG);
            throw new InvalidParameterException("Delimiter is empty");
        }

        if (batch <= 0) {
            l.error("{} Batch must be positive", TAG);
            throw new InvalidParameterException("Batch must be positive");
        }

        if (this.joinPool == null) {
            l.error("{} ForkJoinPool for CSV construction if null", TAG);
            throw new NullPointerException("ForkJoinPool for CSV construction if null");
        }

        int size = tuplesList.size();
        ForkJoinTask<StringBuilder> submitTask = joinPool.submit(new SubTask(tuplesList, 0, size, batch, delimiter));

        StringBuilder result;
        try {
            result = submitTask.get();
        } catch (InterruptedException | ExecutionException e) {
            l.error("{} Get constructed CSV result from submit task exception", TAG, e);
            throw new RuntimeException(e);
        }

        return result;
    }

    private static class SubTask extends RecursiveTask<StringBuilder> {
        private final List<Tuple> tupleList;
        private final int startRow;
        private final int endRow;
        private final int batch;
        private final int totalSize;
        private final String delimiter;

        public SubTask(List<Tuple> list, int startRow, int endRow, int batch, String delimiter) {
            this.tupleList = list;
            this.startRow = startRow;
            this.endRow = endRow;
            this.batch = batch;
            this.totalSize = list.size();
            this.delimiter = delimiter;
        }

        private StringBuilder run(List<Tuple> tuples) {
            StringBuilder sb = new StringBuilder();
            CSVFormat csvFormat = CSVFormat.POSTGRESQL_CSV.builder()
                    .setDelimiter(this.delimiter)
                    .setRecordSeparator('\n').build();

            try (CSVPrinter csvPrinter = generateCSVPrinter(sb, csvFormat)) {
                if (csvPrinter == null) {
                    l.error("{} Get a nullable CSVPrinter", TAG);
                    return null;
                }

                for (Tuple tuple : tuples) {
                    if (tuple == null || tuple.getColumns() == null) {
                        l.error("{} Tuple is empty for CSV construction.", TAG);
                        continue;
                    }
                    List<String> csvColumnRecords = new ArrayList<>();
                    for (Column col : tuple.getColumns()) {
                        if (col.shouldSkip()) {
                            continue;
                        }
                        String csvStr = col.toString();
                        if (StrUtil.isNullOrEmpty(csvStr)) {
                            csvColumnRecords.add(null);
                            continue;
                        }
                        csvColumnRecords.add(csvStr);
                    }
                    try {
                        csvPrinter.printRecord(csvColumnRecords);
                    } catch (IOException e) {
                        l.error("{} CSVPrinter print record exception ", TAG, e);
                        return null;
                    }
                }

                // No CSV string that has been printed into this StringBuilder;
                if (sb.length() == 0) {
                    return null;
                }
                return sb;
            } catch (IOException e) {
                l.error("{} CSVPrinter print with IOException ", TAG, e);
                return null;
            }
        }

        @Override
        protected StringBuilder compute() {
            // Tuple size is equal or less than a batch.
            if (this.endRow - this.startRow <= this.batch) {
                return this.run(this.tupleList.subList(this.startRow, this.endRow));
            }

            // split subtasks recursively.
            int loopNum = (int) Math.ceil((double) totalSize / batch);
            int startIdx = 0;
            List<SubTask> subTasks = new ArrayList<>();
            for (int i = 0; i < loopNum; i++) {
                if (startIdx > this.totalSize) {
                    break;
                }
                int endIdx = Math.min(startIdx + this.batch, this.totalSize);
                subTasks.add(new SubTask(this.tupleList, startIdx, endIdx, this.batch, this.delimiter));
                startIdx += batch;
            }
            invokeAll(subTasks);

            // Append all the StringBuilders from subtasks and return.
            StringBuilder sbs = new StringBuilder();
            for (SubTask subTask : subTasks) {
                StringBuilder sb = subTask.join();
                if (sb == null) {
                    l.error("{} Get a nullable StringBuilder from subTask, will return null result in compute() of RecursiveTask for CSV construction.", TAG);
                    return null;
                }
                sbs.append(subTask.join());
            }
            return sbs;
        }
    }

}
