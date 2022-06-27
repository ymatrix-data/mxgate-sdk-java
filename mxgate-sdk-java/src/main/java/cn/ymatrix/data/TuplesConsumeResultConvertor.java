package cn.ymatrix.data;

import cn.ymatrix.logger.MxLogger;
import cn.ymatrix.utils.StrUtil;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TuplesConsumeResultConvertor {
    private static final String TAG = StrUtil.logTagWrap(TuplesConsumeResultConvertor.class.getName());
    private static final Logger l = MxLogger.init(TuplesConsumeResultConvertor.class);

    public static int convertSucceedTuplesLines(Map<Long, String> errorLinesMap, Tuples tuples) {
        // No error lines need to be converted.
        if (tuples == null) {
            return 0;
        }

        // ErrorLinesMap is not exists, all tuples are succeed.
        if (errorLinesMap == null) {
            return tuples.size();
        }

        // All of tuples in this Tuples are fail, return all of them.
        boolean allTuplesFail = (errorLinesMap.size() == tuples.size());
        if (allTuplesFail) {
            return 0;
        }
        return tuples.size() - errorLinesMap.size();
    }

    public static List<Long> convertSuccessfulSerialNums(Map<Long, String> errorLinesMap, Tuples tuples) {
        List<Long> numList = new ArrayList<>();

        if (tuples == null) {
            return numList;
        }

        // Partially tuples fail.
        int length = tuples.size();
        for (int i = 0; i < length; i++) {
            // All tuples are successful.
            if (errorLinesMap == null) {
                // This is a successful line, add to line number.
                Tuple successTuple = tuples.getTupleByIndex(i);
                if (successTuple != null) {
                    numList.add(successTuple.getSerialNum());
                }
                continue;
            }

            // Some tuples failed.
            long lineNum = i + 1; // Convert tuple index to line number.
            if (errorLinesMap.get(lineNum) != null) { // This is an error line, skip.
                continue;
            }
            // This is a successful line, add to line number.
            Tuple successTuple = tuples.getTupleByIndex(i);
            if (successTuple != null) {
                numList.add(successTuple.getSerialNum());
            }
        }
        return numList;
    }

    public static Map<Tuple, String> convertErrorTuples(Map<Long, String> errorLinesMap, Tuples tuples, String msg) {
        Map<Tuple, String> map = new HashMap<>();

        // No need to convert, return an empty Map.
        if (tuples == null) {
            return map;
        }

        // All of tuples in this Tuples are fail, return all of them.
        boolean allTuplesFail = false;
        // No errorLinesMap only tuples, will think all the tuples are failed.
        if (errorLinesMap == null) {
            allTuplesFail = true;
        } else {
            allTuplesFail = (errorLinesMap.size() == tuples.size());
        }

        if (allTuplesFail) {
            int size = tuples.size();
            for (int i = 0; i < size; i++) {
                Tuple tuple = tuples.getTupleByIndex(i);
                if (tuple != null) {
                    map.put(tuple, msg);
                }
            }
            return map;
        }

        // Partially tuples fail.
        for (Map.Entry<Long, String> entry : errorLinesMap.entrySet()) {
            if (entry != null) {
                try {
                    // Convert the line_number to index of the tuples.
                    int index = Math.toIntExact(entry.getKey() - 1);
                    Tuple tuple = tuples.getTupleByIndex(index);
                    if (tuple != null) {
                        map.put(tuple, entry.getValue());
                    }
                } catch (Exception e) { // Catch all the exceptions.
                    l.error("{} Convert error tuples map exception {}", TAG, e.getMessage());
                    e.printStackTrace();
                }
            }
        }
        return map;
    }
}
