package cn.ymatrix.apiclient;

import cn.ymatrix.data.Tuple;
import cn.ymatrix.data.Tuples;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The callback result of MxClient DataPostListener to
 * Wrap information of data sending result.
 */
public class Result {

    private ResultStatus status;

    private Map<Tuple, String> errorTuplesMap;

    private String msg;

    private int SuccessLines;

    private List<Long> successLinesSerialNumList;

    private Tuples rawTuples;

    public ResultStatus getStatus() {
        return status;
    }

    public void setStatus(ResultStatus status) {
        this.status = status;
    }

    public Result() {

    }

    public Map<Tuple, String> getErrorTuplesMap() {
        if (this.errorTuplesMap == null) {
            return new HashMap<>();
        }
        return errorTuplesMap;
    }

    public void setErrorTuplesMap(Map<Tuple, String> errorTuplesMap) {
        this.errorTuplesMap = errorTuplesMap;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public int getSucceedLines() {
        return SuccessLines;
    }

    public void setSucceedLines(int successLines) {
        SuccessLines = successLines;
    }

    public List<Long> getSuccessLinesSerialNumList() {
        return successLinesSerialNumList;
    }

    public void setSuccessLinesSerialNumList(List<Long> successLinesSerialNumList) {
        this.successLinesSerialNumList = successLinesSerialNumList;
    }

    public Tuples getRawTuples() {
        return rawTuples;
    }

    public void setRawTuples(Tuples rawTuples) {
        this.rawTuples = rawTuples;
    }
}
