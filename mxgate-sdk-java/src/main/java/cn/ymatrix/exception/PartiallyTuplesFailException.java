package cn.ymatrix.exception;

import cn.ymatrix.apiclient.Result;

public class PartiallyTuplesFailException extends RuntimeException {
    private Result result;

    public PartiallyTuplesFailException(String message) {
        super(message);
    }

    public Result getResult() {
        return result;
    }

    public void setResult(Result result) {
        this.result = result;
    }
}
