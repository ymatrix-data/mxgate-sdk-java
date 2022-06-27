package cn.ymatrix.exception;

import cn.ymatrix.apiclient.Result;

public class AllTuplesFailException extends RuntimeException {
    private Result result;

    public AllTuplesFailException(String message) {
        super(message);
    }

    public Result getResult() {
        return result;
    }

    public void setResult(Result result) {
        this.result = result;
    }
}
