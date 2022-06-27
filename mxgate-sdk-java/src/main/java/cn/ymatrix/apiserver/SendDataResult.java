package cn.ymatrix.apiserver;

import cn.ymatrix.api.StatusCode;

import java.util.Map;

public class SendDataResult {
    private final String msg;

    private final StatusCode code;

    private final Map<Long, String> errorLinesMapping;

    public SendDataResult(StatusCode code, Map<Long, String> errorLinesMapping, String msg) {
        this.code = code;
        this.errorLinesMapping = errorLinesMapping;
        this.msg = msg;
    }

    public String getMsg() {
        return msg;
    }

    public StatusCode getCode() {
        return code;
    }

    public Map<Long, String> getErrorLinesMap() {
        return errorLinesMapping;
    }
}
