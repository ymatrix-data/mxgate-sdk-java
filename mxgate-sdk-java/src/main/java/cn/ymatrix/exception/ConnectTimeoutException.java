package cn.ymatrix.exception;

public class ConnectTimeoutException extends RuntimeException {
    public ConnectTimeoutException(String msg) {
        super(msg);
    }
}
