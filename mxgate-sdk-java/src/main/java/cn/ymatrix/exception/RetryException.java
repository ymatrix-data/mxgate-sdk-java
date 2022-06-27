package cn.ymatrix.exception;

public class RetryException extends RuntimeException {
    public RetryException(String cause) {
        super(cause);
    }
}
