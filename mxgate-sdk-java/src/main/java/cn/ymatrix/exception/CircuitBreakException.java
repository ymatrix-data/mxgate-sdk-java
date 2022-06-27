package cn.ymatrix.exception;

public class CircuitBreakException extends RuntimeException {
    public CircuitBreakException(String errMsg) {
        super(errMsg);
    }
}
