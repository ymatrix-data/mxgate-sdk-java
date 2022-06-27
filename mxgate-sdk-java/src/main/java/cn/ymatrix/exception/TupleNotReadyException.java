package cn.ymatrix.exception;

public class TupleNotReadyException extends RuntimeException {
    public TupleNotReadyException(String exceptionMsg) {
        super(exceptionMsg);
    }

    public TupleNotReadyException(String exceptionMsg, Throwable t) {
        super(exceptionMsg, t);
    }
}
