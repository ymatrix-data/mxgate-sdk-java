package cn.ymatrix.exception;

public class ClientClosedException extends RuntimeException {

    public ClientClosedException(String msg) {
        super(msg);
    }

    public ClientClosedException(String msg, Throwable cause) {
        super(msg, cause);
    }


}
