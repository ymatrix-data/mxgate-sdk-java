package cn.ymatrix.exception;

/**
 * Worker Pool has been shutdown.
 */
public class WorkerPoolShutdownException extends RuntimeException {
    public WorkerPoolShutdownException(String message) {
        super(message);
    }
}
