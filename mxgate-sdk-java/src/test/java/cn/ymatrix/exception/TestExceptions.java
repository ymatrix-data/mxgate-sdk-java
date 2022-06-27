package cn.ymatrix.exception;

import cn.ymatrix.apiclient.Result;
import cn.ymatrix.apiclient.ResultStatus;
import org.junit.Assert;
import org.junit.Test;

public class TestExceptions {

    @Test(expected = AllTuplesFailException.class)
    public void TestAllTuplesFailException() {
        try {
            AllTuplesFailException exception = new AllTuplesFailException("All tuples fail exception throw.");
            Result result = new Result();
            result.setStatus(ResultStatus.SUCCESS);
            exception.setResult(result);
            throw exception;
        } catch (AllTuplesFailException e) {
            Assert.assertEquals(e.getResult().getStatus(), ResultStatus.SUCCESS);
            throw e;
        }
    }

    @Test(expected = PartiallyTuplesFailException.class)
    public void TestPartiallyTuplesException() {
        try {
            PartiallyTuplesFailException exception = new PartiallyTuplesFailException("Partially tuples fail exception throw.");
            Result result = new Result();
            result.setStatus(ResultStatus.FAILURE);
            exception.setResult(result);
            throw exception;
        } catch (PartiallyTuplesFailException e) {
            Assert.assertEquals(e.getResult().getStatus(), ResultStatus.FAILURE);
            throw e;
        }
    }

    @Test(expected = BrokenTuplesException.class)
    public void TestBrokenTuplesException() {
        String exceptionMsg = "Broken Tuples Exception";
        ExceptionThrown(new BrokenTuplesException(exceptionMsg), exceptionMsg);
    }

    @Test(expected = CircuitBreakException.class)
    public void TestCircuitBreakException() {
        String exceptionMsg = "Circuit break exception";
        ExceptionThrown(new CircuitBreakException(exceptionMsg), exceptionMsg);
    }

    @Test(expected = ClientClosedException.class)
    public void TestClientCloseException() {
        String exceptionMsg = "Client close exception";
        ExceptionThrown(new ClientClosedException(exceptionMsg), exceptionMsg);
    }

    @Test(expected = ConnectTimeoutException.class)
    public void TestConnectionTimeoutException() {
        String exceptionMsg = "Connect timeout exception";
        ExceptionThrown(new ConnectTimeoutException(exceptionMsg), exceptionMsg);
    }

    @Test(expected = EnqueueException.class)
    public void TestEnqueueException() {
        String exceptionMsg = "Enqueue exception";
        ExceptionThrown(new EnqueueException(exceptionMsg), exceptionMsg);
    }

    @Test(expected = RetryException.class)
    public void TestRetryException() {
        String exceptionMsg = "Retry exception";
        ExceptionThrown(new RetryException(exceptionMsg), exceptionMsg);
    }

    @Test(expected = TupleNotReadyException.class)
    public void TestTupleNotReadyException() {
        String exceptionMsg = "Tuple not ready exception";
        ExceptionThrown(new TupleNotReadyException(exceptionMsg), exceptionMsg);
    }

    @Test(expected = WorkerPoolShutdownException.class)
    public void TestWorkPoolShutdownException() {
        String exceptionMsg = "Work pool shutdown exception";
        ExceptionThrown(new WorkerPoolShutdownException(exceptionMsg), exceptionMsg);
    }

    private void ExceptionThrown(RuntimeException e, String expectedMsg) {
        try {
            throw e;
        } catch (RuntimeException re) {
            Assert.assertEquals(re.getMessage(), expectedMsg);
            throw re;
        }
    }
}
