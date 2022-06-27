package cn.ymatrix.apiclient;

/**
 * The callback for MxClient to listen the data sending Result.
 */
public interface DataPostListener {
    void onSuccess(Result result);

    void onFailure(Result result);
}
