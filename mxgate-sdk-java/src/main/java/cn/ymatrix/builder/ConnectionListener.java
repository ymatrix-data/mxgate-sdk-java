package cn.ymatrix.builder;

import cn.ymatrix.apiclient.MxClient;

public interface ConnectionListener {

    void onSuccess(MxClient client);

    void onFailure(String failureMsg);
}
