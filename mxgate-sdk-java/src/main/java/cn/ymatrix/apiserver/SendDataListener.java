package cn.ymatrix.apiserver;

import cn.ymatrix.data.Tuples;

public interface SendDataListener {

    void onSuccess(SendDataResult result, Tuples tuples);

    void onFailure(SendDataResult result, Tuples tuples);
}
