package cn.ymatrix.apiserver;

import cn.ymatrix.api.JobMetadataWrapper;

public interface GetJobMetadataListener {

    void onSuccess(JobMetadataWrapper metadata);

    void onFailure(String failureMsg);

}
