package cn.ymatrix.api;

import cn.ymatrix.utils.Utils;
import org.junit.Ignore;
import org.junit.Test;

public class TestJobMetadataWrapper {
    private static final String schema = "public";
    private static final String table = "test_table";
    private static final String delimiter = "|";

    @Test
    public void TestWrapJobMetadata() {
        JobMetadataWrapper wrapper = JobMetadataWrapper.wrapJobMetadata(Utils.prepareJobMetadata(schema, table, delimiter, 5, "c"));
    }

    @Ignore
    @Test
    public void TestWrapJobMetadataStrictlyCheck() {
        // TODO
    }






}
