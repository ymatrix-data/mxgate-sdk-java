package cn.ymatrix.utils;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class TestStrUtils {
    @Test
    public void TestStrUtilIsNullableOrEmpty() {
        Assert.assertTrue(StrUtil.isNullOrEmpty(""));
        Assert.assertTrue(StrUtil.isNullOrEmpty(null));
        Assert.assertFalse(StrUtil.isNullOrEmpty("Not empty"));
    }

    @Test
    public void TestStrUtilTagWrap() {
        String tag = "This is tag";
        Assert.assertEquals(StrUtil.logTagWrap(tag), "[" + tag + "]");
    }

    @Test
    public void TestStrUtilConnect() {
        String str1 = "str1";
        String str2 = "str2";
        String str3 = "str3";
        List<String> stringList = new ArrayList<>();
        stringList.add(str1);
        stringList.add(str2);
        stringList.add(str3);
        Assert.assertEquals(StrUtil.connect(stringList), str1 + str2 + str3);

        List<String> stringList1 = new ArrayList<>();
        Assert.assertEquals("", StrUtil.connect(stringList1));

        Assert.assertEquals(StrUtil.connect(str1, str2, str3), str1 + str2 + str3);
        Assert.assertEquals(StrUtil.connect(str1, "", str3), str1 + str3);
    }

    @Test
    public void TestStrUtilQuote() {
        String originalStr = "\" this is a \" string \" with quote \"";
        String expected = "\"\"\" this is a \"\" string \"\" with quote \"\"\"";
        Assert.assertEquals(StrUtil.quote(originalStr), expected);
    }

    @Test
    public void TestStrUtilCSVStrEmpty() {
        Assert.assertTrue(StrUtil.isCSVStrEmpty(null));
        Assert.assertTrue(StrUtil.isCSVStrEmpty(""));
        Assert.assertTrue(StrUtil.isCSVStrEmpty("\""));
        Assert.assertTrue(StrUtil.isCSVStrEmpty("\"\""));
        Assert.assertFalse(StrUtil.isCSVStrEmpty("\" This is not empty\""));
    }

}
