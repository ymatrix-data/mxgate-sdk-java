package cn.ymatrix.data;

import cn.ymatrix.utils.StrUtil;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestColumn {

    @Test
    public void testStringConnect() {
        String prefix = "pre_";
        String suffix = "_sub";
        String content = "content";
        List<String> list = new ArrayList<>();
        list.add(prefix);
        list.add(content);
        list.add(suffix);
        String resultStr = StrUtil.connect(list);
        assertEquals("pre_content_sub", resultStr);
    }

    @Test
    public void testStringQuote() {
        String targetStr = "123456";
        String resultStr = StrUtil.quote(targetStr);
        assertEquals("\"123456\"", resultStr);
    }

    @Test
    public void testElement() {
        // Int element
        Column<Integer> pointInt = ColumnsFactory.columnInt(1000);
        assertEquals("\"1000\"", pointInt.toCSV());

        // String element
        Column<String> pointStr = ColumnsFactory.columnString("this is a string");
        assertEquals(pointStr.toCSV(), "\"this is a string\"");

        // Float element
        Column<Float> pointFloat = ColumnsFactory.columnFloat(1.25F);
        assertEquals(pointFloat.toCSV(), "\"1.25\"");

        // Double element
        Column<Double> pointDouble = ColumnsFactory.columnDouble(1.25);
        assertEquals(pointDouble.toCSV(), "\"1.25\"");
    }

    @Test
    public void testRawElement() {
        // TODO
    }


}
