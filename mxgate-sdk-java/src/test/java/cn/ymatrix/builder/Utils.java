package cn.ymatrix.builder;

import cn.ymatrix.apiclient.MxClient;
import cn.ymatrix.data.Tuple;

public class Utils {
    public static Tuple generateEmptyTupleLite(String delimiter, String schema, String table) {
        return new TupleImplLite(delimiter, schema, table);
    }

}
