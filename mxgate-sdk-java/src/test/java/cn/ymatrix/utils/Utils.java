package cn.ymatrix.utils;

import cn.ymatrix.api.ColumnMeta;
import cn.ymatrix.api.JobMetadata;

public class Utils {
    public static JobMetadata prepareJobMetadata(String schema, String table, String delimiter, int columnSize, String columnPrefix) {
        JobMetadata.Builder builder = JobMetadata.newBuilder();
        builder.setSchema(schema);
        builder.setTable(table);
        builder.setDelimiter(delimiter);
        for (int i = 0; i < columnSize; i++) {
            ColumnMeta.Builder columnBuilder = ColumnMeta.newBuilder();
            if (i == 1) {
                // Not null and has default value.
                columnBuilder.setName(columnPrefix + i).setType("Text").setNum(i + 1).setDefault("default_c1").setIsNotNull(true);
            } else if (i == 2) {
                // Not default value but not null.
                columnBuilder.setName(columnPrefix + i).setType("Text").setNum(i + 1).setIsNotNull(true);
            } else {
                columnBuilder.setName(columnPrefix + i).setType("Text").setNum(i + 1);
            }
            builder.addColumns(columnBuilder.build());
        }
        return builder.build();
    }


}
