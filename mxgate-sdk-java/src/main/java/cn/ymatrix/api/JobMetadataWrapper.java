package cn.ymatrix.api;

import cn.ymatrix.logger.MxLogger;
import cn.ymatrix.utils.StrUtil;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;

public class JobMetadataWrapper {
    private static final String TAG = StrUtil.logTagWrap(JobMetadataWrapper.class.getName());
    private static final Logger l = MxLogger.init(JobMetadataWrapper.class);

    public static JobMetadataWrapper wrapJobMetadata(JobMetadata metadata) throws NullPointerException {
        jobMetadataStrictlyCheck(metadata);
        return new JobMetadataWrapper(metadata);
    }

    private static void jobMetadataStrictlyCheck(JobMetadata metadata) throws NullPointerException {
        if (metadata == null) {
            throw new NullPointerException("wrap on an null JobMetadata.");
        }
        l.debug("{} Get job metadata {}", TAG, metadata);

        if (metadata.getColumnsList().isEmpty()) {
            String errMsg = StrUtil.connect("column metadata list is empty of table ", metadata.getSchema(), ".", metadata.getTable());
            l.error("{} {}", TAG, errMsg);
            throw new NullPointerException(errMsg);
        }

        if (metadata.getDelimiter().isEmpty()) {
            String errMsg = StrUtil.connect("no delimiter set for table ", metadata.getSchema(), ".", metadata.getTable());
            l.error("{} {}", TAG, errMsg);
            throw new NullPointerException(errMsg);
        }

        for (ColumnMeta meta : metadata.getColumnsList()) {
            if (meta.getName().isEmpty()) {
                String errMsg = StrUtil.connect("column name is empty of table ", metadata.getSchema(), ".", metadata.getTable());
                l.error("{} {}", TAG, errMsg);
                throw new NullPointerException(errMsg);
            }
            if (meta.getType().isEmpty()) {
                String errMsg = StrUtil.connect("column type is empty of table ", metadata.getSchema(), ".", metadata.getTable());
                l.error("{} {}", TAG, errMsg);
                throw new NullPointerException(errMsg);
            }
            if (meta.getNum() <= 0) { // Num is the column index, start from 1;
                String errMsg = StrUtil.connect("column num is <= 0 of table ", metadata.getSchema(), ".", metadata.getTable(), " : ", String.valueOf(meta.getNum()));
                l.error("{} {}", TAG, errMsg);
                throw new NullPointerException(errMsg);
            }
        }
    }

    private JobMetadataWrapper(JobMetadata metadata) throws NullPointerException {
        this.schema = metadata.getSchema();
        this.table = metadata.getTable();
        this.projection = metadata.getProjection();
        this.insertProjection = metadata.getInsertProjection();
        this.selectProjection = metadata.getSelectProjection();
        this.uniqueKeyClause = metadata.getUniqueKeyClause();
        this.delimiter = metadata.getDelimiter();
        // Wrap columnMeta.
        this.columnMetaWrapperList = new ArrayList<>();
        int size = metadata.getColumnsList().size();
        for (int i = 0; i < size; i++) {
            ColumnMeta meta = metadata.getColumnsList().get(i);
            if (meta == null) {
                throw new NullPointerException("invalid column metadata: column[" + i + "] is null.");
            }
            this.columnMetaWrapperList.add(ColumnMetaWrapper.wrap(meta));
        }
    }

    private String schema;

    private String table;

    private String projection;

    private String insertProjection;

    private String selectProjection;

    private String uniqueKeyClause;

    private String delimiter;

    private List<ColumnMetaWrapper> columnMetaWrapperList;

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getProjection() {
        return projection;
    }

    public void setProjection(String projection) {
        this.projection = projection;
    }

    public String getInsertProjection() {
        return insertProjection;
    }

    public void setInsertProjection(String insertProjection) {
        this.insertProjection = insertProjection;
    }

    public String getSelectProjection() {
        return selectProjection;
    }

    public void setSelectProjection(String selectProjection) {
        this.selectProjection = selectProjection;
    }

    public String getUniqueKeyClause() {
        return uniqueKeyClause;
    }

    public void setUniqueKeyClause(String uniqueKeyClause) {
        this.uniqueKeyClause = uniqueKeyClause;
    }

    public String getDelimiter() {
        return delimiter;
    }

    public void setDelimiter(String delimiter) {
        this.delimiter = delimiter;
    }

    public List<ColumnMetaWrapper> getColumnMetaList() {
        return this.columnMetaWrapperList;
    }

    public void setColumnMetaList(List<ColumnMetaWrapper> columnMetaWrapperList) {
        this.columnMetaWrapperList = columnMetaWrapperList;
    }
}
