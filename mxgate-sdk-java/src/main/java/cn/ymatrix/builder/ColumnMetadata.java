package cn.ymatrix.builder;

import cn.ymatrix.api.ColumnMetaWrapper;

class ColumnMetadata {
    private final ColumnMetaWrapper columnMetaWrapper;
    // The index of this column in the database.
    private final int index;

    public ColumnMetadata(ColumnMetaWrapper metaWrapper, int index) {
        this.columnMetaWrapper = metaWrapper;
        this.index = index;
    }

    public ColumnMetaWrapper getMetadata() {
        return columnMetaWrapper;
    }

    public int getIndex() {
        return index;
    }
}
