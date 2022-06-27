package cn.ymatrix.data;

abstract class ColumnImpl<T> implements Column<T> {
    private T t;

    private String columnName;

    private boolean skip; // skip this column (e.g. when column type is serial)

    ColumnImpl(T t) {
        this.t = t;
    }

    @Override
    public void setValue(T t) {
        this.t = t;
    }

    @Override
    public T getValue() {
        return this.t;
    }

    @Override
    public boolean isValid() {
        return t == null;
    }

    @Override
    public void setColumnName(String name) {
        this.columnName = name;
    }

    @Override
    public String getColumnName() {
        return this.columnName;
    }

    @Override
    public void setSkip() {
        this.skip =true;
    }

    @Override
    public boolean shouldSkip() {
        return this.skip;
    }
}
