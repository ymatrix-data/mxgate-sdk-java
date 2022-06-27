package cn.ymatrix.builder;

import cn.ymatrix.data.Column;
import cn.ymatrix.data.ColumnsFactory;
import cn.ymatrix.data.Tuple;
import cn.ymatrix.logger.MxLogger;
import cn.ymatrix.utils.StrUtil;
import org.slf4j.Logger;

import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.List;

class TupleImplLite extends BaseTuple {
    private static final String TAG = StrUtil.logTagWrap(TupleImplLite.class.getName());
    private final List<Column> columns;

    TupleImplLite(String delimiter, String schema, String table) throws InvalidParameterException {
        super(delimiter, schema, table);
        columns = new ArrayList<>();
    }

    @Override
    public void addColumn(String key, Object value) throws InvalidParameterException {
        if (key == null) {
            throw new InvalidParameterException("column name is null.");
        }
        if (value == null) {
            throw new InvalidParameterException("column value is null.");
        }
        this.columns.add(ColumnsFactory.columnRaw(value));
    }

    @Override
    public Column[] getColumns() {
        return this.columns.toArray(new Column[this.columns.size()]);
    }

    @Override
    public int size() {
        return this.columns.size();
    }

    @Override
    public void readinessCheck() {
        // For lite TupleImpl, no need to check the Tuple's readiness.
    }

    @Override
    public String getRawInputString() throws NullPointerException {
        if (this.columns == null) {
            return "";
        }
        List<String> strings = new ArrayList<>();
        int length = this.columns.size();
        for (int i = 0; i < length; i++) {
            Column column = columns.get(i);
            if (column == null) {
                strings.add("");
            }
            strings.add(column.toString());
            if (i <= length - 2) {
                strings.add(" ");
            }
        }
        return StrUtil.connect(strings);
    }

    @Override
    public String toString() {
        return this.toCSVLineStr();
    }

    @Override
    public String toCSVLineStr() {
        if (this.columns == null) {
            return ""; // Return an empty string.
        }
        List<String> strings = new ArrayList<>();
        for (int i = 0; i < this.columns.size(); i++) {
            if (columns.get(i) == null) {
                strings.add("");
            } else if (StrUtil.isCSVStrEmpty(columns.get(i).toCSV())) {
                strings.add("");
            } else {
                strings.add(columns.get(i).toCSV());
            }

            // Use element to separate each element.
            if (i <= this.columns.size() - 2) {
                strings.add(this.delimiter);
            }
        }
        return StrUtil.connect(strings);
    }

    @Override
    public synchronized void reset() {
        this.columns.clear();
    }
}
