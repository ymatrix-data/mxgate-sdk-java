package cn.ymatrix.builder;

import cn.ymatrix.data.ColumnsFactory;
import cn.ymatrix.data.Column;
import cn.ymatrix.data.Tuple;
import cn.ymatrix.exception.TupleNotReadyException;
import cn.ymatrix.logger.MxLogger;
import cn.ymatrix.utils.StrUtil;
import org.slf4j.Logger;

import java.security.InvalidParameterException;
import java.util.*;

/**
 * The implementation of Tuple which represent a tuple of a table in the database.
 */
class TupleImpl extends BaseTuple {
    private static final String TAG = StrUtil.logTagWrap(TupleImpl.class.getName());

    // This column metadata map is a reference from MxClient (column_name -> column_metadata).
    private final Map<String, ColumnMetadata> columnMetaMap;

    // This set contains all the column names that already have not been set.
    private final Set<Column> emptyColumnsSet;

    TupleImpl(Map<String, ColumnMetadata> columnMetaMap, String delimiter, String schema, String table)
            throws NullPointerException, IndexOutOfBoundsException, InvalidParameterException {
        super(delimiter, schema, table);
        if (columnMetaMap == null || columnMetaMap.size() == 0) {
            l.error("{} Init tuple on a null or an empty parameter columnMetaMap.", TAG);
            throw new NullPointerException("init tuple on an empty parameter columnMetaMap.");
        }
        this.columnMetaMap = columnMetaMap;
        this.columns = new Column[columnMetaMap.size()];
        this.emptyColumnsSet = new HashSet<>();
        initEmptyColumns();
    }

    // During the initialization of this tuple, all the columns are empty.
    private void initEmptyColumns() throws NullPointerException, IndexOutOfBoundsException {
        for (Map.Entry<String, ColumnMetadata> entry : this.columnMetaMap.entrySet()) {
            if (entry == null || entry.getKey() == null || entry.getValue() == null || entry.getValue().getMetadata() == null) {
                throw new NullPointerException("Map.Entry<String, ColumnMetadata> entry is null.");
            }
            ColumnMetadata meta = entry.getValue();
            if (meta.getIndex() >= this.columns.length) {
                String indexOutOfBound = StrUtil.connect("Index ", String.valueOf(meta.getIndex()), " is out of column bounds ", String.valueOf(this.columns.length));
                throw new IndexOutOfBoundsException(indexOutOfBound);
            }
            // Get an empty column.
            Column columnRaw = ColumnsFactory.columnRaw(null);
            // Set column name.
            columnRaw.setColumnName(meta.getMetadata().getName());
            if (meta.getMetadata().isSerial() || meta.getMetadata().isExcluded()) {
                columnRaw.setSkip();
            }
            // Add empty column into column list.
            this.columns[meta.getIndex()] = columnRaw;
            // Add the empty column into empty set.
            this.emptyColumnsSet.add(columnRaw);
        }
    }

    /**
     * @param column column name
     * @param value  column value
     * @throws InvalidParameterException
     * @throws NullPointerException
     * @throws IndexOutOfBoundsException
     */
    @Override
    public void addColumn(String column, Object value)
            throws InvalidParameterException, NullPointerException, IndexOutOfBoundsException {
        if (column == null) {
            throw new NullPointerException("column name is null.");
        }

        if (value == null) {
            throw new NullPointerException("column value is null.");
        }

        ColumnMetadata metadata = this.columnMetaMap.get(column);

        if (metadata == null) {
            String msg = StrUtil.connect("column ", column, " could not be found in table ", this.schema, ".", this.table);
            throw new NullPointerException(msg);
        }

        int index = metadata.getIndex();
        if (index >= this.columns.length) {
            String msg = StrUtil.connect("index ", String.valueOf(index), " is out of column bounds ", String.valueOf(this.columns.length));
            throw new IndexOutOfBoundsException(msg);
        }

        // There should be an empty raw column in each index, not null.
        if (this.columns[index] == null) {
            String msg = StrUtil.connect("column of index[", String.valueOf(index), "] which name = ", column, " is null.");
            throw new NullPointerException(msg);
        }

        if (!this.columns[index].getColumnName().equals(column)) {
            String msg = StrUtil.connect("column name = ", column, " is not matching with the name in columns list: ", this.columns[index].getColumnName());
            throw new InvalidParameterException(msg);
        }

        // Set the value.
        this.columns[index].setValue(value);
        // Remove the column from the empty set when it has been set.
        removeEmptyColumn(this.columns[index]);
    }

    @Override
    public Column[] getColumns() {
        return this.columns;
    }

    private void removeEmptyColumn(Column column) {
        this.emptyColumnsSet.remove(column);
    }

    @Override
    public int size() {
        return this.columns.length;
    }

    /**
     * To check whether this tuple is ready to be inserted.
     */
    @Override
    public void readinessCheck() throws TupleNotReadyException {
        if (this.columnMetaMap == null || this.columnMetaMap.size() == 0) {
            throw new TupleNotReadyException("columnMetaMap is empty");
        }
        if (this.columns == null) {
            throw new TupleNotReadyException("column list is null");
        }
        // We only need to check the empty columns not all the columns.
        for (Column column : this.emptyColumnsSet) {
            try {
                if (column == null) {
                    throw new NullPointerException();
                }
                if (column.shouldSkip()) {
                    continue;
                }
                getEmptyColumnValue(column.getColumnName());
            } catch (NullPointerException e) {
                l.error(e.getMessage());
                throw new TupleNotReadyException(e.getMessage(), e);
            }
        }
        // If there is no exception to throw, this tuple is ready to be inserted.
    }

    @Override
    public String toCSVLineStr() throws NullPointerException {
        if (this.columns == null) {
            return ""; // Return an empty string.
        }

        int columnLength = this.columns.length;

        List<String> strings = new ArrayList<>();
        for (int i = 0; i < columnLength; i++) {
            if (columns[i] == null) {
                throw new NullPointerException("the column[" + i + "] is null.");
            }
            if (columns[i].shouldSkip()) {
                continue;
            }
            // This is an empty column, try to find default value all value as null.
            if (StrUtil.isNullOrEmpty(columns[i].toString())) {
                try {
                    String defValue = getEmptyColumnValue(columns[i].getColumnName());
                    if (!defValue.isEmpty()) {
                        columns[i].setValue(defValue);
                    }
                } catch (NullPointerException e) {
                    l.error("{} get empty value for column[{}] exception: {}", TAG, i, e.getMessage());
                    throw new NullPointerException(e.getMessage());
                }
            }

            // After the default value set, columns[i] is still empty, we will add nothing as null to this column.
            if (StrUtil.isNullOrEmpty(columns[i].toString())) {
                strings.add("");
            } else {
                strings.add(columns[i].toString());
            }

            // Use element to separate each element.
            if (i <= columnLength - 2) {
                strings.add(this.delimiter);
            }
        }
        return StrUtil.connect(strings);
    }

    @Override
    public String getRawInputString() {
        if (this.columns == null) {
            return ""; // Return an empty string.
        }

        List<String> strings = new ArrayList<>();
        for (int i = 0; i < this.columns.length; i++) {
            if (columns[i] == null) {
                strings.add("");
            } else {
                strings.add(columns[i].toString());
            }

            // Use element to separate each element.
            if (i <= this.columns.length - 2) {
                strings.add(" ");
            }
        }
        return StrUtil.connect(strings);
    }

    @Override
    public synchronized void reset() {
        for (Column column : this.columns) {
            column.setValue(null);
            this.emptyColumnsSet.add(column);
        }
    }

    private String getEmptyColumnValue(String column) throws NullPointerException {
        if (StrUtil.isNullOrEmpty(column)) {
            throw new NullPointerException("column name is null.");
        }

        ColumnMetadata columnMetadata = this.columnMetaMap.get(column);
        if (columnMetadata == null) {
            throw new NullPointerException("column metadata could not be found for column: " + column);
        }

        if (columnMetadata.getMetadata() == null) {
            throw new NullPointerException("column metadata is null.");
        }

        // 1. check whether this column has default value.
        boolean hasNoDefVal = (columnMetadata.getMetadata().getDefaultValue() == null || columnMetadata.getMetadata().getDefaultValue().isEmpty());
        // 2. check whether this column could be null.
        boolean notNull = columnMetadata.getMetadata().isNotNull();
        // If this column has no default value and could not be null, then, it could not be null.
        l.debug("{} column {} hasNoDefValue = {}, notNull = {}", TAG, columnMetadata.getMetadata().getName(), hasNoDefVal, notNull);

        // Has no default value and also could not be null, throw the exception.
        if (hasNoDefVal && notNull) {
            throw new NullPointerException("the column "
                    + columnMetadata.getMetadata().getName()
                    + " has no default value and could not be null, So its value must be set.");
        }

        // Has default value.
        if (!hasNoDefVal) {
            return columnMetadata.getMetadata().getDefaultValue();
        }

        // Could be null, return an empty string.
        return "";
    }


}
