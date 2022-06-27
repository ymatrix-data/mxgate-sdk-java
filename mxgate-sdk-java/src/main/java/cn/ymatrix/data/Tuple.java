package cn.ymatrix.data;


/**
 * The tuple represents a tuple in a database table.
 */
public interface Tuple {
    /**
     * Append column key -> column value
     *
     * @param key   column name
     * @param value column value
     */
    void addColumn(String key, Object value);

    /**
     * Get the table name that this tuple belongs to.
     *
     * @return public.table
     */
    String getTableName();

    /**
     * Return the columns that this tuple contains.
     */
    Column[] getColumns();

    /**
     * Get tuple string bytes.
     *
     * @return tuples string bytes.
     */
    byte[] toBytes();

    /**
     * The delimiter to separate columns in CSV file.
     *
     * @param delimiter CSV file delimiter
     */
    void setDelimiter(String delimiter);

    String getDelimiter();

    /**
     * The total columns size.
     *
     * @return columns size
     */
    int size();

    /**
     * Indicate that this tuple is ready to be sent.
     */
    void readinessCheck();

    String toCSVLineStr();

    String getRawInputString();

    void setSerialNum(long sn);

    long getSerialNum();

    void reset();
}
