package cn.ymatrix.data;

/**
 * An Element represents the value of a column.
 *
 * @param <T>
 */
public interface Column<T> {
    void setValue(T t);

    T getValue();

    /**
     * @return true = not null, false = null
     */
    boolean isValid();

    /**
     * Transform value format to CSV.
     *
     * @return the CSV String with quotation.
     */
    String toCSV();

    String toString();

    void setColumnName(String name);

    String getColumnName();

    void setSkip();

    boolean shouldSkip();
}