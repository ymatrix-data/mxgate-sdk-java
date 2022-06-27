package cn.ymatrix.data;

import java.util.List;

public interface Tuples {
    /**
     * Append a single tuple.
     */
    void append(Tuple tuple);

    /**
     * Append tuples.
     */
    void appendTuples(Tuple... tuples);

    /**
     * Append tuples list.
     */
    void appendTupleList(List<Tuple> tupleList);

    List<Tuple> getTuplesList();

    /**
     * Get total tuple size.
     *
     * @return tuples size.
     */
    int size();

    void setSchema(String schema);

    void setTable(String table);

    String getSchema();

    String getTable();

    void setTarget(TuplesTarget target);

    TuplesTarget getTarget();

    Tuple getTupleByIndex(int index);

    void setSenderID(String senderID);

    String getSenderID();

    boolean isEmpty();

    void reset();

    void setDelimiter(String delimiter);

    String getDelimiter();

    boolean needCompress();

    void setCompress(boolean compress);

    boolean needBase64Encoding4CompressedBytes();

    void setBase64Encoding4CompressedBytes(boolean base64Encoding);

    int getCSVBatchSize();

}
