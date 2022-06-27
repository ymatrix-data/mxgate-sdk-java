package cn.ymatrix.apiclient;

import cn.ymatrix.data.Tuple;
import cn.ymatrix.exception.ClientClosedException;

import java.util.List;

/**
 * MXClient is a proxy to send data to yMatrix backend server.
 */
public interface MxClient {
    /**
     * Append a single tuple asynchronously.
     *
     * @param tuple to be appended.
     */
    void appendTuple(Tuple tuple);
    /**
     * Append a single tuple synchronously.
     *
     * @param tuple to be appended.
     * @return Indicate whether the accumulated size of tuples has reached limit and need to flush(call flushBlocking)
     */
    boolean appendTupleBlocking(Tuple tuple);

    /**
     * Append multiple tuples asynchronously.
     *
     * @param tuples to be appended.
     */
    void appendTuples(Tuple... tuples);
    /**
     * Append multiple tuples synchronously.
     *
     * @param tuples to be appended.
     * @return Indicate whether the accumulated size of tuples has reached limit and need to flush(call flushBlocking)
     */
    boolean appendTuplesBlocking(Tuple... tuples);

    /**
     * Append tuple list asynchronously.
     *
     * @param tuples to be appended.
     */
    void appendTuplesList(List<Tuple> tuples)throws NullPointerException, ClientClosedException, IllegalStateException;
    /**
     * Append tuple list synchronously.
     *
     * @param tuples to be appended.
     * @return Indicate whether the accumulated size of tuples has reached limit and need to flush(call flushBlocking)
     */
    boolean appendTuplesListBlocking(List<Tuple> tuples);

    Tuple generateEmptyTuple();

    Tuple generateEmptyTupleLite();

    /**
     * Flush tuples added to send to mxgate asynchronously.
     */
    void flush();
    /**
     * Flush tuples added to send to mxgate synchronously.
     */
    void flushBlocking();

    /**
     * Register data post listener callback.
     * This callback will be invoked whether the data has been sent successfully or not.
     *
     * @param listener to be registered.
     */
    void registerDataPostListener(DataPostListener listener);

    /**
     * MxClient will accumulate tuples until the total bytes reach or beyond the bytesLimitation.
     *
     * @param bytesLimitation how many bytes are enough to flush into the cache, the default bytes size are 4MB.
     */
    void withEnoughBytesToFlush(long bytesLimitation);

    /**
     * MxClient will try to flush the accumulated tuples when the timeLimitationMillis is out.
     *
     * @param intervalMillis the interval to flush tuples into cache, the default interval is 2s.
     */
    void withIntervalToFlushMillis(long intervalMillis);

    /**
     * MxClient will accumulate tuples until the total lines reach or beyond the lines Limitation.
     * @param lines
     */
    void withEnoughLinesToFlush(int lines);

    /**
     * The Tuple in Tuples will be split by batch to do the CSV construction concurrently.
     * @param size
     */
    void withCSVConstructionBatchSize(int size);

    void useTuplesPool(int size);

    void setDelimiter(String delimiter);

    String getDelimiter();

    void setSchema(String schema);

    String getSchema();

    void setTable(String table);

    String getTable();

    void withCompress();

    void withoutCompress();

    void withBase64Encode4Compress();

    void withoutBase64EncodeCompress();

    String getClientName();
}
