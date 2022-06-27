package cn.ymatrix.cache;


import cn.ymatrix.data.Tuples;

/**
 * Cache, maybe a queue to receive tuples from API Client
 * Which make the sending of tuples async.
 */
public interface Cache {

    boolean offer (Tuples tuples);

    Tuples get() throws InterruptedException;

    void clear();

    boolean isEmpty();

    void setRefuse(boolean refuse);

    boolean isRefused();

    int size();
}
