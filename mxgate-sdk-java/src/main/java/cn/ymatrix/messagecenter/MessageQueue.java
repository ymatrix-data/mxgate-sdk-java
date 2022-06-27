package cn.ymatrix.messagecenter;


public interface MessageQueue<T>  {

    T get() throws InterruptedException;

    void add(T t);

    void clear();

    boolean isEmpty();

}
