package cn.ymatrix.cache;

public class CacheFactory {

    /**
     * Get Cache singleton.
     *
     * @return Cache instance
     */
    public static Cache getCacheInstance(int capacity, long waitTimeoutMS) {
        return QueueCache.getInstance(capacity, waitTimeoutMS);
    }

    public static Cache getCacheInstance() {
        final int DEFAULT_CACHE_CAPACITY = 1000;
        final int DEFAULT_CACHE_TIMEOUT = 1000;
        return getCacheInstance(DEFAULT_CACHE_CAPACITY, DEFAULT_CACHE_TIMEOUT);
    }

}
