package org.apache.flink.contrib.streaming.state.cache;

/**
 * Javadoc for RocksDBStateCache.
 * Interface for cache manager.
 */
public abstract class AbstractCacheManager <V> {

    protected int size; //size of cache, can be in terms of bytes or # of kv pairs

    public AbstractCacheManager(int size) {
        this.size = size;
    }

    // determine if such key exist in the cache
    public abstract boolean has(byte[] key);

    //gets value related to key from cache storage, need to interact with backend instance
    public abstract V get(byte[] key);

    // puts kv pair into cache, does not need to talk to backend
    public abstract void update(byte[] key, V value);

    //private function to evict kv pair when exceed preset size. Need to be overridden. Implement eviction policy here.
    protected abstract void evict();

    //Clears the cache
    protected abstract void clear();
}
