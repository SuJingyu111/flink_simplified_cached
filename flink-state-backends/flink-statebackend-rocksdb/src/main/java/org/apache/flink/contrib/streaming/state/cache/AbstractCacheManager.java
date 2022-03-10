package org.apache.flink.contrib.streaming.state.cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Javadoc for RocksDBStateCache. Interface for cache manager. */
public abstract class AbstractCacheManager<K, V> {

    protected int size; // size of cache, can be in terms of bytes or # of kv pairs

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    public AbstractCacheManager(int size) {
        this.size = size;
    }

    // determine if such key exist in the cache
    public abstract boolean has(K key);

    // gets value related to key from cache storage, need to interact with backend instance
    public abstract V get(K key);

    // puts kv pair into cache, does not need to talk to backend
    public abstract void update(K key, V value);

    // private function to evict kv pair when exceed preset size. Need to be overridden. Implement
    // eviction policy here.
    protected abstract void evict();

    // Clears the cache
    protected abstract void clear();
}
