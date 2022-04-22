package org.apache.flink.contrib.streaming.state.cache;

import org.apache.commons.math3.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Javadoc for RocksDBStateCache. Interface for cache manager. */
public abstract class AbstractCacheManager<V> {

    protected int size; // size of cache, can be in terms of bytes or # of kv pairs

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    public int hitCount, totalCount;

    public AbstractCacheManager(int size) {
        this.size = size;
        this.hitCount = 0;
        this.totalCount = 0;
    }

    // determine if such key exist in the cache
    public abstract boolean has(byte[] key);

    // gets value related to key from cache storage, need to interact with backend instance
    public abstract V get(byte[] key);

    // puts kv pair into cache, does not need to talk to backend
    public abstract Pair<byte[], V> update(byte[] key, V value);

    // private function to evict kv pair when exceed preset size. Need to be overridden. Implement
    // eviction policy here.
    protected abstract Pair<byte[], V> evict();

    protected abstract void remove(byte[] key);

    // Clears the cache
    protected abstract void clear();

    public void printRatio() {
        logger.info("Current Hit Rate: " + getHitRate());
    }

    public float getHitRate() {
        if (this.totalCount == 0) {
            return 0;
        }
        return ((float) this.hitCount) / ((float) this.totalCount);
    }
}
