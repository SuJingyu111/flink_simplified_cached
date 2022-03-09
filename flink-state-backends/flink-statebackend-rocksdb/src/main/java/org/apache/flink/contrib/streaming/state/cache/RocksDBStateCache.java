package org.apache.flink.contrib.streaming.state.cache;

/** Javadoc for RocksDBStateCache. */
public class RocksDBStateCache<V> {

    private AbstractCacheManager<V> cacheManager;

    private final int defaultSize = 10;

    public RocksDBStateCache() {
        this.cacheManager = CacheManagerFactory.getDefaultCacheManager(defaultSize);
    }

    public RocksDBStateCache(int size) {
        this.cacheManager = CacheManagerFactory.getDefaultCacheManager(size);
    }

    // gets value related to key
    public boolean has(byte[] key) {
        return cacheManager.has(key);
    }

    // gets value related to key
    public V get(byte[] key) {
        return cacheManager.get(key);
    }

    // puts kv pair, returns evicted pair
    public void update(byte[] key, V value) {
        cacheManager.update(key, value);
    }

    public void clear() {}
}
