package org.apache.flink.contrib.streaming.state.cache;

/** Javadoc for RocksDBStateCache. */
public class RocksDBStateCache<K, V> {

    private AbstractCacheManager<K, V> cacheManager;

    private final int defaultSize = 10;

    public RocksDBStateCache() {
        this.cacheManager = CacheManagerFactory.getDefaultCacheManager(defaultSize);
    }

    public RocksDBStateCache(int size) {
        this.cacheManager = CacheManagerFactory.getDefaultCacheManager(size);
    }

    // gets value related to key
    public boolean has(K key) {
        return cacheManager.has(key);
    }

    // gets value related to key
    public V get(K key) {
        return cacheManager.get(key);
    }

    // evict value with such key
    public void remove(K key) {
        cacheManager.remove(key);
    }

    // puts kv pair, returns evicted pair
    public void update(K key, V value) {
        cacheManager.update(key, value);
    }

    public void clear() {}
}
