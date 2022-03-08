package org.apache.flink.contrib.streaming.state.cache;

import java.util.LinkedHashMap;
import java.util.Map;

/** Javadoc for LRUCacheManager. */
public class LRUCacheManager<V> extends AbstractCacheManager<V> {

    private LinkedHashMap<byte[], V> storage;

    public LRUCacheManager(int size) {
        super(size);
        storage = new LinkedHashMap<byte[], V>(size, 0.75f, true);
    }

    @Override
    public boolean has(byte[] key) {
        return this.storage.containsKey(key);
    }

    // assume has already check key exists with hash
    @Override
    public V get(byte[] key) {
        logger.info("--- lru get ---");
        return storage.getOrDefault(key, null);
    }

    @Override
    public void update(byte[] key, V value) {
        if (this.storage.size() >= this.size && !this.has(key)) {
            this.evict();
        }
        logger.info("--- lru update ---");
        storage.put(key, value);
    }

    @Override
    protected void evict() {
        logger.info("--- lru evict ---");
        Map.Entry<byte[], V> firstEntry = storage.entrySet().iterator().next();
        this.storage.remove(firstEntry.getKey());
    }

    @Override
    protected void clear() {
        storage.clear();
    }
}
