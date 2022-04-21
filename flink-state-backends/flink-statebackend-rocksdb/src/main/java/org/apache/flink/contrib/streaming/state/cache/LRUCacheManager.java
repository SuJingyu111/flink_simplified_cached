package org.apache.flink.contrib.streaming.state.cache;

import org.apache.commons.math3.util.Pair;

import java.util.LinkedHashMap;
import java.util.Map;

/** Javadoc for LRUCacheManager. */
public class LRUCacheManager<K, V> extends AbstractCacheManager<K, V> {

    private LinkedHashMap<K, V> storage;

    public LRUCacheManager(int size) {
        super(size);
        storage = new LinkedHashMap<K, V>(size, 0.75f, true);
    }

    @Override
    public boolean has(K key) {
        // printRatio();
        boolean hit = false;
        if (this.storage.containsKey(key)) {
            this.hitCount++;
            hit = true;
        }
        this.totalCount++;
        return hit;
    }

    // assume has already check key exists with hash
    @Override
    public V get(K key) {
        logger.info("--- lru get ---");
        return storage.getOrDefault(key, null);
    }

    @Override
    public Pair<K, V> update(K key, V value) {
        Pair<K, V> evictedKV = null;
        if (this.storage.size() >= this.size && !this.has(key)) {
            evictedKV = this.evict();
        }
        logger.info("--- lru update ---");
        storage.put(key, value);
        return evictedKV;
    }

    @Override
    protected Pair<K, V> evict() {
        logger.info("--- lru evict ---");
        Map.Entry<K, V> firstEntry = storage.entrySet().iterator().next();
        K keyToRemove = firstEntry.getKey();
        Pair<K, V> evictedKV = new Pair<K, V>(keyToRemove, this.storage.get(keyToRemove));
        this.storage.remove(keyToRemove);
        return evictedKV;
    }

    @Override
    protected void remove(K key) {
        logger.info("--- lru remove ---");
        this.storage.remove(key);
    }

    @Override
    protected void clear() {
        storage.clear();
    }
}
