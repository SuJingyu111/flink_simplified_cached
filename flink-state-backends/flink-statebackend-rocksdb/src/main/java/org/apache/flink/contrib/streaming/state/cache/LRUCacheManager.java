package org.apache.flink.contrib.streaming.state.cache;

import org.apache.commons.math3.util.Pair;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

/** Javadoc for LRUCacheManager. */
public class LRUCacheManager<V> extends AbstractCacheManager<V> {

    private LinkedHashMap<String, Pair<byte[], V>> storage;

    public LRUCacheManager(int size) {
        super(size);
        storage = new LinkedHashMap(size, 0.75f, true);
    }

    @Override
    public boolean has(byte[] key) {
        // printRatio();
        boolean hit = false;
        String keyString = Arrays.toString(key);
        if (this.storage.containsKey(keyString)) {
            this.hitCount++;
            hit = true;
        }
        this.totalCount++;
        return hit;
    }

    // assume has already check key exists with hash
    @Override
    public V get(byte[] key) {
        logger.info("--- lru get ---");
        String keyString = Arrays.toString(key);

        Pair<byte[], V> result = storage.getOrDefault(keyString, null);
        if (result == null) {
            return null;
        }
        return result.getSecond();
    }

    @Override
    public Pair update(byte[] key, V value) {
        String keyString = Arrays.toString(key);

        Pair evictedKV = null;
        if (this.storage.size() >= this.size && !this.has(key)) {
            evictedKV = this.evict();
        }
        logger.info("--- lru update ---");
        storage.put(keyString, new Pair(key, value));
        return evictedKV;
    }

    @Override
    protected Pair<byte[], V> evict() {
        logger.info("--- lru evict ---");
        Map.Entry firstEntry = storage.entrySet().iterator().next();
        String keyString = (String) firstEntry.getKey();
        Pair evictedKV = (Pair) firstEntry.getValue();
        this.storage.remove(keyString);
        return evictedKV;
    }

    @Override
    protected void remove(byte[] key) {
        logger.info("--- lru remove ---");
        String keyString = Arrays.toString(key);
        this.storage.remove(keyString);
    }

    @Override
    protected void clear() {
        storage.clear();
    }
}
