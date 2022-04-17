package org.apache.flink.contrib.streaming.state.cache;

import org.apache.commons.math3.util.Pair;

import java.util.HashMap;
import java.util.Map;
import java.util.Stack;

/** Javadoc for LRUCacheManager. */
public class LIFOCacheManager<K, V> extends AbstractCacheManager<K, V> {

    private Map<K, V> storage;
    private Stack<K> stack;

    public LIFOCacheManager(int size) {
        super(size);
        storage = new HashMap<>();
        stack = new Stack<K>();
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
        // logger.info("--- lifo get ---");
        return storage.getOrDefault(key, null);
    }

    @Override
    public Pair<K, V> update(K key, V value) {
        if (this.storage.size() >= this.size && !this.has(key)) {
            return this.evict();
        }
        // logger.info("--- lifo update ---");
        if (!this.has(key)) {
            stack.add(key);
        }
        storage.put(key, value);
        return null;
    }

    @Override
    protected Pair<K, V> evict() {
        // logger.info("--- lifo evict ---");
        K keyToRemove = stack.pop();
        Pair<K, V> evictedKV = new Pair<K, V>(keyToRemove, this.storage.get(keyToRemove));
        this.storage.remove(keyToRemove);
        return evictedKV;
    }

    @Override
    protected void remove(K key) {
        // logger.info("--- lru remove ---");
        this.storage.remove(key);
    }

    @Override
    protected void clear() {
        storage.clear();
    }
}
