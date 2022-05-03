package org.apache.flink.contrib.streaming.state.cache;

import org.apache.commons.math3.util.Pair;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Stack;

/** Javadoc for LRUCacheManager. */
public class LIFOCacheManager<V> extends AbstractCacheManager<V> {

    private Map<String, Pair<byte[], V>> storage;
    private Stack<String> stack;

    public LIFOCacheManager(int size) {
        super(size);
        storage = new HashMap<>();
        stack = new Stack<>();
    }

    @Override
    public boolean has(byte[] key) {
        String keyString = Arrays.toString(key);
        boolean hit = false;
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
        String keyString = Arrays.toString(key);
        return storage.get(keyString).getSecond();
    }

    @Override
    public Pair<byte[], V> update(byte[] key, V value) {
        String keyString = Arrays.toString(key);
        Pair<byte[], V> evictedKV = null;
        if (this.storage.size() >= this.size && !this.has(key)) {
            evictedKV = this.evict();
        }
        if (!this.has(key)) {
            stack.add(keyString);
        }
        storage.put(keyString, new Pair(key, value));
        return evictedKV;
    }

    @Override
    protected Pair<byte[], V> evict() {
        String keyToRemove = stack.pop();
        Pair<byte[], V> evictedKV = this.storage.get(keyToRemove);
        this.storage.remove(keyToRemove);
        return evictedKV;
    }

    @Override
    protected void remove(byte[] key) {
        String keyString = Arrays.toString(key);
        this.storage.remove(keyString);
    }

    @Override
    protected void clear() {
        storage.clear();
    }
}
