package org.apache.flink.contrib.streaming.state.cache;

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
        logger.info(key.toString());
        return this.storage.containsKey(key);
    }

    // assume has already check key exists with hash
    @Override
    public V get(K key) {
        logger.info("--- lifo get ---");
        return storage.getOrDefault(key, null);
    }

    @Override
    public void update(K key, V value) {
        if (this.storage.size() >= this.size && !this.has(key)) {
            this.evict();
        }
        logger.info("--- lifo update ---");
        storage.put(key, value);
        stack.add(key);
    }

    @Override
    protected void evict() {
        logger.info("--- lifo evict ---");
        this.storage.remove(stack.pop());
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
