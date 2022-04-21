package org.apache.flink.contrib.streaming.state.cache;

import org.apache.commons.math3.util.Pair;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

/** Javadoc for First-in First-out CacheManager. */
public class FIFOCacheManager<K, V> extends AbstractCacheManager<K, V> {

    private Map<K, V> storage;
    private Queue<K> queue;

    /**
     * . Constructor
     *
     * @param size intial size of queue
     */
    public FIFOCacheManager(int size) {
        super(size);
        storage = new HashMap<>(size);
        queue = new LinkedList<K>();
    }

    /**
     * @param key
     * @return
     */
    @Override
    public boolean has(K key) {
        logger.info(key.toString());
        boolean hit = false;
        if (this.storage.containsKey(key)) {
            this.hitCount++;
            hit = true;
        }
        this.totalCount++;
        return hit;
    }

    /**
     * . Gets the entry, assuming that it exists
     *
     * @param key
     * @return
     */
    @Override
    public V get(K key) {
        logger.info("--- fifo get ---");
        return storage.getOrDefault(key, null);
    }

    @Override
    public Pair<K, V> update(K key, V value) {
        Pair<K, V> evictedKV = null;
        if (this.storage.size() >= this.size && !this.has(key)) {
            evictedKV = this.evict();
        }
        logger.info("--- fifo update ---");
        if (!this.has(key)) {
            queue.add(key);
        }
        storage.put(key, value);
        return evictedKV;
    }

    /** . Evicts the cache using FIFO */
    @Override
    protected Pair<K, V> evict() {
        logger.info("--- fifo evict ---");
        K keyToRemove = queue.peek();
        Pair<K, V> evictedKV = new Pair<K, V>(keyToRemove, this.storage.get(keyToRemove));
        this.storage.remove(keyToRemove);
        return evictedKV;
    }

    @Override
    protected void remove(K key) {
        this.storage.remove(key);
    }

    /** . Clear the storage */
    @Override
    protected void clear() {
        storage.clear();
    }
}
