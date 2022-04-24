package org.apache.flink.contrib.streaming.state.cache;

import org.apache.commons.math3.util.Pair;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

/** Javadoc for First-in First-out CacheManager. */
public class FIFOCacheManager<V> extends AbstractCacheManager<V> {

    private Map<String, Pair<byte[], V>> storage;
    private Queue<String> queue;

    /**
     * . Constructor
     *
     * @param size intial size of queue
     */
    public FIFOCacheManager(int size) {
        super(size);
        storage = new HashMap<>(size);
        queue = new LinkedList<String>();
    }

    /**
     * @param key
     * @return
     */
    @Override
    public boolean has(byte[] key) {
        // logger.info(key.toString());
        String keyString = Arrays.toString(key);
        boolean hit = false;
        if (this.storage.containsKey(keyString)) {
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
    public V get(byte[] key) {
        logger.info("--- fifo get ---");
        String keyString = Arrays.toString(key);
        return storage.getOrDefault(keyString, null).getSecond();
    }

    @Override
    public Pair<byte[], V> update(byte[] key, V value) {
        String keyString = Arrays.toString(key);
        Pair<byte[], V> evictedKV = null;
        if (this.storage.size() >= this.size && !this.has(key)) {
            evictedKV = this.evict();
        }
        logger.info("--- fifo update ---");
        if (!this.has(key)) {
            queue.add(keyString);
        }
        storage.put(keyString, new Pair(key, value));
        return evictedKV;
    }

    /** . Evicts the cache using FIFO */
    @Override
    protected Pair<byte[], V> evict() {
        logger.info("--- fifo evict ---");
        String keyToRemove = queue.peek();
        Pair<byte[], V> evictedKV = this.storage.get(keyToRemove);
        this.storage.remove(keyToRemove);
        return evictedKV;
    }

    @Override
    protected void remove(byte[] key) {
        String keyString = Arrays.toString(key);
        this.storage.remove(keyString);
    }

    /** . Clear the storage */
    @Override
    protected void clear() {
        storage.clear();
    }
}
