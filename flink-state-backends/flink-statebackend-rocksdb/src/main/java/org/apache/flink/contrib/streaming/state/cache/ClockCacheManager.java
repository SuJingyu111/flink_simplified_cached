package org.apache.flink.contrib.streaming.state.cache;

import org.apache.commons.math3.util.Pair;

import java.util.Arrays;
import java.util.HashMap;

/**
 * Javadoc for ClockCacheManager. Introduction to Clock Replacement:
 * https://www.youtube.com/watch?v=b-dRK8B8dQk . Policy in detail: 1. Imagine that all cache slots
 * are arranged around a clock. 2. Initially, the 'use bit' of each cache slot is 0. 3. We have a
 * 'clock hand' that suggests the NEXT page for eviction. 4. Each time a slot is accessed, its use
 * bit will set to 1. 5. When we need to evict a record, we look at the slot under the clock hand:
 * a) If its use bit = 1, clear it and move the hand, repeat step 5. b) If its use bit = 0, evict
 * it.
 */
public class ClockCacheManager<V> extends AbstractCacheManager<V> {

    private final HashMap<String, CacheSlot<String, Pair<byte[], V>>> storage;
    private CacheSlot<String, Pair<byte[], V>>
            clockHand; // clock hand points to the NEXT page for eviction.

    public ClockCacheManager(int size) {
        super(size);
        storage = new HashMap<>();
        clockHand = initDoublyCircularLinkedListWithSize(size);
    }

    @Override
    public boolean has(byte[] key) {
        printRatio();
        String innerKey = Arrays.toString(key);
        boolean hit = this.storage.containsKey(innerKey);
        if (hit) {
            this.hitCount += 1;
        }
        this.totalCount += 1;
        return hit;
    }

    // assume has already check key exists with hash
    @Override
    public V get(byte[] key) {
        String innerKey = Arrays.toString(key);
        CacheSlot<String, Pair<byte[], V>> slot = storage.getOrDefault(innerKey, null);
        if (slot == null) {
            return null;
        }
        slot.useBit = 1; // set use bit to 1 when access the slot
        return slot.slotValue.getSecond();
    }

    @Override
    public Pair<byte[], V> update(byte[] key, V value) {
        // if already contains the key, just update
        String innerKey = Arrays.toString(key);
        if (has(key)) {
            CacheSlot<String, Pair<byte[], V>> slot = storage.get(innerKey);
            slot.slotValue = new Pair<>(key, value);
            slot.useBit = 1;
            return null;
        }
        // move clock hand to empty page
        while (clockHand.useBit == 1) {
            clockHand.useBit = 0;
            clockHand = clockHand.next;
        }
        Pair<byte[], V> evictedKV = null;
        if (storage.size() == size) {
            evictedKV = evict();
        }
        // update clock hand info
        clockHand.slotKey = innerKey;
        clockHand.slotValue = new Pair<>(key, value);
        clockHand.useBit = 1;
        // put new record into map
        storage.put(innerKey, clockHand);
        return evictedKV;
    }

    @Override
    protected Pair<byte[], V> evict() {
        // logger.info("--- clock cache evict ---");

        // now the clockHand points to the page that should be evicted
        // delete from map only if current slot has another record
        if (clockHand.slotKey != null) {
            Pair<byte[], V> evictedKV = clockHand.slotValue;
            storage.remove(clockHand.slotKey);
            return evictedKV;
        }
        return null;
    }

    @Override
    protected void remove(byte[] key) {
        String innerKey = Arrays.toString(key);
        if (storage.containsKey(innerKey)) {
            CacheSlot<String, Pair<byte[], V>> slot = storage.get(innerKey);
            slot.useBit = 0;
            slot.slotKey = null;
            storage.remove(innerKey);
        }
    }

    @Override
    protected void clear() {
        // 1. clear map
        storage.clear();
        // 2. set all use bits to 0
        clockHand.useBit = 0;
        clockHand.slotKey = null;
        CacheSlot<String, Pair<byte[], V>> cur = clockHand.next;
        while (cur != clockHand) {
            cur.useBit = 0;
            cur.slotKey = null;
            cur = cur.next;
        }
    }

    private CacheSlot<String, Pair<byte[], V>> initDoublyCircularLinkedListWithSize(int size) {
        CacheSlot<String, Pair<byte[], V>> head = new CacheSlot<>();
        CacheSlot<String, Pair<byte[], V>> tail = head;
        for (int i = 0; i < size - 1; i++) {
            CacheSlot<String, Pair<byte[], V>> curNode = new CacheSlot<>();
            tail.next = curNode;
            curNode.prev = tail;
            tail = curNode;
        }
        tail.next = head;
        head.prev = tail;

        return head;
    }

    // node of doubly circular linked list
    private static class CacheSlot<K, V> {
        public byte useBit;
        public K slotKey;
        public V slotValue;
        public CacheSlot<K, V> next;
        public CacheSlot<K, V> prev;

        public CacheSlot() {}

        public CacheSlot(K slotKey, V slotValue) {
            this.slotKey = slotKey;
            this.slotValue = slotValue;
            this.useBit = 0;
            this.next = null;
            this.prev = null;
        }
    }
}
