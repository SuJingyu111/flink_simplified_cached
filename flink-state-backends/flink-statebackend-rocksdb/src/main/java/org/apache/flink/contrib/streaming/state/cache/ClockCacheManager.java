package org.apache.flink.contrib.streaming.state.cache;

import java.util.HashMap;
import java.util.Map;

/** Javadoc for ClockCacheManager.
 * Introduction to Clock Replacement: https://www.youtube.com/watch?v=b-dRK8B8dQk .
 * Policy in detail:
 * 1. Imagine that all cache slots are arranged around a clock.
 * 2. Initially, the 'use bit' of each cache slot is 0.
 * 3. We have a 'clock hand' that suggests the NEXT page for eviction.
 * 4. Each time a slot is accessed, its use bit will set to 1.
 * 5. When we need to evict a record, we look at the slot under the clock hand:
 * a) If its use bit = 1, clear it and move the hand, repeat step 5.
 * b) If its use bit = 0, evict it. */
public class ClockCacheManager<V> extends AbstractCacheManager<V> {

    private final HashMap<byte[], CacheSlot<V>> storage;
    private CacheSlot<V> clockHand;  // clock hand points to the NEXT page for eviction.

    public ClockCacheManager(int size) {
        super(size);
        storage = new HashMap<>();
        clockHand = initDoublyCircularLinkedListWithSize(size);
    }

    @Override
    public boolean has(byte[] key) {
        return this.storage.containsKey(key);
    }

    // assume has already check key exists with hash
    @Override
    public V get(byte[] key) {
        logger.info("--- clock cache get ---");
        CacheSlot<V> slot = storage.getOrDefault(key, null);
        if(slot == null) {
            return null;
        }
        slot.useBit = 1;    // set use bit to 1 when access the slot
        return slot.slotValue;
    }

    @Override
    public void update(byte[] key, V value) {
        logger.info("--- clock cache update ---");
        // if already contains the key, just update
        if(has(key)) {
            CacheSlot<V> slot = storage.get(key);
            slot.slotValue = value;
            slot.useBit = 1;
            return;
        }
        // call evict() to move clock hand to empty page
        // not necessary delete a record
        evict();
        // update clock hand info
        clockHand.slotKey = key;
        clockHand.slotValue = value;
        clockHand.useBit = 1;
        // put new record into map
        storage.put(key, clockHand);
    }

    @Override
    protected void evict() {
        logger.info("--- clock cache evict ---");
        while(clockHand.useBit == 1) {
            clockHand.useBit = 0;
            clockHand = clockHand.next;
        }
        // now the clockHand points to the page that should be evicted
        // delete from map only if current slot has another record
        if(clockHand.slotKey != null) {
            storage.remove(clockHand.slotKey);
        }
    }

    @Override
    protected void clear() {
        // 1. clear map
        storage.clear();
        // 2. set all use bits to 0
        clockHand.useBit = 0;
        CacheSlot<V> cur = clockHand.next;
        while(cur != clockHand) {
            cur.useBit = 0;
            cur = cur.next;
        }
    }

    private CacheSlot<V> initDoublyCircularLinkedListWithSize(int size) {
        CacheSlot<V> head = new CacheSlot<>();
        CacheSlot<V> tail = head;
        for(int i = 0; i < size-1; i++) {
            CacheSlot<V> curNode = new CacheSlot<>();
            tail.next = curNode;
            curNode.prev = tail;
            tail = curNode;
        }
        tail.next = head;
        head.prev = tail;

        return head;
    }

    // node of doubly circular linked list
    private static class CacheSlot<V> {
        public byte useBit;
        public byte[] slotKey;
        public V slotValue;
        public CacheSlot<V> next;
        public CacheSlot<V> prev;

        public CacheSlot() {
        }

        public CacheSlot(byte[] slotKey, V slotValue) {
            this.slotKey = slotKey;
            this.slotValue = slotValue;
            this.useBit = 0;
            this.next = null;
            this.prev = null;
        }
    }
}
