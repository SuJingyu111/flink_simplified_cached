package org.apache.flink.contrib.streaming.state.cache;

import org.apache.commons.math3.util.Pair;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;

/** Javadoc for LFUCacheManager. */
public class LFUCacheManager<V> extends AbstractCacheManager<V> {

    /** Javadoc for a data entry. */
    protected class Entry {
        String key;
        V val;
        long freq;

        byte[] byteKey;

        Entry(String key, V val, long freq, byte[] byteKey) {
            this.key = key;
            this.val = val;
            this.freq = freq;
            this.byteKey = byteKey;
        }
    }

    // todo: clear freq counter after a time interval
    private long minFrequency;
    private HashMap<String, Entry> keyEntryMap;
    private HashMap<Long, LinkedList<Entry>> freqEntryListMap;

    public LFUCacheManager(int size) {
        super(size);
        this.minFrequency = 0L;
        this.keyEntryMap = new HashMap<>();
        this.freqEntryListMap = new HashMap<>();
    }

    @Override
    public boolean has(byte[] key) {
        printRatio();
        this.totalCount++;
        String keyString = Arrays.toString(key);
        if (keyEntryMap.containsKey(keyString)) {
            this.hitCount++;
            return true;
        }
        return false;
    }

    @Override
    public V get(byte[] key) {
        String keyString = Arrays.toString(key);
        if (size == 0 || !keyEntryMap.containsKey(keyString)) {
            return null;
        }
        Entry entry = keyEntryMap.get(keyString);
        V val = entry.val;
        long freq = entry.freq;
        freqEntryListMap.get(freq).remove(entry);
        if (freqEntryListMap.get(freq).size() == 0) {
            freqEntryListMap.remove(freq);
            if (minFrequency == freq) {
                minFrequency += 1;
            }
        }
        entry.freq += 1;
        LinkedList<Entry> list = freqEntryListMap.getOrDefault(freq + 1, new LinkedList<>());
        list.offerFirst(entry);
        freqEntryListMap.put(freq + 1, list);
        keyEntryMap.put(keyString, freqEntryListMap.get(freq + 1).peekFirst());
        return val;
    }

    @Override
    public Pair<byte[], V> update(byte[] key, V value) {
        String keyString = Arrays.toString(key);
        if (size == 0) {
            return null;
        }
        Pair<byte[], V> evictedKV = null;
        if (!keyEntryMap.containsKey(keyString)) {
            if (keyEntryMap.size() == size) {
                evictedKV = evict();
            }
            LinkedList<Entry> list = freqEntryListMap.getOrDefault(1L, new LinkedList<Entry>());
            Entry newEntry = new Entry(keyString, value, 1, key);
            list.offerFirst(newEntry);
            freqEntryListMap.put(1L, list);
            keyEntryMap.put(keyString, newEntry);
            minFrequency = 1;
        } else {
            Entry entry = keyEntryMap.get(keyString);
            long freq = entry.freq;
            LinkedList<Entry> list = getUpdatedFreqEntryList(entry);
            entry.freq += 1;
            list.offerFirst(entry);
            freqEntryListMap.put(freq + 1, list);
            keyEntryMap.put(keyString, entry);
        }
        return evictedKV;
    }

    @Override
    protected Pair<byte[], V> evict() {
        Entry entry = freqEntryListMap.get(minFrequency).peekLast();
        assert entry != null;
        String keyToRemove = entry.key;
        Entry removeEntry = keyEntryMap.get(keyToRemove);
        keyEntryMap.remove(keyToRemove);
        freqEntryListMap.get(minFrequency).pollLast();
        if (freqEntryListMap.get(minFrequency).size() == 0) {
            freqEntryListMap.remove(minFrequency);
        }
        return new Pair<>(removeEntry.byteKey, removeEntry.val);
    }

    @Override
    protected void remove(byte[] key) {
        String keyString = Arrays.toString(key);
        if (keyEntryMap.containsKey(keyString)) {
            Entry removedEntry = keyEntryMap.remove(keyString);
            long removedFreq = removedEntry.freq;
            LinkedList<Entry> entryList = freqEntryListMap.get(removedFreq);
            entryList.remove(removedEntry);
            if (entryList.size() == 0) {
                freqEntryListMap.remove(removedEntry.freq);
                if (removedFreq == minFrequency) {
                    long nextLeastFreq = Long.MAX_VALUE;
                    for (long thisFreq : freqEntryListMap.keySet()) {
                        if (thisFreq < nextLeastFreq) {
                            nextLeastFreq = thisFreq;
                        }
                    }
                    minFrequency = nextLeastFreq;
                }
            }
        }
    }

    @Override
    protected void clear() {
        keyEntryMap.clear();
        freqEntryListMap.clear();
        minFrequency = 0;
    }

    private LinkedList<Entry> getUpdatedFreqEntryList(Entry entry) {
        long freq = entry.freq;
        freqEntryListMap.get(freq).remove(entry);
        if (freqEntryListMap.get(freq).size() == 0) {
            freqEntryListMap.remove(freq);
            if (minFrequency == freq) {
                minFrequency += 1;
            }
        }
        return freqEntryListMap.getOrDefault(freq + 1, new LinkedList<>());
    }
}
