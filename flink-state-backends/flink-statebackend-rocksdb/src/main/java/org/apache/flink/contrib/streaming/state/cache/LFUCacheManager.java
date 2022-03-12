package org.apache.flink.contrib.streaming.state.cache;

import java.util.HashMap;
import java.util.LinkedList;

public class LFUCacheManager <K, V> extends AbstractCacheManager<K, V>{

    protected class Entry {
        K key;
        V val;
        long freq;

        Entry(K key, V val, long freq) {
            this.key = key;
            this.val = val;
            this.freq = freq;
        }
    }

    private long minFrequency;
    private HashMap<K, Entry> keyEntryMap;
    private HashMap<Long, LinkedList<Entry>> freqEntryListMap;

    public LFUCacheManager(int size) {
        super(size);
        this.minFrequency = 0L;
        this.keyEntryMap = new HashMap<>();
        this.freqEntryListMap = new HashMap<>();
    }

    @Override
    public boolean has(K key) {
        return keyEntryMap.containsKey(key);
    }

    @Override
    public V get(K key) {
        if (size == 0 || !keyEntryMap.containsKey(key)) {
            return null;
        }
        Entry entry = keyEntryMap.get(key);
        V val = entry.val;
        long freq = entry.freq;
        freqEntryListMap.get(freq).remove(entry);
        if (freqEntryListMap.get(freq).size() == 0) {
            freqEntryListMap.remove(freq);
            if (minFrequency == freq) {
                minFrequency += 1;
            }
        }
        LinkedList<Entry> list = freqEntryListMap.getOrDefault(freq + 1, new LinkedList<Entry>());
        list.offerFirst(new Entry(key, val, freq));
        freqEntryListMap.put(freq + 1, list);
        keyEntryMap.put(key, freqEntryListMap.get(freq + 1).peekFirst());
        return val;
    }

    @Override
    public void update(K key, V value) {
        if (size == 0) {
            return;
        }
        if (!keyEntryMap.containsKey(key)) {
            if (keyEntryMap.size() == size) {
                evict();
            }
            LinkedList<Entry> list = freqEntryListMap.getOrDefault(1L, new LinkedList<Entry>());
            list.offerFirst(new Entry(key, value, 1));
            freqEntryListMap.put(1L, list);
            keyEntryMap.put(key, freqEntryListMap.get(1L).peekFirst());
            minFrequency = 1;
        } else {
            Entry entry = keyEntryMap.get(key);
            long freq = entry.freq;
            LinkedList<Entry> list = getUpdatedFreqEntryList(entry);
            list.offerFirst(new Entry(key, value, freq + 1));
            freqEntryListMap.put(freq + 1, list);
            keyEntryMap.put(key, freqEntryListMap.get(freq + 1).peekFirst());
        }
    }

    @Override
    protected void evict() {
        Entry entry = freqEntryListMap.get(minFrequency).peekLast();
        assert entry != null;
        keyEntryMap.remove(entry.key);
        freqEntryListMap.get(minFrequency).pollLast();
        if (freqEntryListMap.get(minFrequency).size() == 0) {
            freqEntryListMap.remove(minFrequency);
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
        return freqEntryListMap.getOrDefault(freq + 1, new LinkedList<Entry>());
    }
}
