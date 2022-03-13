package org.apache.flink.contrib.streaming.state.cache;

/** Javadoc for CacheManagerFactory. */
public class CacheManagerFactory {

    // Gets Default Cachemanager of the cache
    public static AbstractCacheManager getDefaultCacheManager(int size) {
        return new LRUCacheManager(size);
    }

    public static AbstractCacheManager getLRUCacheManager(int size) {
        return new LRUCacheManager(size);
    }

    public static AbstractCacheManager getLIFOCacheManager(int size) {
        return new LIFOCacheManager(size);
    }

    public static AbstractCacheManager getClockCacheManager(int size) {
        return new ClockCacheManager(size);
    }

    public static AbstractCacheManager getLFUCacheManager(int size) {
        return new LFUCacheManager(size);
    }
}
