package org.apache.flink.contrib.streaming.state.cache;

/** Javadoc for CacheManagerFactory. */
public class CacheManagerFactory {

    // Gets Default Cachemanager of the cache
    public static AbstractCacheManager getDefaultCacheManager(int size) {
        return new ClockCacheManager(size);
    }
}
