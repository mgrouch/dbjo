package org.github.dbjo.rdb;

import java.util.*;
import java.util.function.Consumer;

public interface Dao<T, K> extends AutoCloseable {

    Optional<T> findByKey(K key);

    /** Insert or replace. */
    void upsert(K key, T value);

    /** @return true if existed and was deleted. */
    boolean delete(K key);

    /** Iteration in lexicographic order by key bytes (implementation-defined ordering). */
    void forEach(Consumer<Map.Entry<K, T>> consumer);

    /** Release any DAO resources (usually no-op). */
    @Override
    default void close() { /* no-op by default */ }

    // ---- Defaults (derived from primitives) ----

    /** Default existence check. Override for a cheaper existence probe. */
    default boolean containsKey(K key) {
        return findByKey(key).isPresent();
    }

    /** Bulk get; returns only found keys. Override for batched RocksDB multiGet. */
    default Map<K, T> findByKeys(Collection<K> keys) {
        Objects.requireNonNull(keys, "keys");
        Map<K, T> out = new LinkedHashMap<>(Math.max(16, keys.size() * 2));
        for (K k : keys) {
            findByKey(k).ifPresent(v -> out.put(k, v));
        }
        return out;
    }

    /** Bulk put; default is a loop. Override for a true batch/write-optimized implementation. */
    default void storeAll(Map<K, T> entries) {
        Objects.requireNonNull(entries, "entries");
        for (var e : entries.entrySet()) {
            upsert(e.getKey(), e.getValue());
        }
    }

    /**
     * Cheap-ish size estimate. Default returns -1 meaning "unknown".
     * Override using RocksDB properties if you want.
     */
    default long approximateSize() {
        return -1L;
    }
}
