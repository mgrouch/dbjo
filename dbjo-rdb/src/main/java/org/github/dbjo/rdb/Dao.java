package org.github.dbjo.rdb;

import java.util.*;
import java.util.function.Consumer;

public interface Dao<T, K> extends AutoCloseable {
    Optional<T> findByKey(K key);

    void upsert(K key, T value);

    boolean delete(K key);

    default int deleteByIds(Collection<K> ids) {
        Objects.requireNonNull(ids, "ids");
        int deleted = 0;
        for (K id : ids) {
            if (delete(id)) deleted++;
        }
        return deleted;
    }

    boolean containsKey(K key);

    // Bulk defaults
    default Map<K, T> getAll(Collection<K> keys) {
        Map<K, T> out = new LinkedHashMap<>();
        for (K k : keys) findByKey(k).ifPresent(v -> out.put(k, v));
        return out;
    }

    default void putAll(Map<K, T> entries) {
        for (var e : entries.entrySet()) upsert(e.getKey(), e.getValue());
    }

    default long approximateSize() { return -1; }

    void forEach(Consumer<Map.Entry<K, T>> consumer);

    @Override
    void close();
}
