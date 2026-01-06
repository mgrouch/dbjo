package org.github.dbjo.rdb;

import java.util.*;
import java.util.function.Consumer;

public interface Dao<T, K> extends AutoCloseable {
    Optional<T> get(K key);

    // Put / update
    void put(K key, T value);

    // Create-only (fails if exists) or Update-only are easy to add later.

    void upsert(K key, T value);

    boolean delete(K key);

    boolean containsKey(K key);

    // Bulk
    Map<K, T> getAll(Collection<K> keys);

    void putAll(Map<K, T> entries);

    long approximateSize(); // cheap-ish; can be implemented with properties

    // Iteration (lexicographic by key bytes)
    void forEach(Consumer<Map.Entry<K, T>> consumer);

    @Override
    void close();
}

