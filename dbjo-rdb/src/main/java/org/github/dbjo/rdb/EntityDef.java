package org.github.dbjo.rdb;

import org.rocksdb.ColumnFamilyHandle;

import java.util.List;
import java.util.Objects;

public record EntityDef<T, K>(
        String name,
        ColumnFamilyHandle primaryCf,
        KeyCodec<K> keyCodec,
        Codec<T> valueCodec,
        List<IndexDef<T>> indexes
) {
    public EntityDef {
        Objects.requireNonNull(name);
        Objects.requireNonNull(primaryCf);
        Objects.requireNonNull(keyCodec);
        Objects.requireNonNull(valueCodec);
        indexes = List.copyOf(indexes);
    }
}
