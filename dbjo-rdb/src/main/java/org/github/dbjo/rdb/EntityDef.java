package org.github.dbjo.rdb;

import org.rocksdb.ColumnFamilyHandle;

import java.util.List;
import java.util.Objects;

public record EntityDef<T, K>(
        String name,
        ColumnFamilyHandle primaryCf,
        KeyCodec<K> keyCodec,
        Codec<T> valueCodec,
        List<? extends IndexDef<T, ?>> indexes
) {
    public EntityDef {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(primaryCf, "primaryCf");
        Objects.requireNonNull(keyCodec, "keyCodec");
        Objects.requireNonNull(valueCodec, "valueCodec");
        Objects.requireNonNull(indexes, "indexes");
        indexes = List.copyOf(indexes);
    }

    public static <T, K> EntityDef<T, K> of(
            String name,
            ColumnFamilyHandle cf,
            KeyCodec<K> keyCodec,
            Codec<T> valueCodec,
            List<? extends IndexDef<T, ?>> indexes
    ) {
        return new EntityDef<>(name, cf, keyCodec, valueCodec, indexes);
    }
}
