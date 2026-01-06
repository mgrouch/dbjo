package org.github.dbjo.rdb;

import org.rocksdb.ColumnFamilyHandle;
import java.util.Map;

public record DaoDefinition<T, K, D extends AbstractRocksDao<T, K>>(
        String name,
        String primaryCf,
        Map<String, String> indexNameToCf,
        KeyCodec<K> keyCodec,
        Codec<T> valueCodec,
        Factory<T, K, D> factory
) {
    @FunctionalInterface
    public interface Factory<T, K, D extends AbstractRocksDao<T, K>> {
        D create(RocksSessions access,
                 ColumnFamilyHandle primaryCf,
                 Map<String, ColumnFamilyHandle> indexCfs,
                 KeyCodec<K> keyCodec,
                 Codec<T> valueCodec);
    }
}
