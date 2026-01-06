package org.github.dbjo.rdb;

import org.rocksdb.ColumnFamilyHandle;

import java.util.Map;

public record DaoDefinition<T,K>(
        String name,
        String primaryCf,
        Map<String,String> indexNameToCf,
        KeyCodec<K> keyCodec,
        Codec<T> valueCodec,
        DaoFactory<T,K> factory
) {
    public interface DaoFactory<T,K> {
        AbstractRocksDao<T,K> create(SpringRocksAccess access,
                                     ColumnFamilyHandle primary,
                                     Map<String, ColumnFamilyHandle> indexes,
                                     KeyCodec<K> keyCodec,
                                     Codec<T> valueCodec);
    }
}

