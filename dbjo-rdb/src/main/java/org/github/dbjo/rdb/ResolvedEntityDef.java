package org.github.dbjo.rdb;

import org.rocksdb.ColumnFamilyHandle;

import java.util.Map;
import java.util.Objects;

public record ResolvedEntityDef<T, K>(
        EntityDef<T, K> def,
        Map<String, ColumnFamilyHandle> indexCfs
) {
    public ResolvedEntityDef {
        Objects.requireNonNull(def, "def");
        Objects.requireNonNull(indexCfs, "indexCfs");
        indexCfs = Map.copyOf(indexCfs);
    }

    public ColumnFamilyHandle indexCf(String indexName) {
        ColumnFamilyHandle h = indexCfs.get(indexName);
        if (h == null) throw new IllegalArgumentException("Unknown index CF for index: " + indexName);
        return h;
    }
}
