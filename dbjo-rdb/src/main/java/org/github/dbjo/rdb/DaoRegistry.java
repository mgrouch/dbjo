package org.github.dbjo.rdb;

import org.rocksdb.ColumnFamilyHandle;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public final class DaoRegistry implements AutoCloseable {
    private final RocksSessions sessions;
    private final RocksDbHandle h;
    private final Map<String, Dao<?, ?>> daosByName = new HashMap<>();

    public DaoRegistry(RocksSessions sessions, RocksDbHandle h) {
        this.sessions = Objects.requireNonNull(sessions);
        this.h = Objects.requireNonNull(h);
    }

    public <T, K, D extends Dao<T, K>> D register(DaoDefinition<T, K, D> def) {
        Objects.requireNonNull(def);

        ColumnFamilyHandle primary = h.cf(def.primaryCf());

        Map<String, ColumnFamilyHandle> idx = new HashMap<>();
        def.indexNameToCf().forEach((idxName, cfName) -> idx.put(idxName, h.cf(cfName)));

        D dao = def.factory().create(
                sessions,
                primary,
                Map.copyOf(idx),
                def.keyCodec(),
                def.valueCodec()
        );

        daosByName.put(def.name(), dao);
        return dao;
    }

    @SuppressWarnings("unchecked")
    public <D extends Dao<?, ?>> D get(String name) {
        return (D) daosByName.get(name);
    }

    @Override
    public void close() {
        daosByName.clear();
    }
}
