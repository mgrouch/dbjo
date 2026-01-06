package org.github.dbjo.rdb;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.TransactionDB;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public final class DaoRegistry implements AutoCloseable {
    private final TransactionDB txDb;
    private final Map<String, ColumnFamilyHandle> cfByName;
    private final SpringRocksAccess access;

    private final Map<String, AbstractRocksDao<?, ?>> daos = new HashMap<>();

    public DaoRegistry(TransactionDB txDb, Map<String, ColumnFamilyHandle> cfByName) {
        this.txDb = Objects.requireNonNull(txDb);
        this.cfByName = Map.copyOf(Objects.requireNonNull(cfByName));
        this.access = new SpringRocksAccess(txDb, this.cfByName);
    }

    public <T, K, D extends AbstractRocksDao<T, K>> D register(DaoDefinition<T, K, D> def) {
        ColumnFamilyHandle primary = cfByName.get(def.primaryCf());
        if (primary == null) throw new IllegalArgumentException("Unknown CF: " + def.primaryCf());

        Map<String, ColumnFamilyHandle> idx = new HashMap<>();
        def.indexNameToCf().forEach((idxName, cfName) -> {
            ColumnFamilyHandle cf = cfByName.get(cfName);
            if (cf == null) throw new IllegalArgumentException("Unknown index CF: " + cfName);
            idx.put(idxName, cf);
        });

        D dao = def.factory().create(access, primary, Map.copyOf(idx), def.keyCodec(), def.valueCodec());
        daos.put(def.name(), dao);
        return dao;
    }

    @Override public void close() {
        daos.clear();
    }
}
