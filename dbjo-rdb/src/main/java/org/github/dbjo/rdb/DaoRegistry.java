package org.github.dbjo.rdb;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.TransactionDB;

import java.util.HashMap;
import java.util.Map;

public final class DaoRegistry implements AutoCloseable {
    private final TransactionDB txDb;
    private final Map<String, ColumnFamilyHandle> cfByName;
    private final SpringRocksAccess access;

    private final Map<String, AbstractRocksDao<?,?>> daos = new HashMap<>();

    public DaoRegistry(TransactionDB txDb, Map<String, ColumnFamilyHandle> cfByName) {
        this.txDb = txDb;
        this.cfByName = cfByName;
        this.access = new SpringRocksAccess(txDb, cfByName);
    }

    public <T,K> AbstractRocksDao<T,K> register(DaoDefinition<T,K> def) {
        ColumnFamilyHandle primary = cfByName.get(def.primaryCf());
        Map<String, ColumnFamilyHandle> idx = new HashMap<>();
        def.indexNameToCf().forEach((idxName, cfName) -> idx.put(idxName, cfByName.get(cfName)));

        var dao = def.factory().create(access, primary, Map.copyOf(idx), def.keyCodec(), def.valueCodec());
        daos.put(def.name(), dao);
        @SuppressWarnings("unchecked")
        var typed = (AbstractRocksDao<T,K>) dao;
        return typed;
    }

    @Override public void close() {
        // DAOs usually don’t own the DB; close DB elsewhere.
        daos.clear();
    }
}
