package org.github.dbjo.rdb;

import org.rocksdb.*;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.util.Map;

public final class SpringRocksAccess implements RocksAccess {
    private final TransactionDB txDb; // or OptimisticTransactionDB
    private final Map<String, ColumnFamilyHandle> cfs;

    public SpringRocksAccess(TransactionDB txDb, Map<String, ColumnFamilyHandle> cfs) {
        this.txDb = txDb;
        this.cfs = cfs;
    }

    @Override public Transaction currentTxnOrNull() {
        var res = TransactionSynchronizationManager.getResource(txDb);
        if (res instanceof RocksDbTransactionManager.TxObject txObj) return txObj.transaction;
        return null;
    }

    public ReadOptions currentReadOptionsOrNull() {
        var res = TransactionSynchronizationManager.getResource(txDb);
        if (res instanceof RocksDbTransactionManager.TxObject txObj) return txObj.readOptions;
        return null;
    }

    @Override public TransactionDB txDb() { return txDb; }
    @Override public ColumnFamilyHandle cf(String name) { return cfs.get(name); }
}
