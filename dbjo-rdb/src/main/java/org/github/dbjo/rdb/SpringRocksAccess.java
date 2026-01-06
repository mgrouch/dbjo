package org.github.dbjo.rdb;

import org.rocksdb.ReadOptions;
import org.rocksdb.Transaction;
import org.rocksdb.TransactionDB;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.util.Map;
import java.util.Objects;

public final class SpringRocksAccess {

    private final TransactionDB txDb;
    private final Map<String, ?> cfByName; // keep whatever type you had

    public SpringRocksAccess(TransactionDB txDb, Map<String, ?> cfByName) {
        this.txDb = Objects.requireNonNull(txDb);
        this.cfByName = Objects.requireNonNull(cfByName);
    }

    public TransactionDB txDb() {
        return txDb;
    }

    public Transaction currentTxnOrNull() {
        Object res = TransactionSynchronizationManager.getResource(RocksDbTransactionManager.Keys.TXN);
        return (res instanceof Transaction t) ? t : null;
    }

    public ReadOptions currentReadOptionsOrNull() {
        Object res = TransactionSynchronizationManager.getResource(RocksDbTransactionManager.Keys.READ_OPTIONS);
        return (res instanceof ReadOptions ro) ? ro : null;
    }
}
