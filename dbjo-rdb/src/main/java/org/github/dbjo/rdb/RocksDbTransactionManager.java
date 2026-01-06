package org.github.dbjo.rdb;

import org.rocksdb.*;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.DefaultTransactionStatus;

public final class RocksDbTransactionManager extends AbstractPlatformTransactionManager {

    /** Public resource keys (no leaking TxObject type). */
    public static final class Keys {
        private Keys() {}
        public static final Object TXN = new Object();
        public static final Object READ_OPTIONS = new Object();
    }

    private final TransactionDB txDb;

    public RocksDbTransactionManager(TransactionDB txDb) {
        this.txDb = txDb;
    }

    @Override
    protected Object doGetTransaction() {
        // We don't need a custom transaction object for binding.
        return new Object();
    }

    @Override
    protected boolean isExistingTransaction(Object transaction) {
        return TransactionSynchronizationManager.getResource(Keys.TXN) instanceof Transaction;
    }

    @Override
    protected void doBegin(Object transaction, TransactionDefinition definition) {
        try {
            // You can tune these
            WriteOptions wo = new WriteOptions();
            Transaction txn = txDb.beginTransaction(wo);

            ReadOptions ro = new ReadOptions();
            // Optional: snapshot consistency inside a tx
            // Snapshot snap = txDb.getSnapshot();
            // ro.setSnapshot(snap);

            TransactionSynchronizationManager.bindResource(Keys.TXN, txn);
            TransactionSynchronizationManager.bindResource(Keys.READ_OPTIONS, ro);
        } catch (Exception e) {
            throw new org.springframework.transaction.TransactionSystemException("RocksDB begin failed", e);
        }
    }

    @Override
    protected void doCommit(DefaultTransactionStatus status) {
        Object res = TransactionSynchronizationManager.getResource(Keys.TXN);
        if (res instanceof Transaction txn) {
            try {
                txn.commit();
            } catch (RocksDBException e) {
                throw new org.springframework.transaction.TransactionSystemException("RocksDB commit failed", e);
            }
        }
    }

    @Override
    protected void doRollback(DefaultTransactionStatus status) {
        Object res = TransactionSynchronizationManager.getResource(Keys.TXN);
        if (res instanceof Transaction txn) {
            try {
                txn.rollback();
            } catch (RocksDBException e) {
                throw new org.springframework.transaction.TransactionSystemException("RocksDB rollback failed", e);
            }
        }
    }

    @Override
    protected void doCleanupAfterCompletion(Object transaction) {
        Object txnObj = TransactionSynchronizationManager.unbindResourceIfPossible(Keys.TXN);
        Object roObj  = TransactionSynchronizationManager.unbindResourceIfPossible(Keys.READ_OPTIONS);

        if (txnObj instanceof Transaction txn) txn.close();
        if (roObj instanceof ReadOptions ro) ro.close();

        // If you enabled snapshots above, also release them here:
        // if (snap != null) txDb.releaseSnapshot(snap);
    }
}
