package org.github.dbjo.rdb;

import org.rocksdb.*;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionSystemException;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;
import org.springframework.transaction.support.TransactionSynchronizationManager;

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
        // Optional: this.setRollbackOnCommitFailure(true);
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
            // WriteOptions is a native resource; close after beginTransaction returns.
            Transaction txn;
            try (WriteOptions wo = new WriteOptions()) {
                // You can tune wo here (disableWAL, sync, etc.)
                txn = txDb.beginTransaction(wo);
            }

            // Enable snapshot for repeatable reads inside this transaction.
            // This makes txn.getSnapshot() non-null.
            txn.setSnapshot();

            // Bind txn for the session provider (RocksSessions)
            TransactionSynchronizationManager.bindResource(Keys.TXN, txn);

            // Optionally bind a shared ReadOptions if you want a single RO instance.
            // Your RocksSession.newReadOptions() can also just create per-use ReadOptions.
            ReadOptions ro = new ReadOptions();
            Snapshot snap = txn.getSnapshot();
            if (snap != null) ro.setSnapshot(snap);
            TransactionSynchronizationManager.bindResource(Keys.READ_OPTIONS, ro);

        } catch (Exception e) {
            throw new TransactionSystemException("RocksDB begin failed", e);
        }
    }

    @Override
    protected void doCommit(DefaultTransactionStatus status) {
        Object res = TransactionSynchronizationManager.getResource(Keys.TXN);
        if (res instanceof Transaction txn) {
            try {
                txn.commit();
            } catch (RocksDBException e) {
                throw new TransactionSystemException("RocksDB commit failed", e);
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
                throw new TransactionSystemException("RocksDB rollback failed", e);
            }
        }
    }

    @Override
    protected void doCleanupAfterCompletion(Object transaction) {
        // Unbind in reverse order of typical usage
        Object roObj  = TransactionSynchronizationManager.unbindResourceIfPossible(Keys.READ_OPTIONS);
        Object txnObj = TransactionSynchronizationManager.unbindResourceIfPossible(Keys.TXN);

        if (roObj instanceof ReadOptions ro) {
            ro.close();
        }

        if (txnObj instanceof Transaction txn) {
            // Clear snapshot and close txn (both native)
            try {
                txn.clearSnapshot();
            } catch (Throwable ignored) {
                // clearSnapshot may throw if txn already closed; ignore
            }
            txn.close();
        }
    }
}
