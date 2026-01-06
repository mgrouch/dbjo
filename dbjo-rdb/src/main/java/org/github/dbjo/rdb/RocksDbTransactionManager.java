package org.github.dbjo.rdb;

import org.rocksdb.*;
import org.springframework.transaction.*;
import org.springframework.transaction.support.*;

import java.util.Objects;

public final class RocksDbTransactionManager extends AbstractPlatformTransactionManager {
    private final TransactionDB txDb; // or OptimisticTransactionDB
    private final WriteOptions writeOptions;

    public RocksDbTransactionManager(TransactionDB txDb) {
        this.txDb = Objects.requireNonNull(txDb);
        this.writeOptions = new WriteOptions();
        setNestedTransactionAllowed(false);
    }

    @Override
    protected Object doGetTransaction() {
        return new TxObject();
    }

    @Override
    protected boolean isExistingTransaction(Object transaction) {
        TxObject tx = (TxObject) transaction;
        return tx.transaction != null;
    }

    @Override
    protected void doBegin(Object transaction, TransactionDefinition definition) {
        TxObject tx = (TxObject) transaction;
        tx.transaction = txDb.beginTransaction(writeOptions);
        // Optional: snapshot for repeatable reads
        tx.snapshot = txDb.getSnapshot();
        tx.readOptions = new ReadOptions().setSnapshot(tx.snapshot);

        TransactionSynchronizationManager.bindResource(txDb, tx);
    }

    @Override
    protected void doCommit(DefaultTransactionStatus status) {
        TxObject tx = (TxObject) TransactionSynchronizationManager.getResource(txDb);
        try {
            tx.transaction.commit();
        } catch (RocksDBException e) {
            throw new TransactionSystemException("RocksDB commit failed", e);
        } finally {
            cleanup(tx);
        }
    }

    @Override
    protected void doRollback(DefaultTransactionStatus status) {
        TxObject tx = (TxObject) TransactionSynchronizationManager.getResource(txDb);
        try {
            tx.transaction.rollback();
        } catch (RocksDBException e) {
            throw new TransactionSystemException("RocksDB rollback failed", e);
        } finally {
            cleanup(tx);
        }
    }

    @Override
    protected void doCleanupAfterCompletion(Object transaction) {
        TxObject tx = (TxObject) TransactionSynchronizationManager.unbindResource(txDb);
        if (tx != null) cleanup(tx);
    }

    private void cleanup(TxObject tx) {
        try {
            if (tx.readOptions != null) tx.readOptions.close();
            if (tx.snapshot != null) txDb.releaseSnapshot(tx.snapshot);
            if (tx.transaction != null) tx.transaction.close();
        } finally {
            tx.transaction = null;
            tx.snapshot = null;
            tx.readOptions = null;
        }
    }

    public static final class TxObject {
        Transaction transaction;
        Snapshot snapshot;
        ReadOptions readOptions;
    }
}
