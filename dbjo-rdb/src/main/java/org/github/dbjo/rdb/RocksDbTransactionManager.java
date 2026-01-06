package org.github.dbjo.rdb;

import org.rocksdb.*;
import org.springframework.transaction.*;
import org.springframework.transaction.support.*;

public final class RocksDbTransactionManager extends AbstractPlatformTransactionManager {

    private final TransactionDB db;

    public RocksDbTransactionManager(TransactionDB db) {
        this.db = db;
        setNestedTransactionAllowed(false);
    }

    private static final class TxObject {
        RocksTxResource resource;
    }

    static final class RocksTxResource implements AutoCloseable {
        final Transaction txn;
        final WriteOptions wo;
        final TransactionOptions to;

        RocksTxResource(Transaction txn, WriteOptions wo, TransactionOptions to) {
            this.txn = txn;
            this.wo = wo;
            this.to = to;
        }

        @Override public void close() {
            txn.close(); // Transaction is AutoCloseable
            wo.close();
            to.close();
        }
    }

    @Override
    protected Object doGetTransaction() {
        TxObject obj = new TxObject();
        obj.resource = (RocksTxResource) TransactionSynchronizationManager.getResource(db);
        return obj;
    }

    @Override
    protected boolean isExistingTransaction(Object transaction) {
        return ((TxObject) transaction).resource != null;
    }

    @Override
    protected void doBegin(Object transaction, TransactionDefinition definition) {
        TxObject obj = (TxObject) transaction;
        if (obj.resource != null) return;

        WriteOptions wo = new WriteOptions();
        TransactionOptions to = new TransactionOptions();
        Transaction txn = db.beginTransaction(wo, to); // :contentReference[oaicite:3]{index=3}

        // Optional (recommended for repeatable reads inside txn):
        txn.setSnapshot(); // :contentReference[oaicite:4]{index=4}

        obj.resource = new RocksTxResource(txn, wo, to);

        TransactionSynchronizationManager.bindResource(db, obj.resource);
    }

    @Override
    protected void doCommit(DefaultTransactionStatus status) throws TransactionException {
        TxObject obj = (TxObject) status.getTransaction();
        try {
            obj.resource.txn.commit();
        } catch (RocksDBException e) {
            throw new TransactionSystemException("RocksDB commit failed", e);
        }
    }

    @Override
    protected void doRollback(DefaultTransactionStatus status) throws TransactionException {
        TxObject obj = (TxObject) status.getTransaction();
        try {
            obj.resource.txn.rollback();
        } catch (RocksDBException e) {
            throw new TransactionSystemException("RocksDB rollback failed", e);
        }
    }

    @Override
    protected void doCleanupAfterCompletion(Object transaction) {
        TxObject obj = (TxObject) transaction;
        RocksTxResource res = obj.resource;
        if (res != null) {
            TransactionSynchronizationManager.unbindResource(db);
            res.close();
            obj.resource = null;
        }
    }
}
