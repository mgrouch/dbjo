package org.github.dbjo.rdb;

import org.rocksdb.*;
import org.springframework.transaction.support.TransactionSynchronizationManager;

public final class SpringRocksSessions implements RocksSessions {
    private final TransactionDB db;

    public SpringRocksSessions(TransactionDB db) {
        this.db = db;
    }

    @Override
    public RocksSession current() {
        Object txnObj = TransactionSynchronizationManager.getResource(RocksDbTransactionManager.Keys.TXN);
        if (txnObj instanceof Transaction txn) {
            return new TxBoundSession(db, txn);
        }
        return new AutoCommitSession(db);
    }

    private static final class AutoCommitSession implements RocksSession {
        private final TransactionDB db;

        AutoCommitSession(TransactionDB db) { this.db = db; }

        @Override public ReadOptions newReadOptions() { return new ReadOptions(); }

        @Override
        public byte[] get(ColumnFamilyHandle cf, ReadOptions ro, byte[] key) throws RocksDBException {
            return db.get(cf, ro, key);
        }

        @Override
        public RocksIterator iterator(ColumnFamilyHandle cf, ReadOptions ro) {
            return db.newIterator(cf, ro);
        }

        @Override
        public void write(RocksWriteBatch batch) throws RocksDBException {
            if (batch.isEmpty()) return;
            try (WriteOptions wo = new WriteOptions(); WriteBatch wb = new WriteBatch()) {
                for (var op : batch.ops()) {
                    if (op instanceof RocksWriteBatch.Put p) wb.put(p.cf(), p.key(), p.value());
                    else if (op instanceof RocksWriteBatch.Delete d) wb.delete(d.cf(), d.key());
                }
                db.write(wo, wb);
            }
        }
    }

    private static final class TxBoundSession implements RocksSession {
        @SuppressWarnings("unused")
        private final TransactionDB db; // not strictly needed, but handy if you extend later
        private final Transaction txn;

        TxBoundSession(TransactionDB db, Transaction txn) {
            this.db = db;
            this.txn = txn;
        }

        @Override public ReadOptions newReadOptions() {
            ReadOptions ro = new ReadOptions();
            // If your tx manager called txn.setSnapshot() at begin, this gives repeatable reads:
            Snapshot snap = txn.getSnapshot();
            if (snap != null) ro.setSnapshot(snap);
            return ro;
        }

        @Override
        public byte[] get(ColumnFamilyHandle cf, ReadOptions ro, byte[] key) throws RocksDBException {
            return txn.get(cf, ro, key); // sees uncommitted txn writes
        }

        @Override
        public RocksIterator iterator(ColumnFamilyHandle cf, ReadOptions ro) {
            return txn.getIterator(ro, cf); // can include uncommitted txn writes
        }

        @Override
        public void write(RocksWriteBatch batch) throws RocksDBException {
            for (var op : batch.ops()) {
                if (op instanceof RocksWriteBatch.Put p) txn.put(p.cf(), p.key(), p.value());
                else if (op instanceof RocksWriteBatch.Delete d) txn.delete(d.cf(), d.key());
            }
        }
    }
}
