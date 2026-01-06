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
        var res = (RocksDbTransactionManager.RocksTxResource)
                TransactionSynchronizationManager.getResource(db);

        if (res == null) return new AutoCommitSession(db);
        return new TxBoundSession(db, res);
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
        private final TransactionDB db;
        private final RocksDbTransactionManager.RocksTxResource res;

        TxBoundSession(TransactionDB db, RocksDbTransactionManager.RocksTxResource res) {
            this.db = db;
            this.res = res;
        }

        @Override public ReadOptions newReadOptions() {
            ReadOptions ro = new ReadOptions();
            // Ensure consistent reads inside tx if snapshot was set:
            var snap = res.txn.getSnapshot();
            if (snap != null) ro.setSnapshot(snap);
            return ro;
        }

        @Override
        public byte[] get(ColumnFamilyHandle cf, ReadOptions ro, byte[] key) throws RocksDBException {
            return res.txn.get(cf, ro, key); // reads txn’s uncommitted writes too :contentReference[oaicite:5]{index=5}
        }

        @Override
        public RocksIterator iterator(ColumnFamilyHandle cf, ReadOptions ro) {
            return res.txn.getIterator(ro, cf); // includes uncommitted txn writes :contentReference[oaicite:6]{index=6}
        }

        @Override
        public void write(RocksWriteBatch batch) throws RocksDBException {
            for (var op : batch.ops()) {
                if (op instanceof RocksWriteBatch.Put p) res.txn.put(p.cf(), p.key(), p.value());
                else if (op instanceof RocksWriteBatch.Delete d) res.txn.delete(d.cf(), d.key());
            }
        }
    }
}
