package org.github.dbjo.rdb;

import org.rocksdb.*;

import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public abstract class AbstractRocksDao<T, K> implements Dao<T, K> {
    protected final SpringRocksAccess access;
    protected final ColumnFamilyHandle primaryCf;
    protected final KeyCodec<K> keyCodec;
    protected final Codec<T> valueCodec;

    // index CFs (recommended): indexName -> CF
    protected final Map<String, ColumnFamilyHandle> indexCfs;

    protected AbstractRocksDao(SpringRocksAccess access,
                               ColumnFamilyHandle primaryCf,
                               KeyCodec<K> keyCodec,
                               Codec<T> valueCodec,
                               Map<String, ColumnFamilyHandle> indexCfs) {
        this.access = access;
        this.primaryCf = primaryCf;
        this.keyCodec = keyCodec;
        this.valueCodec = valueCodec;
        this.indexCfs = Map.copyOf(indexCfs);
    }

    public Optional<T> get(K key) {
        try {
            byte[] kb = keyCodec.encodeKey(key);
            Transaction txn = access.currentTxnOrNull();
            if (txn != null) {
                ReadOptions ro = access.currentReadOptionsOrNull();
                byte[] v = txn.get(primaryCf, ro, kb);
                return v == null ? Optional.empty() : Optional.of(valueCodec.decode(v));
            } else {
                byte[] v = access.txDb().get(primaryCf, kb);
                return v == null ? Optional.empty() : Optional.of(valueCodec.decode(v));
            }
        } catch (RocksDBException e) {
            throw new RocksDaoException("get failed", e);
        }
    }

    public void upsert(K key, T value) {
        try {
            byte[] kb = keyCodec.encodeKey(key);
            byte[] vb = valueCodec.encode(value);

            Transaction txn = access.currentTxnOrNull();
            if (txn != null) {
                // maintain indexes using a WriteBatch-like pattern on the txn
                Optional<T> old = get(key);
                txn.put(primaryCf, kb, vb);
                maintainIndexes(txn, key, old.orElse(null), value);
            } else {
                // outside tx: do batch write
                Optional<T> old = get(key);
                try (WriteBatch wb = new WriteBatch(); WriteOptions wo = new WriteOptions()) {
                    wb.put(primaryCf, kb, vb);
                    maintainIndexes(wb, key, old.orElse(null), value);
                    access.txDb().write(wo, wb);
                }
            }
        } catch (RocksDBException e) {
            throw new RocksDaoException("upsert failed", e);
        }
    }

    public boolean delete(K key) {
        Optional<T> old = get(key);
        if (old.isEmpty()) return false;

        try {
            byte[] kb = keyCodec.encodeKey(key);
            Transaction txn = access.currentTxnOrNull();
            if (txn != null) {
                txn.delete(primaryCf, kb);
                maintainIndexesOnDelete(txn, key, old.get());
            } else {
                try (WriteBatch wb = new WriteBatch(); WriteOptions wo = new WriteOptions()) {
                    wb.delete(primaryCf, kb);
                    maintainIndexesOnDelete(wb, key, old.get());
                    access.txDb().write(wo, wb);
                }
            }
            return true;
        } catch (RocksDBException e) {
            throw new RocksDaoException("delete failed", e);
        }
    }

    /** Criteria query -> Stream (closes iterator on close). */
    public Stream<Map.Entry<K,T>> stream(Query<K> q) {
        var spliterator = new DaoSpliterator<>(this, q);
        return StreamSupport.stream(spliterator, false).onClose(spliterator::close);
    }

    // ---- Index maintenance hooks (batch or txn) ----
    protected abstract void maintainIndexes(IndexWriter w, K key, T oldValueOrNull, T newValue) throws RocksDBException;
    protected abstract void maintainIndexesOnDelete(IndexWriter w, K key, T oldValue) throws RocksDBException;

    /** Unifies WriteBatch and Transaction for index updates. */
    protected interface IndexWriter {
        void put(ColumnFamilyHandle cf, byte[] key, byte[] value) throws RocksDBException;
        void delete(ColumnFamilyHandle cf, byte[] key) throws RocksDBException;
    }

    protected static IndexWriter writer(WriteBatch wb) {
        return new IndexWriter() {
            @Override public void put(ColumnFamilyHandle cf, byte[] key, byte[] value) throws RocksDBException { wb.put(cf, key, value); }
            @Override public void delete(ColumnFamilyHandle cf, byte[] key) throws RocksDBException { wb.delete(cf, key); }
        };
    }

    protected static IndexWriter writer(Transaction txn) {
        return new IndexWriter() {
            @Override public void put(ColumnFamilyHandle cf, byte[] key, byte[] value) throws RocksDBException { txn.put(cf, key, value); }
            @Override public void delete(ColumnFamilyHandle cf, byte[] key) throws RocksDBException { txn.delete(cf, key); }
        };
    }
}
