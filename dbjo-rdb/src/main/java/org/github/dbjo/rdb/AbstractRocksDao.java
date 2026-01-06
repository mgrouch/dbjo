package org.github.dbjo.rdb;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public abstract class AbstractRocksDao<T, K> implements Dao<T, K> {
    private final RocksSessions sessions;              // provider (tx-aware outside DAO)
    protected final ColumnFamilyHandle primaryCf;
    protected final KeyCodec<K> keyCodec;
    protected final Codec<T> valueCodec;
    protected final Map<String, ColumnFamilyHandle> indexCfs;

    protected AbstractRocksDao(
            RocksSessions sessions,
            ColumnFamilyHandle primaryCf,
            KeyCodec<K> keyCodec,
            Codec<T> valueCodec,
            Map<String, ColumnFamilyHandle> indexCfs
    ) {
        this.sessions = Objects.requireNonNull(sessions);
        this.primaryCf = Objects.requireNonNull(primaryCf);
        this.keyCodec = Objects.requireNonNull(keyCodec);
        this.valueCodec = Objects.requireNonNull(valueCodec);
        this.indexCfs = Map.copyOf(indexCfs);
    }

    @Override
    public Optional<T> get(K key) {
        Objects.requireNonNull(key);
        try {
            RocksSession s = sessions.current();
            byte[] kb = keyCodec.encodeKey(key);
            byte[] vb = s.get(primaryCf, kb);
            return vb == null ? Optional.empty() : Optional.of(valueCodec.decode(vb));
        } catch (RocksDBException e) {
            throw new RocksDaoException("get failed", e);
        }
    }

    @Override
    public void upsert(K key, T value) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(value);

        try {
            RocksSession s = sessions.current();
            Optional<T> old = get(key); // consistent if session uses snapshot in tx

            byte[] kb = keyCodec.encodeKey(key);
            byte[] vb = valueCodec.encode(value);

            RocksWriteBatch batch = new RocksWriteBatch();
            batch.put(primaryCf, kb, vb);

            maintainIndexes(batch, key, old.orElse(null), value);

            s.write(batch);
        } catch (RocksDBException e) {
            throw new RocksDaoException("upsert failed", e);
        }
    }

    @Override
    public boolean delete(K key) {
        Objects.requireNonNull(key);

        Optional<T> old = get(key);
        if (old.isEmpty()) return false;

        try {
            RocksSession s = sessions.current();

            byte[] kb = keyCodec.encodeKey(key);

            RocksWriteBatch batch = new RocksWriteBatch();
            batch.delete(primaryCf, kb);

            maintainIndexesOnDelete(batch, key, old.get());

            s.write(batch);
            return true;
        } catch (RocksDBException e) {
            throw new RocksDaoException("delete failed", e);
        }
    }

    public Stream<Map.Entry<K, T>> stream(Query<K> q) {
        DaoSpliterator<K, T> sp = new DaoSpliterator<>(
                sessions.current(),
                primaryCf,
                indexCfs,     // <-- missing argument
                keyCodec,
                valueCodec,
                q
        );
        return StreamSupport.stream(sp, false).onClose(sp::close);
    }
    protected abstract void maintainIndexes(RocksWriteBatch batch, K key, T oldValueOrNull, T newValue) throws RocksDBException;
    protected abstract void maintainIndexesOnDelete(RocksWriteBatch batch, K key, T oldValue) throws RocksDBException;
}
