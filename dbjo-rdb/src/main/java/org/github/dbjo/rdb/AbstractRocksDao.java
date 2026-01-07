package org.github.dbjo.rdb;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public abstract class AbstractRocksDao<T, K> implements Dao<T, K> {

    private final RocksSessions sessions;
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

    /** Convenience: matches your EntityDef-based design. */
    protected AbstractRocksDao(RocksSessions sessions, EntityDef<T, K> entity, Map<String, ColumnFamilyHandle> indexCfs) {
        this(sessions, entity.primaryCf(), entity.keyCodec(), entity.valueCodec(), indexCfs);
    }

    protected AbstractRocksDao(RocksSessions sessions, ResolvedEntityDef<T, K> ent) {
        this(sessions,
                ent.def().primaryCf(),
                ent.def().keyCodec(),
                ent.def().valueCodec(),
                ent.indexCfs());
    }

    @Override
    public Optional<T> findByKey(K key) {
        Objects.requireNonNull(key);
        try {
            RocksSession s = sessions.current();
            byte[] kb = keyCodec.encodeKey(key);
            try (ReadOptions ro = s.newReadOptions()) {
                byte[] vb = s.get(primaryCf, ro, kb);
                return vb == null ? Optional.empty() : Optional.of(valueCodec.decode(vb));
            }
        } catch (RocksDBException e) {
            throw new RocksDaoException("findByKey failed", e);
        }
    }

    @Override
    public void upsert(K key, T value) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(value);

        try {
            RocksSession s = sessions.current();
            byte[] kb = keyCodec.encodeKey(key);

            // Read old using one RO (tx snapshot-consistent)
            T oldOrNull;
            try (ReadOptions ro = s.newReadOptions()) {
                byte[] oldBytes = s.get(primaryCf, ro, kb);
                oldOrNull = (oldBytes == null) ? null : valueCodec.decode(oldBytes);
            }

            RocksWriteBatch batch = new RocksWriteBatch();
            batch.put(primaryCf, kb, valueCodec.encode(value));

            maintainIndexes(batch, key, oldOrNull, value);

            s.write(batch);
        } catch (RocksDBException e) {
            throw new RocksDaoException("upsert failed", e);
        }
    }

    @Override
    public boolean delete(K key) {
        Objects.requireNonNull(key);

        try {
            RocksSession s = sessions.current();
            byte[] kb = keyCodec.encodeKey(key);

            T oldOrNull;
            try (ReadOptions ro = s.newReadOptions()) {
                byte[] oldBytes = s.get(primaryCf, ro, kb);
                oldOrNull = (oldBytes == null) ? null : valueCodec.decode(oldBytes);
            }

            if (oldOrNull == null) return false;

            RocksWriteBatch batch = new RocksWriteBatch();
            batch.delete(primaryCf, kb);

            maintainIndexesOnDelete(batch, key, oldOrNull);

            s.write(batch);
            return true;
        } catch (RocksDBException e) {
            throw new RocksDaoException("delete failed", e);
        }
    }

    @Override
    public boolean containsKey(K key) {
        Objects.requireNonNull(key);
        return existsKeyBytes(keyCodec.encodeKey(key));
    }

    protected boolean existsKeyBytes(byte[] kb) {
        try {
            RocksSession s = sessions.current();
            try (ReadOptions ro = s.newReadOptions()) {
                return s.get(primaryCf, ro, kb) != null;
            }
        } catch (RocksDBException e) {
            throw new RocksDaoException("exists failed", e);
        }
    }

    @Override
    public void forEach(java.util.function.Consumer<Map.Entry<K, T>> consumer) {
        Objects.requireNonNull(consumer);
        try (Stream<Map.Entry<K, T>> st = stream(Query.<K>builder().limit(Integer.MAX_VALUE).build())) {
            st.forEach(consumer);
        }
    }

    /** Your scan API (primary or index-driven) */
    public Stream<Map.Entry<K, T>> stream(Query<K> q) {
        DaoSpliterator<K, T> sp = new DaoSpliterator<>(
                sessions.current(),
                primaryCf,
                indexCfs,
                keyCodec,
                valueCodec,
                q
        );
        return StreamSupport.stream(sp, false).onClose(sp::close);
    }

    @Override
    public void close() { /* no-op */ }

    protected abstract void maintainIndexes(RocksWriteBatch batch, K key, T oldValueOrNull, T newValue) throws RocksDBException;
    protected abstract void maintainIndexesOnDelete(RocksWriteBatch batch, K key, T oldValue) throws RocksDBException;
}
