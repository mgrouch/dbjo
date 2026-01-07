package org.github.dbjo.rdb;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;

import java.util.*;

public abstract class IndexedRocksDao<T, K> extends AbstractRocksDao<T, K> {

    private static final byte[] EMPTY = new byte[0];
    private final List<IndexDef<T>> indexes;

    protected IndexedRocksDao(
            RocksSessions sessions,
            ColumnFamilyHandle primaryCf,
            KeyCodec<K> keyCodec,
            Codec<T> valueCodec,
            Map<String, ColumnFamilyHandle> indexCfs,
            List<IndexDef<T>> indexes
    ) {
        super(sessions, primaryCf, keyCodec, valueCodec, indexCfs);
        this.indexes = List.copyOf(indexes);
    }

    /** Convenience: pull key/codec/indexes from EntityDef. */
    protected IndexedRocksDao(RocksSessions sessions, EntityDef<T, K> entity, Map<String, ColumnFamilyHandle> indexCfs) {
        this(sessions, entity.primaryCf(), entity.keyCodec(), entity.valueCodec(), indexCfs, entity.indexes());
    }

    protected IndexedRocksDao(RocksSessions sessions, ResolvedEntityDef<T, K> ent) {
        this(sessions,
                ent.def().primaryCf(),
                ent.def().keyCodec(),
                ent.def().valueCodec(),
                ent.indexCfs(),
                ent.def().indexes());
    }

    @Override
    protected final void maintainIndexes(RocksWriteBatch batch, K key, T oldValueOrNull, T newValue)
            throws RocksDBException {

        final byte[] pk = keyCodec.encodeKey(key);

        for (IndexDef<T> idx : indexes) {
            ColumnFamilyHandle cf = indexCfs.get(idx.name());
            if (cf == null) throw new IllegalStateException("Missing index CF for " + idx.name());

            Set<ByteArrayKey> oldKeys = toSet(idx.valueKeysOrEmpty(oldValueOrNull));
            Set<ByteArrayKey> newKeys = toSet(idx.valueKeysOrEmpty(newValue));

            // delete removed
            for (ByteArrayKey v : oldKeys) {
                if (!newKeys.contains(v)) {
                    batch.delete(cf, IndexKeys.unique(v.bytes(), pk));
                }
            }
            // insert added
            for (ByteArrayKey v : newKeys) {
                if (!oldKeys.contains(v)) {
                    batch.put(cf, IndexKeys.unique(v.bytes(), pk), EMPTY);
                }
            }
        }
    }

    @Override
    protected final void maintainIndexesOnDelete(RocksWriteBatch batch, K key, T oldValue)
            throws RocksDBException {

        final byte[] pk = keyCodec.encodeKey(key);

        for (IndexDef<T> idx : indexes) {
            ColumnFamilyHandle cf = indexCfs.get(idx.name());
            if (cf == null) throw new IllegalStateException("Missing index CF for " + idx.name());

            for (byte[] v : idx.valueKeysOrEmpty(oldValue)) {
                if (v == null) continue;
                batch.delete(cf, IndexKeys.unique(v, pk));
            }
        }
    }

    private static Set<ByteArrayKey> toSet(Iterable<byte[]> keys) {
        HashSet<ByteArrayKey> s = new HashSet<>();
        for (byte[] k : keys) {
            if (k == null) continue;
            s.add(new ByteArrayKey(k));
        }
        return s;
    }
}
