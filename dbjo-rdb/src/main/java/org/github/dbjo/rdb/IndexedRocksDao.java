package org.github.dbjo.rdb;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;

import java.util.*;

public abstract class IndexedRocksDao<T, K> extends AbstractRocksDao<T, K> {

    private final List<IndexDef<T>> indexes;

    protected IndexedRocksDao(RocksSessions sessions,
                              ColumnFamilyHandle primaryCf,
                              KeyCodec<K> keyCodec,
                              Codec<T> valueCodec,
                              Map<String, ColumnFamilyHandle> indexCfs,
                              List<IndexDef<T>> indexes) {
        super(sessions, primaryCf, keyCodec, valueCodec, indexCfs);
        this.indexes = List.copyOf(indexes);
    }

    @Override
    protected final void maintainIndexes(RocksWriteBatch batch, K key, T oldValueOrNull, T newValue)
            throws RocksDBException {

        byte[] pk = keyCodec.encodeKey(key);

        for (IndexDef<T> idx : indexes) {
            ColumnFamilyHandle cf = indexCfs.get(idx.name());
            if (cf == null) throw new IllegalStateException("Missing index CF for " + idx.name());

            // old set
            Set<ByteArrayKey> oldKeys = toSet(idx.valueKeysOrEmpty(oldValueOrNull));
            // new set
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
                    batch.put(cf, IndexKeys.unique(v.bytes(), pk), new byte[0]);
                }
            }
        }
    }

    @Override
    protected final void maintainIndexesOnDelete(RocksWriteBatch batch, K key, T oldValue)
            throws RocksDBException {

        byte[] pk = keyCodec.encodeKey(key);

        for (IndexDef<T> idx : indexes) {
            ColumnFamilyHandle cf = indexCfs.get(idx.name());
            if (cf == null) throw new IllegalStateException("Missing index CF for " + idx.name());

            for (byte[] v : idx.valueKeysOrEmpty(oldValue)) {
                batch.delete(cf, IndexKeys.unique(v, pk));
            }
        }
    }

    private static Set<ByteArrayKey> toSet(Iterable<byte[]> keys) {
        HashSet<ByteArrayKey> s = new HashSet<>();
        for (byte[] k : keys) s.add(new ByteArrayKey(k));
        return s;
    }
}
