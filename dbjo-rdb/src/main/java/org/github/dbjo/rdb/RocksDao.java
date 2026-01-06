package org.github.dbjo.rdb;

import org.rocksdb.RocksDBException;

import java.util.*;

public class RocksDao<T, K> extends AbstractRocksDao<T, K> {

    private final EntityDef<T, K> def;

    public RocksDao(RocksSessions sessions, EntityDef<T, K> def) {
        super(sessions,
                def.primaryCf(),
                def.keyCodec(),
                def.valueCodec(),
                // still pass name->CF for query planning if you want:
                def.indexes().stream().collect(java.util.stream.Collectors.toUnmodifiableMap(IndexDef::name, IndexDef::cf))
        );
        this.def = def;
    }

    @Override
    protected void maintainIndexes(RocksWriteBatch batch, K key, T oldValueOrNull, T newValue) throws RocksDBException {
        byte[] pkBytes = keyCodec.encodeKey(key);

        // compute old/new index entry keys per index
        for (IndexDef<T> idx : def.indexes()) {
            Set<ByteArrayWrapper> oldKeys = (oldValueOrNull == null)
                    ? Set.of()
                    : toIndexEntryKeys(idx, oldValueOrNull, pkBytes);

            Set<ByteArrayWrapper> newKeys = toIndexEntryKeys(idx, newValue, pkBytes);

            // delete old \ new
            for (var kOld : oldKeys) {
                if (!newKeys.contains(kOld)) batch.delete(idx.cf(), kOld.bytes());
            }
            // put new \ old
            for (var kNew : newKeys) {
                if (!oldKeys.contains(kNew)) batch.put(idx.cf(), kNew.bytes(), new byte[0]);
            }
        }
    }

    @Override
    protected void maintainIndexesOnDelete(RocksWriteBatch batch, K key, T oldValue) throws RocksDBException {
        byte[] pkBytes = keyCodec.encodeKey(key);
        for (IndexDef<T> idx : def.indexes()) {
            for (var k : toIndexEntryKeys(idx, oldValue, pkBytes)) {
                batch.delete(idx.cf(), k.bytes());
            }
        }
    }

    private Set<ByteArrayWrapper> toIndexEntryKeys(IndexDef<T> idx, T entity, byte[] pkBytes) {
        Set<ByteArrayWrapper> out = new HashSet<>();
        for (byte[] valueBytes : idx.valueBytesExtractor().apply(entity)) {
            if (valueBytes == null) continue;
            out.add(new ByteArrayWrapper(ByteArrays.concat(valueBytes, (byte) 0x00, pkBytes)));
        }
        return out;
    }

    /** tiny helper for hashing byte[] keys */
    private record ByteArrayWrapper(byte[] bytes) {
        @Override public boolean equals(Object o) {
            return (o instanceof ByteArrayWrapper w) && Arrays.equals(bytes, w.bytes);
        }
        @Override public int hashCode() { return Arrays.hashCode(bytes); }
    }
}
