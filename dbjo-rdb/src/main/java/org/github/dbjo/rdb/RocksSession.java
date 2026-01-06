package org.github.dbjo.rdb;

import org.rocksdb.*;

public interface RocksSession {

    ReadOptions newReadOptions(); // caller (or helper) closes

    byte[] get(ColumnFamilyHandle cf, ReadOptions ro, byte[] key) throws RocksDBException;

    RocksIterator iterator(ColumnFamilyHandle cf, ReadOptions ro);

    void write(RocksWriteBatch batch) throws RocksDBException;

    // ---- Convenience: common get without having to manage ReadOptions everywhere ----
    default byte[] get(ColumnFamilyHandle cf, byte[] key) throws RocksDBException {
        try (ReadOptions ro = newReadOptions()) {
            return get(cf, ro, key);
        }
    }

    // ---- Iterator handle that closes BOTH iterator and ReadOptions ----
    default IteratorHandle openIterator(ColumnFamilyHandle cf) {
        ReadOptions ro = newReadOptions();
        RocksIterator it = iterator(cf, ro);
        return new IteratorHandle(it, ro);
    }

    record IteratorHandle(RocksIterator it, ReadOptions ro) implements AutoCloseable {
        @Override public void close() {
            // Order doesn’t matter much, but close iterator first.
            it.close();
            ro.close();
        }
    }
}
