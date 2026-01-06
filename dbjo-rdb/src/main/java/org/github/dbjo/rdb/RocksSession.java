package org.github.dbjo.rdb;

import org.rocksdb.*;

public interface RocksSession {
    ReadOptions newReadOptions();

    byte[] get(ColumnFamilyHandle cf, ReadOptions ro, byte[] key) throws RocksDBException;

    RocksIterator iterator(ColumnFamilyHandle cf, ReadOptions ro);

    void write(RocksWriteBatch batch) throws RocksDBException;

    // Convenience for simple point-get
    default byte[] get(ColumnFamilyHandle cf, byte[] key) throws RocksDBException {
        try (ReadOptions ro = newReadOptions()) {
            return get(cf, ro, key);
        }
    }

    // Iterator handle that closes BOTH iterator and RO
    default IteratorHandle openIterator(ColumnFamilyHandle cf) {
        ReadOptions ro = newReadOptions();
        RocksIterator it = iterator(cf, ro);
        return new IteratorHandle(it, ro);
    }

    record IteratorHandle(RocksIterator it, ReadOptions ro) implements AutoCloseable {
        @Override public void close() {
            it.close();
            ro.close();
        }
    }
}
