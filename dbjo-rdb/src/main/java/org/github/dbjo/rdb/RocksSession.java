package org.github.dbjo.rdb;

import org.rocksdb.*;

public interface RocksSession {
    byte[] get(ColumnFamilyHandle cf, byte[] key) throws RocksDBException;
    void put(ColumnFamilyHandle cf, byte[] key, byte[] value) throws RocksDBException;
    void delete(ColumnFamilyHandle cf, byte[] key) throws RocksDBException;

    RocksIterator newIterator(ColumnFamilyHandle cf) throws RocksDBException;

    /** Atomically apply multiple ops (atomic via WriteBatch or via the ambient transaction). */
    void write(RocksWriteBatch batch) throws RocksDBException;
}
