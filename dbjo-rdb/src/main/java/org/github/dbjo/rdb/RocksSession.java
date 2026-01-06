package org.github.dbjo.rdb;

import org.rocksdb.*;

public interface RocksSession {
    byte[] get(ColumnFamilyHandle cf, byte[] key) throws RocksDBException;
    void write(RocksWriteBatch batch) throws RocksDBException;
    RocksIterator newIterator(ColumnFamilyHandle cf) throws RocksDBException;
}
