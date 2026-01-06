package org.github.dbjo.rdb;

import org.rocksdb.*;

public interface RocksSession {
    ReadOptions newReadOptions();                 // caller closes
    byte[] get(ColumnFamilyHandle cf, ReadOptions ro, byte[] key) throws RocksDBException;
    RocksIterator iterator(ColumnFamilyHandle cf, ReadOptions ro); // caller closes iterator
    void write(RocksWriteBatch batch) throws RocksDBException;
}