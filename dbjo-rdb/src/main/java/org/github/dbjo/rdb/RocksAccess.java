package org.github.dbjo.rdb;

import org.rocksdb.*;

public interface RocksAccess {
    // If a Spring tx is active, returns the Transaction; otherwise null.
    Transaction currentTxnOrNull();

    // For non-tx ops (or when you want raw access)
    TransactionDB txDb(); // or OptimisticTransactionDB

    ColumnFamilyHandle cf(String name);
}
