package org.github.dbjo.rdb;

import org.rocksdb.*;

import java.util.List;
import java.util.Map;

public final class RocksDbHandle implements AutoCloseable {
    private final TransactionDB db;
    private final DBOptions dbOptions;
    private final TransactionDBOptions txOptions;
    private final List<ColumnFamilyHandle> cfHandles;
    private final Map<String, ColumnFamilyHandle> cfByName;

    public RocksDbHandle(TransactionDB db,
                         DBOptions dbOptions,
                         TransactionDBOptions txOptions,
                         List<ColumnFamilyHandle> cfHandles,
                         Map<String, ColumnFamilyHandle> cfByName) {
        this.db = db;
        this.dbOptions = dbOptions;
        this.txOptions = txOptions;
        this.cfHandles = cfHandles;
        this.cfByName = cfByName;
    }

    public TransactionDB db() { return db; }
    public ColumnFamilyHandle cf(String name) { return cfByName.get(name); }
    public Map<String, ColumnFamilyHandle> cfByName() { return cfByName; }

    @Override public void close() {
        // Close CF handles first, then DB/options.
        for (var h : cfHandles) h.close();
        db.close();
        txOptions.close();
        dbOptions.close();
    }
}
