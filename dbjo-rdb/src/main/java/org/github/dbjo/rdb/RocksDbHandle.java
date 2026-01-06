package org.github.dbjo.rdb;

import org.rocksdb.*;

import java.util.*;

public final class RocksDbHandle implements AutoCloseable {
    private final TransactionDB db;
    private final DBOptions dbOptions;
    private final TransactionDBOptions txOptions;
    private final ColumnFamilyOptions cfOptions;

    private final List<ColumnFamilyHandle> handles;
    private final Map<String, ColumnFamilyHandle> cfByName;

    public RocksDbHandle(TransactionDB db,
                         DBOptions dbOptions,
                         TransactionDBOptions txOptions,
                         ColumnFamilyOptions cfOptions,
                         List<ColumnFamilyHandle> handles,
                         Map<String, ColumnFamilyHandle> cfByName) {
        this.db = db;
        this.dbOptions = dbOptions;
        this.txOptions = txOptions;
        this.cfOptions = cfOptions;
        this.handles = handles;
        this.cfByName = cfByName;
    }

    public TransactionDB db() {
        return db;
    }

    public Map<String, ColumnFamilyHandle> cfByName() {
        return cfByName;
    }

    public ColumnFamilyHandle cf(String name) {
        ColumnFamilyHandle h = cfByName.get(name);
        if (h == null) throw new IllegalArgumentException("Unknown CF: " + name);
        return h;
    }

    @Override
    public void close() {
        // Close CF handles first, then DB, then options.
        for (ColumnFamilyHandle h : handles) {
            if (h != null) h.close();
        }
        db.close();
        txOptions.close();
        dbOptions.close();
        cfOptions.close();
    }
}
