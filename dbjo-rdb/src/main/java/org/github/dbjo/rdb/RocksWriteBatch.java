package org.github.dbjo.rdb;

import org.rocksdb.ColumnFamilyHandle;

import java.util.ArrayList;
import java.util.List;

public final class RocksWriteBatch {
    public sealed interface Op permits Put, Del { }

    public record Put(ColumnFamilyHandle cf, byte[] key, byte[] value) implements Op { }
    public record Del(ColumnFamilyHandle cf, byte[] key) implements Op { }

    private final List<Op> ops = new ArrayList<>();

    public void put(ColumnFamilyHandle cf, byte[] key, byte[] value) {
        ops.add(new Put(cf, key, value));
    }

    public void delete(ColumnFamilyHandle cf, byte[] key) {
        ops.add(new Del(cf, key));
    }

    public List<Op> ops() { return ops; }
    public boolean isEmpty() { return ops.isEmpty(); }
}
