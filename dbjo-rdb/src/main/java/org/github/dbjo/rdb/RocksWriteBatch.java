package org.github.dbjo.rdb;

import org.rocksdb.ColumnFamilyHandle;

import java.util.*;

public final class RocksWriteBatch {
    public sealed interface Op permits Put, Delete {}
    public record Put(ColumnFamilyHandle cf, byte[] key, byte[] value) implements Op {}
    public record Delete(ColumnFamilyHandle cf, byte[] key) implements Op {}

    private final List<Op> ops = new ArrayList<>();

    public void put(ColumnFamilyHandle cf, byte[] key, byte[] value) { ops.add(new Put(cf, key, value)); }
    public void delete(ColumnFamilyHandle cf, byte[] key) { ops.add(new Delete(cf, key)); }

    public List<Op> ops() { return List.copyOf(ops); }
    public boolean isEmpty() { return ops.isEmpty(); }
}
