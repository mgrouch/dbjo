package org.github.dbjo.rdb;

import org.rocksdb.ColumnFamilyHandle;

import java.util.Map;
import java.util.Objects;

public final class DaoRegistry {
    private final Map<String, ColumnFamilyHandle> cfByName;
    public DaoRegistry(Map<String, ColumnFamilyHandle> cfByName) { this.cfByName = Map.copyOf(cfByName); }
    public ColumnFamilyHandle cf(String name) { return Objects.requireNonNull(cfByName.get(name), "Unknown CF: " + name); }
}