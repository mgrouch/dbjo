package org.github.dbjo.rdb;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.TransactionDB;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public final class DaoRegistry implements AutoCloseable {
    private final TransactionDB db; // optional, but handy
    private final Map<String, ColumnFamilyHandle> cfByName;
    private final boolean closeDbOnClose;

    public DaoRegistry(TransactionDB db, Map<String, ColumnFamilyHandle> cfByName) {
        this(db, cfByName, false);
    }

    /** If you want registry.close() to also close the DB, pass closeDbOnClose=true. */
    public DaoRegistry(TransactionDB db, Map<String, ColumnFamilyHandle> cfByName, boolean closeDbOnClose) {
        this.db = Objects.requireNonNull(db, "db");
        this.cfByName = Map.copyOf(Objects.requireNonNull(cfByName, "cfByName"));
        this.closeDbOnClose = closeDbOnClose;
    }

    public TransactionDB db() { return db; }

    public Map<String, ColumnFamilyHandle> cfByName() { return cfByName; }

    public boolean hasCf(String name) { return cfByName.containsKey(name); }

    public ColumnFamilyHandle cf(String name) {
        ColumnFamilyHandle h = cfByName.get(name);
        if (h == null) throw new IllegalArgumentException("Unknown CF: " + name);
        return h;
    }

    /**
     * Resolve index CFs by convention: index CF name == indexDef.name().
     */
    public <T, K> ResolvedEntityDef<T, K> entity(EntityDef<T, K> def) {
        Objects.requireNonNull(def, "def");
        Map<String, ColumnFamilyHandle> idx = new HashMap<>();
        for (IndexDef<T> i : def.indexes()) {
            idx.put(i.name(), cf(i.name()));
        }
        return new ResolvedEntityDef<>(def, idx);
    }

    /**
     * Resolve index CFs when CF names differ from index names.
     * Map: indexName -> cfName
     */
    public <T, K> ResolvedEntityDef<T, K> entity(EntityDef<T, K> def, Map<String, String> indexNameToCfName) {
        Objects.requireNonNull(def, "def");
        Objects.requireNonNull(indexNameToCfName, "indexNameToCfName");

        Map<String, ColumnFamilyHandle> idx = new HashMap<>();
        for (IndexDef<T> i : def.indexes()) {
            String idxName = i.name();
            String cfName = indexNameToCfName.getOrDefault(idxName, idxName);
            idx.put(idxName, cf(cfName));
        }
        return new ResolvedEntityDef<>(def, idx);
    }

    @Override
    public void close() {
        // ColumnFamilyHandle lifecycle is typically owned by the RocksDbHandle/DB owner.
        // We only optionally close DB.
        if (closeDbOnClose) {
            db.close();
        }
    }
}
