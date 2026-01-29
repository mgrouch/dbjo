package org.github.dbjo.rdb.jdbc;

import org.github.dbjo.rdb.IndexKeys;
import org.github.dbjo.rdb.jdbc.catalog.*;
import org.rocksdb.*;

import javax.sql.rowset.CachedRowSet;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.*;

/**
 * Opens RocksDB with the CFs described by the generated RocksJdbcCatalog
 * and provides:
 *  - SELECT execution via RocksJdbcExecutor
 *  - optional index rebuild (safe for your case: no persisted data yet)
 */
public final class RocksJdbcEngine implements AutoCloseable {
    private final RocksJdbcCatalog catalog;

    private final RocksDB db;
    private final DBOptions dbOptions;
    private final List<ColumnFamilyHandle> handles;
    private final Map<String, ColumnFamilyHandle> cfsByName;

    public RocksJdbcEngine(RocksJdbcCatalog catalog, String dbPath, boolean rebuildIndexes) throws SQLException {
        this.catalog = Objects.requireNonNull(catalog, "catalog");

        try {
            RocksDB.loadLibrary();
        } catch (Throwable ignore) {
            // ok if already loaded
        }

        try {
            this.dbOptions = new DBOptions()
                    .setCreateIfMissing(true)
                    .setCreateMissingColumnFamilies(true);

            List<ColumnFamilyDescriptor> desc = new ArrayList<>();
            // Default CF must always exist
            desc.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, new ColumnFamilyOptions()));

            // collect CF names
            LinkedHashSet<String> cfNames = new LinkedHashSet<>();
            for (RocksJdbcTable t : catalog.tables()) {
                cfNames.add(t.cfName());
                for (RocksJdbcIndex ix : t.indexes()) {
                    if (ix != null && ix.indexName() != null) cfNames.add(ix.indexName());
                }
            }
            for (String n : cfNames) {
                desc.add(new ColumnFamilyDescriptor(n.getBytes(StandardCharsets.UTF_8), new ColumnFamilyOptions()));
            }

            this.handles = new ArrayList<>();
            this.db = RocksDB.open(dbOptions, dbPath, desc, handles);

            HashMap<String, ColumnFamilyHandle> map = new HashMap<>();
            // default handle is handles[0] but we don't address by name
            for (int i = 1; i < desc.size(); i++) {
                String n = new String(desc.get(i).getName(), StandardCharsets.UTF_8);
                map.put(n, handles.get(i));
            }
            this.cfsByName = Collections.unmodifiableMap(map);

            if (rebuildIndexes) {
                rebuildAllIndexes();
            }
        } catch (RocksDBException ex) {
            throw new SQLException("Failed to open RocksDB: " + dbPath, ex);
        }
    }

    public RocksJdbcCatalog catalog() { return catalog; }

    public CachedRowSet query(String sql, int statementMaxRows) throws SQLException {
        return RocksJdbcExecutor.execute(db, cfsByName, catalog, sql, statementMaxRows);
    }

    public Map<String, ColumnFamilyHandle> cfsByName() { return cfsByName; }

    public RocksDB db() { return db; }

    /**
     * Rebuild all index CFs from primary CFs using the SAME key materialization
     * as your generated schema (idxKey/idxPart semantics).
     */
    public void rebuildAllIndexes() throws SQLException {
        for (RocksJdbcTable t : catalog.tables()) {
            rebuildIndexesForTable(t);
        }
    }

    private void rebuildIndexesForTable(RocksJdbcTable t) throws SQLException {
        ColumnFamilyHandle primary = requireCf(t.cfName());

        RocksJdbcIndex[] indexes = t.indexes();
        if (indexes == null || indexes.length == 0) return;

        // map columnName->getterName
        Map<String, String> getterByColUpper = new HashMap<>();
        for (RocksJdbcColumn c : t.columns()) {
            getterByColUpper.put(c.name().toUpperCase(Locale.ROOT), c.getterName());
        }

        // clear each index CF
        for (RocksJdbcIndex ix : indexes) {
            if (ix == null) continue;
            ColumnFamilyHandle idxCf = requireCf(ix.indexName());
            clearCf(idxCf);
        }

        // scan primary, repopulate
        RocksIterator it = db.newIterator(primary);

        try (it; WriteBatch wb = new WriteBatch();
             WriteOptions wo = new WriteOptions().setDisableWAL(true)) {
            it.seekToFirst();

            int batchOps = 0;
            final byte[] EMPTY = new byte[0];

            // reflection cache for getters
            Map<String, Method> getterCache = new HashMap<>();

            while (it.isValid()) {
                byte[] pk = it.key();
                byte[] val = it.value();

                Object bean = t.decoder().decode(val);

                for (RocksJdbcIndex ix : indexes) {
                    if (ix == null) continue;

                    String idxName = ix.indexName();
                    ColumnFamilyHandle idxCf = requireCf(idxName);

                    String[] cols = ix.columnNames();
                    Object[] parts = new Object[cols.length];
                    boolean anyNull = false;

                    for (int i = 0; i < cols.length; i++) {
                        String cn = cols[i];
                        String g = getterByColUpper.get(cn.toUpperCase(Locale.ROOT));
                        Object v = invokeGetter(bean, t.rowClass(), g, getterCache);
                        parts[i] = v;
                        if (v == null) anyNull = true;
                    }

                    if (anyNull) continue;

                    String keyStr = idxKey(parts);
                    if (keyStr == null) continue;
                    byte[] vbytes = keyStr.getBytes(StandardCharsets.UTF_8);

                    byte[] ukey = IndexKeys.unique(vbytes, pk);
                    wb.put(idxCf, ukey, EMPTY);
                    batchOps++;

                    if (batchOps >= 10_000) {
                        db.write(wo, wb);
                        wb.clear();
                        batchOps = 0;
                    }
                }

                it.next();
            }

            if (batchOps > 0) {
                db.write(wo, wb);
                wb.clear();
            }
        } catch (Exception e) {
            if (e instanceof SQLException se) throw se;
            throw new SQLException("Index rebuild failed for " + t.tableName(), e);
        }
    }

    private void clearCf(ColumnFamilyHandle cf) throws SQLException {
        try (WriteBatch wb = new WriteBatch();
             WriteOptions wo = new WriteOptions().setDisableWAL(true)) {

            RocksIterator it = db.newIterator(cf);
            it.seekToFirst();

            int ops = 0;
            while (it.isValid()) {
                wb.delete(cf, it.key());
                ops++;
                if (ops >= 20_000) {
                    db.write(wo, wb);
                    wb.clear();
                    ops = 0;
                }
                it.next();
            }

            if (ops > 0) {
                db.write(wo, wb);
                wb.clear();
            }

            it.close();
        } catch (RocksDBException ex) {
            throw new SQLException("Failed to clear CF", ex);
        }
    }

    private ColumnFamilyHandle requireCf(String name) throws SQLException {
        ColumnFamilyHandle cf = cfsByName.get(name);
        if (cf == null) throw new SQLException("Missing CF: " + name);
        return cf;
    }

    private static Object invokeGetter(Object bean, Class<?> rowClass, String getter, Map<String, Method> cache) throws SQLException {
        if (bean == null || getter == null) return null;
        Method m = cache.get(getter);
        if (m == null) {
            try {
                m = rowClass.getMethod(getter);
                m.setAccessible(true);
                cache.put(getter, m);
            } catch (Exception e) {
                throw new SQLException("Missing getter " + getter + " on " + rowClass.getName(), e);
            }
        }
        try {
            return m.invoke(bean);
        } catch (Exception e) {
            throw new SQLException("Getter failed: " + getter, e);
        }
    }

    // Must match RocksSchemaGenerator semantics
    private static String idxKey(Object... parts) {
        if (parts == null || parts.length == 0) return null;
        for (Object p : parts) if (p == null) return null;
        if (parts.length == 1) return idxPart(parts[0]);

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < parts.length; i++) {
            if (i > 0) sb.append('\u0000');
            sb.append(idxPart(parts[i]));
        }
        return sb.toString();
    }

    private static String idxPart(Object v) {
        if (v == null) return null;
        if (v instanceof byte[] b) return Base64.getEncoder().encodeToString(b);
        if (v instanceof java.math.BigDecimal bd) return bd.toPlainString();
        return v.toString();
    }

    @Override
    public void close() {
        // close handles, db, options
        try {
            for (ColumnFamilyHandle h : handles) {
                try { h.close(); } catch (Throwable ignore) {}
            }
        } finally {
            try { db.close(); } catch (Throwable ignore) {}
            try { dbOptions.close(); } catch (Throwable ignore) {}
        }
    }
}
