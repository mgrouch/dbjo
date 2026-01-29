package org.github.dbjo.rdb.jdbc.catalog;

import org.github.dbjo.rdb.IndexKeys;
import org.github.dbjo.rdb.jdbc.rowset.SimpleRowSetMetaData;
import org.rocksdb.*;

import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetProvider;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.*;
import java.util.Base64;

/**
 * Executes a parsed+planned SELECT/COUNT using:
 *  - Full scan on primary CF
 *  - Index scans on index CFs (EQ/IN/RANGE)
 *
 * Always applies full WHERE evaluation on decoded rows.
 */
public final class RocksJdbcExecutor {
    private RocksJdbcExecutor() {}

    public static CachedRowSet execute(
            RocksDB db,
            Map<String, ColumnFamilyHandle> cfsByName,
            RocksJdbcCatalog catalog,
            String sql,
            int statementMaxRows
    ) throws SQLException {
        RocksJdbcSqlParser.Parsed p = RocksJdbcSqlParser.parse(sql);
        RocksJdbcTable table = catalog.requireTable(p.tableName());
        ColumnFamilyHandle primaryCf = requireCf(cfsByName, table.cfName());

        RocksJdbcWhereParser.Expr where = (p.whereSql() == null) ? new RocksJdbcWhereParser.True()
                : RocksJdbcWhereParser.parse(p.whereSql());

        RocksJdbcPlanner.Access access = RocksJdbcPlanner.plan(table, where);

        if (p.countStar()) {
            long n = count(db, cfsByName, table, primaryCf, where, access);
            return singleLong("COUNT", n);
        }

        List<RocksJdbcColumn> selected = selectColumns(table, p.selectColumns());
        int limit = limit(p.limit(), statementMaxRows);

        CachedRowSet rs = RowSetProvider.newFactory().createCachedRowSet();
        rs.setMetaData(buildMeta(selected));

        RowAccessor acc = new RowAccessor(table.rowClass(), table.columns());

        int out = 0;
        for (RowEntry e : scan(db, cfsByName, table, primaryCf, access)) {
            if (out >= limit) break;

            Object bean = decode(table, e.valueBytes());
            if (!where.eval(colName -> acc.get(bean, colName))) continue;

            rs.moveToInsertRow();
            for (int i = 0; i < selected.size(); i++) {
                RocksJdbcColumn c = selected.get(i);
                Object v = acc.getByGetter(bean, c.getterName());
                rs.updateObject(i + 1, v);
            }
            rs.insertRow();
            rs.moveToCurrentRow();
            out++;
        }

        rs.beforeFirst();
        return rs;
    }

    // --- core scan/candidate iteration ---

    private record RowEntry(byte[] pkBytes, byte[] valueBytes) {}

    private static Iterable<RowEntry> scan(
            RocksDB db,
            Map<String, ColumnFamilyHandle> cfsByName,
            RocksJdbcTable table,
            ColumnFamilyHandle primaryCf,
            RocksJdbcPlanner.Access access
    ) throws SQLException {
        if (access instanceof RocksJdbcPlanner.FullScan) {
            return () -> new Iterator<>() {
                final RocksIterator it = db.newIterator(primaryCf);
                { it.seekToFirst(); }
                @Override public boolean hasNext() { return it.isValid(); }
                @Override public RowEntry next() {
                    if (!it.isValid()) throw new NoSuchElementException();
                    byte[] k = it.key();
                    byte[] v = it.value();
                    it.next();
                    return new RowEntry(k, v);
                }
            };
        }

        if (access instanceof RocksJdbcPlanner.IndexEq eq) {
            ColumnFamilyHandle idxCf = requireCf(cfsByName, eq.indexName());
            byte[] prefix = IndexKeys.prefix(eq.valueBytesRaw());

            return () -> new Iterator<>() {
                final RocksIterator it = db.newIterator(idxCf);
                boolean init = false;

                private void ensureInit() {
                    if (init) return;
                    init = true;
                    it.seek(prefix);
                }

                @Override public boolean hasNext() {
                    ensureInit();
                    if (!it.isValid()) return false;
                    return IndexKeys.startsWith(it.key(), prefix);
                }

                @Override public RowEntry next() {
                    if (!hasNext()) throw new NoSuchElementException();
                    byte[] idxKey = it.key();
                    byte[] pk = IndexKeys.pkFromIndexKey(idxKey);
                    it.next();
                    try {
                        byte[] v = db.get(primaryCf, pk);
                        return (v == null) ? null : new RowEntry(pk, v);
                    } catch (RocksDBException ex) {
                        throw new RuntimeException(ex);
                    }
                }
            };
        }

        if (access instanceof RocksJdbcPlanner.IndexIn in) {
            ColumnFamilyHandle idxCf = requireCf(cfsByName, in.indexName());
            List<byte[]> values = in.valuesBytesRaw();

            return () -> new Iterator<>() {
                int vi = 0;
                RocksIterator it = null;
                byte[] prefix = null;
                final HashSet<BytesKey> seen = new HashSet<>();
                RowEntry next = null;

                @Override public boolean hasNext() {
                    if (next != null) return true;

                    while (true) {
                        if (it == null) {
                            if (vi >= values.size()) return false;
                            prefix = IndexKeys.prefix(values.get(vi++));
                            it = db.newIterator(idxCf);
                            it.seek(prefix);
                        }

                        while (it.isValid() && IndexKeys.startsWith(it.key(), prefix)) {
                            byte[] pk = IndexKeys.pkFromIndexKey(it.key());
                            it.next();

                            BytesKey bk = new BytesKey(pk);
                            if (!seen.add(bk)) continue;

                            try {
                                byte[] v = db.get(primaryCf, pk);
                                if (v == null) continue;
                                next = new RowEntry(pk, v);
                                return true;
                            } catch (RocksDBException ex) {
                                throw new RuntimeException(ex);
                            }
                        }

                        it.close();
                        it = null;
                        prefix = null;
                    }
                }

                @Override public RowEntry next() {
                    if (!hasNext()) throw new NoSuchElementException();
                    RowEntry r = next;
                    next = null;
                    return r;
                }
            };
        }

        if (access instanceof RocksJdbcPlanner.IndexRange r) {
            ColumnFamilyHandle idxCf = requireCf(cfsByName, r.indexName());

            final byte[] fromPrefix = (r.fromBytesRaw() == null) ? null : IndexKeys.prefix(r.fromBytesRaw());
            final byte[] toPrefix   = (r.toBytesRaw() == null) ? null : IndexKeys.prefix(r.toBytesRaw());

            return () -> new Iterator<>() {
                final RocksIterator it = db.newIterator(idxCf);
                boolean init = false;
                RowEntry next = null;

                private void ensureInit() {
                    if (init) return;
                    init = true;
                    if (fromPrefix == null) it.seekToFirst();
                    else it.seek(fromPrefix);
                }

                @Override public boolean hasNext() {
                    if (next != null) return true;
                    ensureInit();

                    while (it.isValid()) {
                        byte[] k = it.key();
                        byte[] vprefix = IndexKeys.escapedValuePart(k);

                        // lower bound handling: if exclusive and value == from -> skip
                        if (fromPrefix != null && !r.fromInclusive()) {
                            if (Arrays.equals(vprefix, fromPrefix)) {
                                it.next();
                                continue;
                            }
                        }

                        // upper bound handling
                        if (toPrefix != null) {
                            int cmp = lexCompare(vprefix, toPrefix);
                            if (cmp > 0) return false;
                            if (cmp == 0 && !r.toInclusive()) {
                                // value == to and exclusive -> stop entirely
                                return false;
                            }
                        }

                        byte[] pk = IndexKeys.pkFromIndexKey(k);
                        it.next();
                        try {
                            byte[] pv = db.get(requireCf(cfsByName, table.cfName()), pk);
                            if (pv == null) continue;
                            next = new RowEntry(pk, pv);
                            return true;
                        } catch (RocksDBException | SQLException ex) {
                            throw new RuntimeException(ex);
                        }
                    }

                    return false;
                }

                @Override public RowEntry next() {
                    if (!hasNext()) throw new NoSuchElementException();
                    RowEntry r = next;
                    next = null;
                    return r;
                }
            };
        }

        throw new SQLException("Unknown access: " + access.getClass());
    }

    private static int lexCompare(byte[] a, byte[] b) {
        int n = Math.min(a.length, b.length);
        for (int i = 0; i < n; i++) {
            int da = a[i] & 0xFF;
            int db = b[i] & 0xFF;
            if (da != db) return Integer.compare(da, db);
        }
        return Integer.compare(a.length, b.length);
    }

    // --- count ---

    private static long count(
            RocksDB db,
            Map<String, ColumnFamilyHandle> cfsByName,
            RocksJdbcTable table,
            ColumnFamilyHandle primaryCf,
            RocksJdbcWhereParser.Expr where,
            RocksJdbcPlanner.Access access
    ) throws SQLException {
        RowAccessor acc = new RowAccessor(table.rowClass(), table.columns());
        long n = 0;
        for (RowEntry e : scan(db, cfsByName, table, primaryCf, access)) {
            if (e == null) continue;
            Object bean = decode(table, e.valueBytes());
            if (where.eval(colName -> acc.get(bean, colName))) n++;
        }
        return n;
    }

    // --- helpers ---

    private static ColumnFamilyHandle requireCf(Map<String, ColumnFamilyHandle> cfsByName, String name) throws SQLException {
        ColumnFamilyHandle cf = cfsByName.get(name);
        if (cf == null) throw new SQLException("Missing column family: " + name);
        return cf;
    }

    private static Object decode(RocksJdbcTable table, byte[] valueBytes) throws SQLException {
        return table.decoder().decode(valueBytes);
    }

    private static List<RocksJdbcColumn> selectColumns(RocksJdbcTable table, List<String> cols) throws SQLException {
        if (cols == null || cols.isEmpty()) return List.of(table.columns());

        Map<String, RocksJdbcColumn> byLower = new HashMap<>();
        for (RocksJdbcColumn c : table.columns()) {
            byLower.put(c.name().toLowerCase(Locale.ROOT), c);
        }

        ArrayList<RocksJdbcColumn> out = new ArrayList<>();
        for (String n : cols) {
            RocksJdbcColumn c = byLower.get(n.trim().toLowerCase(Locale.ROOT));
            if (c == null) throw new SQLException("Unknown column: " + n);
            out.add(c);
        }
        return out;
    }

    private static int limit(Integer sqlLimit, int stmtMaxRows) {
        int lim = (sqlLimit == null) ? Integer.MAX_VALUE : Math.max(0, sqlLimit);
        if (stmtMaxRows > 0) lim = Math.min(lim, stmtMaxRows);
        return lim;
    }

    private static SimpleRowSetMetaData buildMeta(List<RocksJdbcColumn> cols) throws SQLException {
        SimpleRowSetMetaData md = new SimpleRowSetMetaData(cols.size());
        for (int i = 0; i < cols.size(); i++) {
            RocksJdbcColumn c = cols.get(i);
            int col = i + 1;
            md.setColumnName(col, c.name());
            md.setColumnLabel(col, c.name());
            md.setColumnType(col, c.sqlType());
            md.setColumnTypeName(col, c.typeName());
            md.setColumnDisplaySize(col, c.size());
            md.setScale(col, c.scale());
            md.setNullable(col, c.nullable() ? java.sql.DatabaseMetaData.columnNullable : DatabaseMetaData.columnNoNulls);
        }
        return md;
    }

    private static CachedRowSet singleLong(String name, long v) throws SQLException {
        CachedRowSet rs = RowSetProvider.newFactory().createCachedRowSet();
        SimpleRowSetMetaData md = new SimpleRowSetMetaData(1);
        md.setColumnName(1, name);
        md.setColumnLabel(1, name);
        md.setColumnType(1, java.sql.Types.BIGINT);
        md.setColumnTypeName(1, "BIGINT");
        rs.setMetaData(md);

        rs.moveToInsertRow();
        rs.updateLong(1, v);
        rs.insertRow();
        rs.moveToCurrentRow();
        rs.beforeFirst();
        return rs;
    }

    // --- row accessor (reflection cached) ---

    static final class RowAccessor {
        private final Map<String, String> getterByColLower = new HashMap<>();
        private final Map<String, Method> getterCache = new HashMap<>();
        private final Class<?> rowClass;

        RowAccessor(Class<?> rowClass, RocksJdbcColumn[] cols) throws SQLException {
            this.rowClass = rowClass;
            for (RocksJdbcColumn c : cols) {
                getterByColLower.put(c.name().toLowerCase(Locale.ROOT), c.getterName());
            }
        }

        Object get(Object bean, String col) throws SQLException {
            if (bean == null) return null;
            if (col == null) return null;
            String g = getterByColLower.get(col.trim().toLowerCase(Locale.ROOT));
            if (g == null) return null;
            return getByGetter(bean, g);
        }

        Object getByGetter(Object bean, String getterName) throws SQLException {
            if (bean == null) return null;
            if (getterName == null) return null;

            Method m = getterCache.get(getterName);
            if (m == null) {
                try {
                    m = rowClass.getMethod(getterName);
                    m.setAccessible(true);
                    getterCache.put(getterName, m);
                } catch (Exception e) {
                    throw new SQLException("Missing getter " + getterName + " on " + rowClass.getName(), e);
                }
            }

            try {
                return m.invoke(bean);
            } catch (Exception e) {
                throw new SQLException("Getter failed: " + getterName, e);
            }
        }
    }

    static final class BytesKey {
        final byte[] b;
        final int h;
        BytesKey(byte[] b) {
            this.b = (b == null) ? new byte[0] : b.clone();
            this.h = Arrays.hashCode(this.b);
        }
        @Override public boolean equals(Object o) { return (o instanceof BytesKey k) && Arrays.equals(b, k.b); }
        @Override public int hashCode() { return h; }
    }
}
