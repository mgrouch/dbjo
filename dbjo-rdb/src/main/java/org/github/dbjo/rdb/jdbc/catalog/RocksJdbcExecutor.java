package org.github.dbjo.rdb.jdbc.catalog;

import org.github.dbjo.criteria.Condition;
import org.github.dbjo.criteria.eval.ConditionEvaluator;
import org.github.dbjo.rdb.IndexKeys;
import org.github.dbjo.rdb.IndexPredicate;
import org.github.dbjo.rdb.criteria.CriteriaIndexPlanner;
import org.rocksdb.*;

import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetProvider;
import javax.sql.rowset.RowSetMetaDataImpl;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.*;

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
        if (isTablesQuery(p)) {
            return listTables(catalog, limit(p.limit(), statementMaxRows), offset(p.offset()));
        }
        RocksJdbcTable table = catalog.requireTable(p.tableName());
        ColumnFamilyHandle primaryCf = requireCf(cfsByName, table.cfName());
        QueryPlan plan = planQuery(table, p, statementMaxRows);

        CachedRowSet rs = RowSetProvider.newFactory().createCachedRowSet();
        RowAccessor acc = new RowAccessor(table.rowClass(), table.columns());

        rs.setMetaData(plan.meta());

        if (plan.aggregateQuery()) {
            runAggregateQuery(
                    rs,
                    db,
                    cfsByName,
                    table,
                    primaryCf,
                    acc,
                    plan.criteria(),
                    plan.legacyWhere(),
                    plan.access(),
                    plan.parsed(),
                    plan.havingExpr(),
                    plan.limit(),
                    plan.offset()
            );
        } else {
            runRowQuery(
                    rs,
                    db,
                    cfsByName,
                    table,
                    primaryCf,
                    acc,
                    plan.criteria(),
                    plan.legacyWhere(),
                    plan.access(),
                    plan.selectedColumns(),
                    plan.parsed().orderBy(),
                    plan.limit(),
                    plan.offset()
            );
        }

        rs.beforeFirst();
        return rs;
    }

    private static boolean isTablesQuery(RocksJdbcSqlParser.Parsed parsed) {
        if (!"tables".equalsIgnoreCase(parsed.tableName())) {
            return false;
        }
        if (!parsed.selectAll() || !parsed.selectItems().isEmpty()) {
            return false;
        }
        return parsed.whereSql() == null
                && parsed.havingSql() == null
                && parsed.groupByColumns().isEmpty()
                && parsed.orderBy().isEmpty();
    }

    private static CachedRowSet listTables(RocksJdbcCatalog catalog, int limit, int offset) throws SQLException {
        CachedRowSet rs = RowSetProvider.newFactory().createCachedRowSet();
        RowSetMetaDataImpl md = new RowSetMetaDataImpl();
        md.setColumnCount(1);
        md.setColumnName(1, "TABLE_NAME");
        md.setColumnLabel(1, "TABLE_NAME");
        md.setColumnType(1, Types.VARCHAR);
        md.setColumnTypeName(1, "VARCHAR");
        md.setColumnDisplaySize(1, 0);
        md.setScale(1, 0);
        md.setNullable(1, DatabaseMetaData.columnNoNulls);
        rs.setMetaData(md);

        int out = 0;
        int skipped = 0;
        for (RocksJdbcTable table : catalog.tables()) {
            if (skipped < offset) {
                skipped++;
                continue;
            }
            if (out >= limit) break;
            rs.moveToInsertRow();
            rs.updateString(1, table.tableName());
            rs.insertRow();
            rs.moveToCurrentRow();
            out++;
        }

        rs.beforeFirst();
        return rs;
    }

    // --- core scan/candidate iteration ---

    private record RowEntry(byte[] pkBytes, byte[] valueBytes) {}

    private interface CloseableIterator<T> extends Iterator<T>, AutoCloseable {
        @Override
        void close();
    }

    private record QueryPlan(
            RocksJdbcSqlParser.Parsed parsed,
            RocksJdbcPlanner.Access access,
            Condition<?> criteria,
            RocksJdbcWhereParser.Expr legacyWhere,
            RocksJdbcWhereParser.Expr havingExpr,
            boolean aggregateQuery,
            List<RocksJdbcColumn> selectedColumns,
            RowSetMetaDataImpl meta,
            int limit,
            int offset
    ) {}

    private static QueryPlan planQuery(
            RocksJdbcTable table,
            RocksJdbcSqlParser.Parsed parsed,
            int statementMaxRows
    ) throws SQLException {
        Map<String, org.github.dbjo.criteria.PropertyTerm<?, ? extends java.io.Serializable>> termsByColumnLower =
                table.termsByColumnLower();
        boolean criteriaEnabled = termsByColumnLower != null && !termsByColumnLower.isEmpty();

        Condition<?> criteria = null;
        RocksJdbcWhereParser.Expr legacyWhere = null;

        if (parsed.whereSql() != null) {
            if (criteriaEnabled) {
                criteria = RocksJdbcWhereCompiler.compile(parsed.whereSql(), (Map) termsByColumnLower);
            } else {
                legacyWhere = RocksJdbcWhereParser.parse(parsed.whereSql());
            }
        } else if (!criteriaEnabled) {
            legacyWhere = new RocksJdbcWhereParser.True();
        }

        RocksJdbcPlanner.Access access = criteriaEnabled
                ? planCriteriaAccess(table, criteria, termsByColumnLower)
                : RocksJdbcPlanner.plan(table, legacyWhere);

        boolean hasAggregates = parsed.selectItems().stream().anyMatch(i -> i instanceof RocksJdbcSqlParser.SelectAgg);
        boolean hasGroupBy = !parsed.groupByColumns().isEmpty();
        boolean aggregateQuery = hasAggregates || hasGroupBy;
        RocksJdbcWhereParser.Expr havingExpr = (parsed.havingSql() == null)
                ? null
                : RocksJdbcWhereParser.parse(normalizeHavingSql(parsed.havingSql()));

        int limit = limit(parsed.limit(), statementMaxRows);
        int offset = offset(parsed.offset());

        List<RocksJdbcColumn> selected = parsed.selectAll()
                ? List.of(table.columns())
                : selectColumns(table, parsed.selectItems());
        RowSetMetaDataImpl meta = aggregateQuery
                ? buildMetaForSelectItems(table, parsed.selectItems(), parsed.selectAll(), parsed.groupByColumns())
                : buildMeta(selected);

        return new QueryPlan(
                parsed,
                access,
                criteria,
                legacyWhere,
                havingExpr,
                aggregateQuery,
                selected,
                meta,
                limit,
                offset
        );
    }

    private static CloseableIterator<RowEntry> scan(
            RocksDB db,
            Map<String, ColumnFamilyHandle> cfsByName,
            RocksJdbcTable table,
            ColumnFamilyHandle primaryCf,
            RocksJdbcPlanner.Access access
    ) throws SQLException {
        if (access instanceof RocksJdbcPlanner.FullScan) {
            return new CloseableIterator<>() {
                final RocksIterator it = db.newIterator(primaryCf);
                { it.seekToFirst(); }
                boolean closed = false;
                @Override public boolean hasNext() {
                    if (closed) return false;
                    return it.isValid();
                }
                @Override public RowEntry next() {
                    if (!it.isValid()) throw new NoSuchElementException();
                    byte[] k = it.key();
                    byte[] v = it.value();
                    it.next();
                    if (!it.isValid()) {
                        close();
                    }
                    return new RowEntry(k, v);
                }
                @Override public void close() {
                    if (closed) return;
                    closed = true;
                    it.close();
                }
            };
        }

        if (access instanceof RocksJdbcPlanner.IndexEq eq) {
            ColumnFamilyHandle idxCf = requireCf(cfsByName, eq.indexName());
            byte[] prefix = IndexKeys.prefix(eq.valueBytesRaw());

            return new CloseableIterator<>() {
                final RocksIterator it = db.newIterator(idxCf);
                boolean init = false;
                boolean closed = false;

                private void ensureInit() {
                    if (init) return;
                    init = true;
                    it.seek(prefix);
                }

                @Override public boolean hasNext() {
                    if (closed) return false;
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
                        if (v == null) return null;
                        return new RowEntry(pk, v);
                    } catch (RocksDBException ex) {
                        throw new RuntimeException(ex);
                    } finally {
                        if (!it.isValid() || !IndexKeys.startsWith(it.key(), prefix)) {
                            close();
                        }
                    }
                }
                @Override public void close() {
                    if (closed) return;
                    closed = true;
                    it.close();
                }
            };
        }

        if (access instanceof RocksJdbcPlanner.IndexIn in) {
            ColumnFamilyHandle idxCf = requireCf(cfsByName, in.indexName());
            List<byte[]> values = in.valuesBytesRaw();

            return new CloseableIterator<>() {
                int vi = 0;
                RocksIterator it = null;
                byte[] prefix = null;
                final HashSet<BytesKey> seen = new HashSet<>();
                RowEntry next = null;
                boolean closed = false;

                @Override public boolean hasNext() {
                    if (closed) return false;
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

                        closeCurrentIterator();
                    }
                }

                @Override public RowEntry next() {
                    if (!hasNext()) throw new NoSuchElementException();
                    RowEntry r = next;
                    next = null;
                    return r;
                }

                private void closeCurrentIterator() {
                    if (it == null) return;
                    it.close();
                    it = null;
                    prefix = null;
                }

                @Override public void close() {
                    if (closed) return;
                    closed = true;
                    closeCurrentIterator();
                }
            };
        }

        if (access instanceof RocksJdbcPlanner.IndexRange r) {
            ColumnFamilyHandle idxCf = requireCf(cfsByName, r.indexName());

            final byte[] fromPrefix = (r.fromBytesRaw() == null) ? null : IndexKeys.prefix(r.fromBytesRaw());
            final byte[] toPrefix   = (r.toBytesRaw() == null) ? null : IndexKeys.prefix(r.toBytesRaw());
            final byte[] fromEscaped = (r.fromBytesRaw() == null) ? null : IndexKeys.escapeValue(r.fromBytesRaw());
            final byte[] toEscaped   = (r.toBytesRaw() == null) ? null : IndexKeys.escapeValue(r.toBytesRaw());

            return new CloseableIterator<>() {
                final RocksIterator it = db.newIterator(idxCf);
                boolean init = false;
                RowEntry next = null;
                boolean closed = false;

                private void ensureInit() {
                    if (init) return;
                    init = true;
                    if (fromPrefix == null) it.seekToFirst();
                    else it.seek(fromPrefix);
                }

                @Override public boolean hasNext() {
                    if (closed) return false;
                    if (next != null) return true;
                    ensureInit();

                    while (it.isValid()) {
                        byte[] k = it.key();
                        byte[] vprefix = IndexKeys.escapedValuePart(k);

                        // lower bound handling: if exclusive and value == from -> skip
                        if (fromEscaped != null && !r.fromInclusive()) {
                            if (Arrays.equals(vprefix, fromEscaped)) {
                                it.next();
                                continue;
                            }
                        }

                        // upper bound handling
                        if (toEscaped != null) {
                            int cmp = lexCompare(Objects.requireNonNull(vprefix), toEscaped);
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

                    close();
                    return false;
                }

                @Override public RowEntry next() {
                    if (!hasNext()) throw new NoSuchElementException();
                    RowEntry r = next;
                    next = null;
                    return r;
                }

                @Override public void close() {
                    if (closed) return;
                    closed = true;
                    it.close();
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

    // --- helpers ---

    private static ColumnFamilyHandle requireCf(Map<String, ColumnFamilyHandle> cfsByName, String name) throws SQLException {
        ColumnFamilyHandle cf = cfsByName.get(name);
        if (cf == null) throw new SQLException("Missing column family: " + name);
        return cf;
    }

    private static Object decode(RocksJdbcTable table, byte[] valueBytes) throws SQLException {
        return table.decoder().decode(valueBytes);
    }

    private static List<RocksJdbcColumn> selectColumns(RocksJdbcTable table, List<RocksJdbcSqlParser.SelectItem> items)
            throws SQLException {
        Map<String, RocksJdbcColumn> byLower = new HashMap<>();
        for (RocksJdbcColumn c : table.columns()) {
            byLower.put(c.name().toLowerCase(Locale.ROOT), c);
        }

        ArrayList<RocksJdbcColumn> out = new ArrayList<>();
        for (RocksJdbcSqlParser.SelectItem item : items) {
            if (!(item instanceof RocksJdbcSqlParser.SelectColumn col)) continue;
            String n = col.name();
            RocksJdbcColumn c = byLower.get(n.trim().toLowerCase(Locale.ROOT));
            if (c == null) throw new SQLException("Unknown column: " + n);
            out.add(c);
        }
        return out;
    }

    private static void runRowQuery(
            CachedRowSet rs,
            RocksDB db,
            Map<String, ColumnFamilyHandle> cfsByName,
            RocksJdbcTable table,
            ColumnFamilyHandle primaryCf,
            RowAccessor acc,
            Condition<?> criteria,
            RocksJdbcWhereParser.Expr legacyWhere,
            RocksJdbcPlanner.Access access,
            List<RocksJdbcColumn> selected,
            List<RocksJdbcSqlParser.OrderItem> orderBy,
            int limit,
            int offset
    ) throws SQLException {
        List<String> labels = new ArrayList<>();
        for (RocksJdbcColumn c : selected) labels.add(c.name());
        Set<String> labelLower = new HashSet<>();
        for (String label : labels) {
            if (label != null) labelLower.add(label.trim().toLowerCase(Locale.ROOT));
        }
        List<String> orderExtras = new ArrayList<>();
        if (orderBy != null) {
            for (RocksJdbcSqlParser.OrderItem item : orderBy) {
                String col = item.column();
                if (col == null) continue;
                String key = col.trim().toLowerCase(Locale.ROOT);
                if (!labelLower.contains(key)) orderExtras.add(col);
            }
        }

        List<RowResult> rows = new ArrayList<>();
        try (CloseableIterator<RowEntry> it = scan(db, cfsByName, table, primaryCf, access)) {
            while (it.hasNext()) {
                RowEntry e = it.next();
                if (e == null) continue;

                Object bean = decode(table, e.valueBytes());
                if (!matchesWhere(bean, acc, criteria, legacyWhere)) continue;

                Object[] values = new Object[selected.size()];
                for (int i = 0; i < selected.size(); i++) {
                    RocksJdbcColumn c = selected.get(i);
                    values[i] = acc.getByGetter(bean, c.getterName());
                }
                Map<String, Object> extraValues = new HashMap<>();
                for (String col : orderExtras) {
                    extraValues.put(col.trim().toLowerCase(Locale.ROOT), acc.get(bean, col));
                }
                rows.add(new RowResult(labels, values, extraValues));
            }
        }

        applyOrderBy(rows, orderBy);

        int out = 0;
        int skipped = 0;
        for (RowResult row : rows) {
            if (skipped < offset) {
                skipped++;
                continue;
            }
            if (out >= limit) break;
            rs.moveToCurrentRow();
            rs.afterLast();
            rs.moveToInsertRow();
            for (int i = 0; i < row.values().length; i++) {
                rs.updateObject(i + 1, row.values()[i]);
            }
            rs.insertRow();
            rs.moveToCurrentRow();
            out++;
        }
    }

    private static void runAggregateQuery(
            CachedRowSet rs,
            RocksDB db,
            Map<String, ColumnFamilyHandle> cfsByName,
            RocksJdbcTable table,
            ColumnFamilyHandle primaryCf,
            RowAccessor acc,
            Condition<?> criteria,
            RocksJdbcWhereParser.Expr legacyWhere,
            RocksJdbcPlanner.Access access,
            RocksJdbcSqlParser.Parsed parsed,
            RocksJdbcWhereParser.Expr havingExpr,
            int limit,
            int offset
    ) throws SQLException {
        List<String> groupByCols = parsed.groupByColumns();
        List<RocksJdbcSqlParser.SelectItem> items = parsed.selectItems();
        List<RocksJdbcSqlParser.OrderItem> orderBy = parsed.orderBy();

        if (parsed.selectAll()) {
            throw new SQLException("SELECT * is not supported with GROUP BY or aggregate functions.");
        }

        boolean hasAgg = items.stream().anyMatch(i -> i instanceof RocksJdbcSqlParser.SelectAgg);
        boolean hasCol = items.stream().anyMatch(i -> i instanceof RocksJdbcSqlParser.SelectColumn);

        if (hasAgg && hasCol && groupByCols.isEmpty()) {
            throw new SQLException("Mixing aggregates and columns without GROUP BY is not supported.");
        }

        if (!groupByCols.isEmpty()) {
            for (RocksJdbcSqlParser.SelectItem item : items) {
                if (item instanceof RocksJdbcSqlParser.SelectColumn col) {
                    if (groupByCols.stream().noneMatch(g -> g.equalsIgnoreCase(col.name()))) {
                        throw new SQLException("Column not in GROUP BY: " + col.name());
                    }
                }
            }
        }

        Map<GroupKey, AggState> groups = new LinkedHashMap<>();

        try (CloseableIterator<RowEntry> it = scan(db, cfsByName, table, primaryCf, access)) {
            while (it.hasNext()) {
                RowEntry e = it.next();
                if (e == null) continue;
                Object bean = decode(table, e.valueBytes());
                if (!matchesWhere(bean, acc, criteria, legacyWhere)) continue;

                Object[] groupValues = new Object[groupByCols.size()];
                for (int i = 0; i < groupByCols.size(); i++) {
                    groupValues[i] = acc.get(bean, groupByCols.get(i));
                }
                GroupKey key = new GroupKey(groupValues);
                AggState state = groups.computeIfAbsent(key, k -> AggState.create(groupValues, groupByCols, items));
                state.accumulate(bean, acc);
            }
        }

        if (groups.isEmpty() && groupByCols.isEmpty()) {
            GroupKey key = new GroupKey(new Object[0]);
            groups.put(key, AggState.create(new Object[0], groupByCols, items));
        }

        List<String> labels = selectLabels(table, items, parsed.selectAll());
        Set<String> labelLower = new HashSet<>();
        for (String label : labels) {
            if (label != null) labelLower.add(label.trim().toLowerCase(Locale.ROOT));
        }
        List<RowResult> rows = new ArrayList<>();
        int out = 0;
        int skipped = 0;
        for (AggState state : groups.values()) {
            Object[] values = new Object[items.size()];
            for (int i = 0; i < items.size(); i++) {
                values[i] = state.valueFor(items.get(i));
            }
            Map<String, Object> extraValues = new HashMap<>();
            for (String groupCol : groupByCols) {
                if (groupCol == null) continue;
                String key = groupCol.trim().toLowerCase(Locale.ROOT);
                if (!labelLower.contains(key)) {
                    extraValues.put(key, state.groupValue(groupCol));
                }
            }
            RowResult row = new RowResult(labels, values, extraValues);
            if (havingExpr != null && !havingExpr.eval(row::valueFor)) continue;
            rows.add(row);
        }

        applyOrderBy(rows, orderBy);

        for (RowResult row : rows) {
            if (skipped < offset) {
                skipped++;
                continue;
            }
            if (out >= limit) break;
            rs.moveToCurrentRow();
            rs.afterLast();
            rs.moveToInsertRow();
            for (int i = 0; i < row.values().length; i++) {
                rs.updateObject(i + 1, row.values()[i]);
            }
            rs.insertRow();
            rs.moveToCurrentRow();
            out++;
        }
    }

    private static boolean matchesWhere(
            Object bean,
            RowAccessor acc,
            Condition<?> criteria,
            RocksJdbcWhereParser.Expr legacyWhere
    ) throws SQLException {
        if (criteria != null) {
            if (!(bean instanceof java.io.Serializable s)) {
                throw new SQLException("Criteria requires Serializable row values: " + bean.getClass().getName());
            }
            return ConditionEvaluator.test((Condition) criteria, s);
        }
        if (legacyWhere != null) {
            return legacyWhere.eval(colName -> acc.get(bean, colName));
        }
        return true;
    }

    private static int limit(Integer sqlLimit, int stmtMaxRows) {
        int lim = (sqlLimit == null) ? Integer.MAX_VALUE : Math.max(0, sqlLimit);
        if (stmtMaxRows > 0) lim = Math.min(lim, stmtMaxRows);
        return lim;
    }

    private static int offset(Integer sqlOffset) {
        if (sqlOffset == null) return 0;
        return Math.max(0, sqlOffset);
    }

    private static RowSetMetaDataImpl buildMeta(List<RocksJdbcColumn> cols) throws SQLException {
        RowSetMetaDataImpl md = new RowSetMetaDataImpl();
        md.setColumnCount(cols.size());
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

    private static RowSetMetaDataImpl buildMetaForSelectItems(
            RocksJdbcTable table,
            List<RocksJdbcSqlParser.SelectItem> items,
            boolean selectAll,
            List<String> groupByCols
    ) throws SQLException {
        if (selectAll && items.isEmpty()) {
            return buildMeta(List.of(table.columns()));
        }
        RowSetMetaDataImpl md = new RowSetMetaDataImpl();
        md.setColumnCount(items.size());
        for (int i = 0; i < items.size(); i++) {
            RocksJdbcSqlParser.SelectItem item = items.get(i);
            int col = i + 1;
            if (item instanceof RocksJdbcSqlParser.SelectColumn sc) {
                RocksJdbcColumn c = columnByName(table, sc.name());
                if (c == null) throw new SQLException("Unknown column: " + sc.name());
                md.setColumnName(col, c.name());
                md.setColumnLabel(col, c.name());
                md.setColumnType(col, c.sqlType());
                md.setColumnTypeName(col, c.typeName());
                md.setColumnDisplaySize(col, c.size());
                md.setScale(col, c.scale());
                md.setNullable(col, c.nullable() ? java.sql.DatabaseMetaData.columnNullable : DatabaseMetaData.columnNoNulls);
            } else if (item instanceof RocksJdbcSqlParser.SelectAgg agg) {
                String label = aggLabel(agg);
                md.setColumnName(col, label);
                md.setColumnLabel(col, label);
                md.setColumnType(col, aggSqlType(table, agg));
                md.setColumnTypeName(col, aggTypeName(table, agg));
                md.setColumnDisplaySize(col, 0);
                md.setScale(col, 0);
                md.setNullable(col, DatabaseMetaData.columnNullable);
            } else {
                throw new SQLException("Unsupported select item: " + item);
            }
        }
        return md;
    }

    private static List<String> selectLabels(
            RocksJdbcTable table,
            List<RocksJdbcSqlParser.SelectItem> items,
            boolean selectAll
    ) throws SQLException {
        if (selectAll && items.isEmpty()) {
            List<String> labels = new ArrayList<>();
            for (RocksJdbcColumn c : table.columns()) labels.add(c.name());
            return labels;
        }
        List<String> labels = new ArrayList<>();
        for (RocksJdbcSqlParser.SelectItem item : items) {
            if (item instanceof RocksJdbcSqlParser.SelectColumn sc) {
                labels.add(sc.name());
            } else if (item instanceof RocksJdbcSqlParser.SelectAgg agg) {
                labels.add(aggLabel(agg));
            }
        }
        return labels;
    }

    private static int aggSqlType(RocksJdbcTable table, RocksJdbcSqlParser.SelectAgg agg) throws SQLException {
        return switch (agg.fn()) {
            case COUNT -> java.sql.Types.BIGINT;
            case SUM -> java.sql.Types.DECIMAL;
            case MIN, MAX -> {
                if (agg.column() == null) yield java.sql.Types.VARCHAR;
                RocksJdbcColumn c = columnByName(table, agg.column());
                if (c == null) throw new SQLException("Unknown column: " + agg.column());
                yield c.sqlType();
            }
        };
    }

    private static String aggTypeName(RocksJdbcTable table, RocksJdbcSqlParser.SelectAgg agg) throws SQLException {
        return switch (agg.fn()) {
            case COUNT -> "BIGINT";
            case SUM -> "DECIMAL";
            case MIN, MAX -> {
                if (agg.column() == null) yield "VARCHAR";
                RocksJdbcColumn c = columnByName(table, agg.column());
                if (c == null) throw new SQLException("Unknown column: " + agg.column());
                yield c.typeName();
            }
        };
    }

    private static String aggLabel(RocksJdbcSqlParser.SelectAgg agg) {
        if (agg.fn() == RocksJdbcSqlParser.AggFn.COUNT && agg.countStar()) return "COUNT";
        if (agg.column() == null) return agg.fn().name();
        return agg.fn().name() + "_" + agg.column();
    }

    private static RocksJdbcColumn columnByName(RocksJdbcTable table, String name) {
        if (table == null || name == null) return null;
        String want = name.trim().toLowerCase(Locale.ROOT);
        for (RocksJdbcColumn c : table.columns()) {
            if (c.name().trim().equalsIgnoreCase(want)) return c;
        }
        return null;
    }

    private static RocksJdbcPlanner.Access planCriteriaAccess(
            RocksJdbcTable table,
            Condition<?> criteria,
            Map<String, org.github.dbjo.criteria.PropertyTerm<?, ? extends java.io.Serializable>> termsByColumnLower
    ) throws SQLException {
        if (criteria == null) return new RocksJdbcPlanner.FullScan();

        Map<String, CriteriaIndexPlanner.IndexInfo> indexByPropLower = new HashMap<>();
        for (RocksJdbcIndex ix : table.indexes()) {
            if (ix == null) continue;
            String[] cols = ix.columnNames();
            if (cols == null || cols.length == 0 || cols[0] == null) continue;
            String colName = cols[0].trim().toLowerCase(Locale.ROOT);
            org.github.dbjo.criteria.PropertyTerm<?, ? extends java.io.Serializable> term = termsByColumnLower.get(colName);
            if (term == null) continue;
            String propName = term.prop().getPropertyName();
            if (propName == null || propName.isBlank()) continue;
            RocksJdbcColumn col = columnByName(table, cols[0]);
            if (col == null) continue;
            indexByPropLower.put(propName.trim().toLowerCase(Locale.ROOT), new CriteriaIndexPlanner.IndexInfo(
                    ix.indexName(),
                    value -> {
                        try {
                            return RocksJdbcValueEncoder.encodeForColumn(col, value);
                        } catch (SQLException e) {
                            return null;
                        }
                    }
            ));
        }

        CriteriaIndexPlanner.Candidate cand = CriteriaIndexPlanner.bestCandidate(criteria, indexByPropLower);
        if (cand == null) return new RocksJdbcPlanner.FullScan();

        if (!cand.unionPredicates().isEmpty()) {
            List<IndexPredicate> union = cand.unionPredicates();
            String indexName = union.get(0) instanceof IndexPredicate.Eq eq ? eq.indexName() : null;
            if (indexName == null) return new RocksJdbcPlanner.FullScan();
            ArrayList<byte[]> values = new ArrayList<>();
            for (IndexPredicate ip : union) {
                if (!(ip instanceof IndexPredicate.Eq eq)) return new RocksJdbcPlanner.FullScan();
                values.add(eq.valueBytes());
            }
            return new RocksJdbcPlanner.IndexIn(indexName, values);
        }

        IndexPredicate ip = cand.predicate();
        if (ip instanceof IndexPredicate.Eq eq) {
            return new RocksJdbcPlanner.IndexEq(eq.indexName(), eq.valueBytes());
        }
        if (ip instanceof IndexPredicate.Range r) {
            return new RocksJdbcPlanner.IndexRange(r.indexName(), r.from(), r.fromInclusive(), r.to(), r.toInclusive());
        }
        return new RocksJdbcPlanner.FullScan();
    }

    private static CachedRowSet singleLong(String name, long v) throws SQLException {
        CachedRowSet rs = RowSetProvider.newFactory().createCachedRowSet();
        RowSetMetaDataImpl md = new RowSetMetaDataImpl();
        md.setColumnCount(1);
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

    private static void applyOrderBy(List<RowResult> rows, List<RocksJdbcSqlParser.OrderItem> orderBy) {
        if (orderBy == null || orderBy.isEmpty() || rows.isEmpty()) return;
        rows.sort((a, b) -> compareRow(a, b, orderBy));
    }

    private static String normalizeHavingSql(String havingSql) {
        if (havingSql == null || havingSql.isBlank()) return havingSql;
        String normalized = replaceAggReferences(havingSql, "COUNT", true);
        normalized = replaceAggReferences(normalized, "SUM", false);
        normalized = replaceAggReferences(normalized, "MIN", false);
        normalized = replaceAggReferences(normalized, "MAX", false);
        return normalized;
    }

    private static String replaceAggReferences(String sql, String fn, boolean allowStar) {
        String normalized = sql;
        if (allowStar) {
            java.util.regex.Pattern starPattern = java.util.regex.Pattern.compile(
                    "(?i)" + fn + "\\s*\\(\\s*(\\*|1)\\s*\\)"
            );
            java.util.regex.Matcher starMatcher = starPattern.matcher(normalized);
            StringBuffer sbStar = new StringBuffer();
            while (starMatcher.find()) {
                starMatcher.appendReplacement(sbStar, fn);
            }
            starMatcher.appendTail(sbStar);
            normalized = sbStar.toString();
        }
        java.util.regex.Pattern colPattern = java.util.regex.Pattern.compile(
                "(?i)" + fn + "\\s*\\(\\s*([^\\)]+)\\s*\\)"
        );
        java.util.regex.Matcher colMatcher = colPattern.matcher(normalized);
        StringBuffer sbCol = new StringBuffer();
        while (colMatcher.find()) {
            String raw = colMatcher.group(1);
            String col = normalizeIdentifier(raw);
            String label = fn + "_" + col;
            colMatcher.appendReplacement(sbCol, java.util.regex.Matcher.quoteReplacement(label));
        }
        colMatcher.appendTail(sbCol);
        return sbCol.toString();
    }

    private static String normalizeIdentifier(String raw) {
        if (raw == null) return "";
        String s = raw.trim();
        if (s.length() >= 2) {
            char first = s.charAt(0);
            char last = s.charAt(s.length() - 1);
            if ((first == '"' && last == '"') || (first == '`' && last == '`') || (first == '[' && last == ']')) {
                s = s.substring(1, s.length() - 1);
            }
        }
        int dot = s.lastIndexOf('.');
        if (dot >= 0 && dot + 1 < s.length()) {
            s = s.substring(dot + 1);
        }
        return s.trim();
    }

    private static int compareRow(RowResult a, RowResult b, List<RocksJdbcSqlParser.OrderItem> orderBy) {
        for (RocksJdbcSqlParser.OrderItem item : orderBy) {
            int cmp = compareValues(a.valueFor(item.column()), b.valueFor(item.column()));
            if (cmp != 0) return item.descending() ? -cmp : cmp;
        }
        return 0;
    }

    private static final class RowResult {
        private final Map<String, Integer> indexByLabelLower = new HashMap<>();
        private final Map<String, Object> extraByLabelLower = new HashMap<>();
        private final Object[] values;

        RowResult(List<String> labels, Object[] values, Map<String, Object> extraValues) {
            this.values = values;
            for (int i = 0; i < labels.size(); i++) {
                String label = labels.get(i);
                if (label != null) indexByLabelLower.put(label.trim().toLowerCase(Locale.ROOT), i);
            }
            if (extraValues != null) {
                for (Map.Entry<String, Object> entry : extraValues.entrySet()) {
                    String key = entry.getKey();
                    if (key == null) continue;
                    extraByLabelLower.put(key.trim().toLowerCase(Locale.ROOT), entry.getValue());
                }
            }
        }

        Object valueFor(String label) {
            if (label == null) return null;
            Integer idx = indexByLabelLower.get(label.trim().toLowerCase(Locale.ROOT));
            if (idx != null) return values[idx];
            return extraByLabelLower.get(label.trim().toLowerCase(Locale.ROOT));
        }

        Object[] values() {
            return values;
        }
    }

    private static final class GroupKey {
        private final Object[] values;
        private final int hash;

        GroupKey(Object[] values) {
            this.values = (values == null) ? new Object[0] : values.clone();
            this.hash = Arrays.deepHashCode(this.values);
        }

        @Override public boolean equals(Object o) {
            return (o instanceof GroupKey k) && Arrays.deepEquals(values, k.values);
        }

        @Override public int hashCode() { return hash; }
    }

    private static final class AggState {
        private final Object[] groupValues;
        private final AggAccumulator[] accs;

        private AggState(Object[] groupValues, AggAccumulator[] accs) {
            this.groupValues = (groupValues == null) ? new Object[0] : groupValues.clone();
            this.accs = accs;
        }

        static AggState create(
                Object[] groupValues,
                List<String> groupByCols,
                List<RocksJdbcSqlParser.SelectItem> items
        ) {
            AggAccumulator[] accs = new AggAccumulator[items.size()];
            for (int i = 0; i < items.size(); i++) {
                RocksJdbcSqlParser.SelectItem item = items.get(i);
                if (item instanceof RocksJdbcSqlParser.SelectAgg agg) {
                    accs[i] = new AggAccumulator(agg);
                }
            }
            AggState state = new AggState(groupValues, accs);
            state.initGroupIndex(groupByCols);
            return state;
        }

        void accumulate(Object bean, RowAccessor acc) throws SQLException {
            for (AggAccumulator a : accs) {
                if (a == null) continue;
                a.accumulate(bean, acc);
            }
        }

        Object valueFor(RocksJdbcSqlParser.SelectItem item) {
            if (item instanceof RocksJdbcSqlParser.SelectColumn col) {
                Integer idx = groupIndexByColumnLower.get(col.name().trim().toLowerCase(Locale.ROOT));
                return (idx == null) ? null : groupValues[idx];
            }
            if (item instanceof RocksJdbcSqlParser.SelectAgg agg) {
                AggAccumulator acc = accFor(agg);
                return acc == null ? null : acc.value();
            }
            return null;
        }

        Object groupValue(String col) {
            if (col == null) return null;
            Integer idx = groupIndexByColumnLower.get(col.trim().toLowerCase(Locale.ROOT));
            return (idx == null) ? null : groupValues[idx];
        }

        private final Map<String, Integer> groupIndexByColumnLower = new HashMap<>();

        private void initGroupIndex(List<String> groupByCols) {
            for (int i = 0; i < groupByCols.size(); i++) {
                String key = groupByCols.get(i);
                if (key != null) groupIndexByColumnLower.put(key.trim().toLowerCase(Locale.ROOT), i);
            }
        }

        private AggAccumulator accFor(RocksJdbcSqlParser.SelectAgg agg) {
            for (AggAccumulator a : accs) {
                if (a != null && a.agg == agg) return a;
            }
            return null;
        }
    }

    private static final class AggAccumulator {
        private final RocksJdbcSqlParser.SelectAgg agg;
        private long count = 0L;
        private BigDecimal sum = BigDecimal.ZERO;
        private Object min;
        private Object max;
        private boolean hasValue;

        AggAccumulator(RocksJdbcSqlParser.SelectAgg agg) {
            this.agg = agg;
        }

        void accumulate(Object bean, RowAccessor acc) throws SQLException {
            Object v = (agg.column() == null) ? null : acc.get(bean, agg.column());
            switch (agg.fn()) {
                case COUNT -> {
                    if (agg.countStar()) {
                        count++;
                    } else if (v != null) {
                        count++;
                    }
                }
                case SUM -> {
                    if (v == null) return;
                    BigDecimal bd = toDecimalOrNull(v);
                    if (bd != null) {
                        sum = sum.add(bd);
                        hasValue = true;
                    }
                }
                case MIN -> {
                    if (v == null) return;
                    if (min == null || compareValues(v, min) < 0) min = v;
                    hasValue = true;
                }
                case MAX -> {
                    if (v == null) return;
                    if (max == null || compareValues(v, max) > 0) max = v;
                    hasValue = true;
                }
            }
        }

        Object value() {
            return switch (agg.fn()) {
                case COUNT -> count;
                case SUM -> hasValue ? sum : null;
                case MIN -> hasValue ? min : null;
                case MAX -> hasValue ? max : null;
            };
        }
    }

    private static BigDecimal toDecimalOrNull(Object o) {
        if (o == null) return null;
        if (o instanceof BigDecimal bd) return bd;
        if (o instanceof Number n) {
            try {
                return new BigDecimal(String.valueOf(n));
            } catch (Exception ignore) {
                return null;
            }
        }
        if (o instanceof String s) {
            try {
                return new BigDecimal(s.trim());
            } catch (Exception ignore) {
                return null;
            }
        }
        return null;
    }

    private static int compareValues(Object a, Object b) {
        if (a == null && b == null) return 0;
        if (a == null) return -1;
        if (b == null) return 1;
        if (a instanceof Number || b instanceof Number) {
            BigDecimal da = toDecimalOrNull(a);
            BigDecimal db = toDecimalOrNull(b);
            if (da != null && db != null) return da.compareTo(db);
        }
        if (a instanceof Comparable<?> ca && a.getClass().isInstance(b)) {
            @SuppressWarnings("unchecked")
            int c = ((Comparable<Object>) ca).compareTo(b);
            return c;
        }
        return String.valueOf(a).compareTo(String.valueOf(b));
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
