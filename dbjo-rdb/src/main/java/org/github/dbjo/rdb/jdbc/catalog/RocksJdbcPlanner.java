package org.github.dbjo.rdb.jdbc.catalog;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.*;

public final class RocksJdbcPlanner {
    private RocksJdbcPlanner() {}

    public static RocksJdbcPlan plan(RocksJdbcSql.Parsed parsed, RocksJdbcTableMeta meta) throws SQLException {
        RocksJdbcWhere.Expr whereAst = RocksJdbcWhere.parse(parsed.whereSql());
        RocksJdbcPlan.AccessPath ap = chooseAccessPath(meta, whereAst);

        if (parsed.kind() == RocksJdbcSql.Kind.COUNT) {
            return new RocksJdbcPlan.Count(parsed.tableName(), parsed.limit(), ap, parsed.whereSql());
        }
        if (parsed.kind() == RocksJdbcSql.Kind.SELECT) {
            return new RocksJdbcPlan.Select(parsed.tableName(), parsed.projection(), parsed.limit(), ap, parsed.whereSql());
        }
        return new RocksJdbcPlan.ListTables(parsed.limit());
    }

    private static RocksJdbcPlan.AccessPath chooseAccessPath(RocksJdbcTableMeta meta, RocksJdbcWhere.Expr whereAst) {
        if (whereAst == null) return new RocksJdbcPlan.FullScan();

        // Only AND-only predicates are planned into indexes (OR/NOT => scan)
        List<RocksJdbcWhere.Pred> preds = new ArrayList<>();
        if (!collectAndOnly(whereAst, preds)) return new RocksJdbcPlan.FullScan();

        Map<String, Serializable> eq = new HashMap<>();
        Map<String, RocksJdbcWhere.Between> between = new HashMap<>();
        Map<String, RocksJdbcWhere.In> in = new HashMap<>();

        for (RocksJdbcWhere.Pred p : preds) {
            if (p instanceof RocksJdbcWhere.Cmp c && c.op() == RocksJdbcWhere.Op.EQ) {
                eq.put(normCol(c.col()), c.value());
            } else if (p instanceof RocksJdbcWhere.Between b) {
                between.put(normCol(b.col()), b);
            } else if (p instanceof RocksJdbcWhere.In ii) {
                in.put(normCol(ii.col()), ii);
            }
        }

        RocksJdbcTableMeta.IndexMeta best = null;
        int bestEqPrefix = 0;

        for (RocksJdbcTableMeta.IndexMeta idx : meta.indexes()) {
            int k = eqPrefixLen(idx, eq);
            if (k > bestEqPrefix) { bestEqPrefix = k; best = idx; }
        }

        if (best != null) {
            List<String> idxCols = best.columns();
            List<Serializable> prefix = new ArrayList<>();
            for (int i = 0; i < bestEqPrefix; i++) prefix.add(eq.get(normCol(idxCols.get(i))));

            if (bestEqPrefix < idxCols.size()) {
                String next = normCol(idxCols.get(bestEqPrefix));
                RocksJdbcWhere.Between b = between.get(next);
                if (b != null) return new RocksJdbcPlan.IndexRange(best.name(), idxCols, prefix, b.a(), b.b());
                RocksJdbcWhere.In ii = in.get(next);
                if (ii != null) return new RocksJdbcPlan.IndexIn(best.name(), idxCols, prefix, ii.values());
            }
            return new RocksJdbcPlan.IndexEq(best.name(), idxCols, prefix);
        }

        // No eq-prefix. Try range/in on first column of an index.
        for (RocksJdbcTableMeta.IndexMeta idx : meta.indexes()) {
            if (idx.columns().isEmpty()) continue;
            String c0 = normCol(idx.columns().get(0));
            RocksJdbcWhere.Between b = between.get(c0);
            if (b != null) return new RocksJdbcPlan.IndexRange(idx.name(), idx.columns(), List.of(), b.a(), b.b());
            RocksJdbcWhere.In ii = in.get(c0);
            if (ii != null) return new RocksJdbcPlan.IndexIn(idx.name(), idx.columns(), List.of(), ii.values());
        }

        return new RocksJdbcPlan.FullScan();
    }

    private static int eqPrefixLen(RocksJdbcTableMeta.IndexMeta idx, Map<String, Serializable> eq) {
        int k = 0;
        for (String col : idx.columns()) {
            if (eq.containsKey(normCol(col))) k++;
            else break;
        }
        return k;
    }

    private static boolean collectAndOnly(RocksJdbcWhere.Expr e, List<RocksJdbcWhere.Pred> out) {
        if (e instanceof RocksJdbcWhere.And a) {
            return collectAndOnly(a.left(), out) && collectAndOnly(a.right(), out);
        }
        if (e instanceof RocksJdbcWhere.Pred p) {
            out.add(p);
            return true;
        }
        return false; // Or/Not => no index planning
    }

    private static String normCol(String ident) {
        String raw = (ident == null) ? "" : ident.trim();
        int dot = raw.lastIndexOf('.');
        if (dot >= 0) raw = raw.substring(dot + 1).trim();
        if (raw.length() >= 2) {
            char a = raw.charAt(0), b = raw.charAt(raw.length()-1);
            if ((a == '"' && b == '"') || (a == '`' && b == '`')) raw = raw.substring(1, raw.length()-1);
        }
        return raw.toLowerCase(Locale.ROOT);
    }
}
