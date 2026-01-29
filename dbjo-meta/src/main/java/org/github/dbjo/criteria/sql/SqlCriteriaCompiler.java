package org.github.dbjo.criteria.sql;

import org.github.dbjo.criteria.*;
import org.github.dbjo.meta.entity.PropertyMeta;
import org.github.dbjo.meta.jdbc.DbMeta;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public final class SqlCriteriaCompiler {
    private SqlCriteriaCompiler() {}

    public record Compiled(String sql, Object[] params) {}

    public static Compiled compileSelectAll(DbMeta<?> meta, Query<? extends Serializable> q) {
        Objects.requireNonNull(meta, "meta");
        Objects.requireNonNull(q, "q");

        String base = meta.selectAllBaseSql();
        Builder b = new Builder(meta);

        if (q.scan() != null) appendScan(b, q.scan());
        appendCond(b, q.where());

        String where = b.whereSql();
        String sql = where.isEmpty() ? base : (base + " WHERE " + where);
        return new Compiled(sql, b.params.toArray());
    }

    private static void appendScan(Builder b, Scan<?, ? extends Serializable> scan) {
        Range<? extends Serializable> r = scan.range();
        if (r == null) return;

        String col = b.col(scan.prop());

        if (r.lowerBound() != Bound.UNBOUNDED) {
            String op = (r.lowerBound() == Bound.INCLUSIVE) ? ">=" : ">";
            b.and(col + " " + op + " ?", r.lower());
        }
        if (r.upperBound() != Bound.UNBOUNDED) {
            String op = (r.upperBound() == Bound.INCLUSIVE) ? "<=" : "<";
            b.and(col + " " + op + " ?", r.upper());
        }
    }

    private static void appendCond(Builder b, Condition<? extends Serializable> cond) {
        if (cond == null) return;
        String expr = compileCond(b, cond);
        if (!expr.isEmpty()) b.and(expr);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static String compileCond(Builder b, Condition cond) {
        if (cond instanceof TrueCond) return "";
        if (cond instanceof FalseCond) return "1=0";

        if (cond instanceof Eq e) {
            String col = b.col(e.prop());
            Object v = e.value();
            if (v == null) return col + " IS NULL";
            b.params.add(v);
            return col + " = ?";
        }

        if (cond instanceof Ne e) {
            String col = b.col(e.prop());
            Object v = e.value();
            if (v == null) return col + " IS NOT NULL";
            b.params.add(v);
            return col + " <> ?";
        }

        if (cond instanceof IsNull e) return b.col(e.prop()) + " IS NULL";
        if (cond instanceof IsNotNull e) return b.col(e.prop()) + " IS NOT NULL";

        if (cond instanceof Between e) {
            String col = b.col(e.prop());
            b.params.add(e.lo());
            b.params.add(e.hi());
            return col + " BETWEEN ? AND ?";
        }

        if (cond instanceof Cmp e) {
            String col = b.col(e.prop());
            String op = switch (e.op()) {
                case LT -> "<";
                case LE -> "<=";
                case GT -> ">";
                case GE -> ">=";
            };
            b.params.add(e.value());
            return col + " " + op + " ?";
        }

        if (cond instanceof In e) {
            String col = b.col(e.prop());
            List vals = e.values();
            if (vals == null || vals.isEmpty()) return "1=0";

            boolean hasNull = false;
            ArrayList<Object> nonNull = new ArrayList<>(vals.size());
            for (Object v : vals) {
                if (v == null) hasNull = true;
                else nonNull.add(v);
            }

            if (nonNull.isEmpty()) return col + " IS NULL";

            StringBuilder sb = new StringBuilder();
            if (hasNull) sb.append('(');
            sb.append(col).append(" IN (");
            for (int i = 0; i < nonNull.size(); i++) {
                if (i > 0) sb.append(',');
                sb.append('?');
                b.params.add(nonNull.get(i));
            }
            sb.append(')');
            if (hasNull) sb.append(" OR ").append(col).append(" IS NULL)");
            return sb.toString();
        }

        if (cond instanceof Like e) {
            String col = b.col(e.prop());
            b.params.add(e.pattern());
            return col + " LIKE ?";
        }

        if (cond instanceof Not n) {
            String inner = compileCond(b, n.inner());
            if (inner.isEmpty()) return "";
            return "NOT (" + inner + ")";
        }

        if (cond instanceof And a) {
            String l = compileCond(b, a.left());
            String r = compileCond(b, a.right());
            if (l.isEmpty()) return r;
            if (r.isEmpty()) return l;
            return "(" + l + ") AND (" + r + ")";
        }

        if (cond instanceof Or o) {
            String l = compileCond(b, o.left());
            String r = compileCond(b, o.right());
            if (l.isEmpty()) return r;
            if (r.isEmpty()) return l;
            return "(" + l + ") OR (" + r + ")";
        }

        throw new IllegalArgumentException("Unsupported condition: " + cond.getClass().getName());
    }

    private static final class Builder {
        private final DbMeta<?> meta;
        private final ArrayList<String> terms = new ArrayList<>();
        private final ArrayList<Object> params = new ArrayList<>();

        private Builder(DbMeta<?> meta) { this.meta = meta; }

        private <B extends Serializable, V extends Serializable> String col(PropertyMeta<B, V> prop) {
            String name = (prop == null) ? null : prop.getPropertyName();
            if (name == null || name.isBlank()) throw new IllegalArgumentException("PropertyMeta has blank propertyName");
            return meta.columnSql(name);
        }

        private void and(String expr, Object... newParams) {
            String e = (expr == null) ? "" : expr.trim();
            if (!e.isEmpty()) terms.add(e);
            if (newParams != null) Collections.addAll(params, newParams);
        }

        private String whereSql() {
            if (terms.isEmpty()) return "";
            if (terms.size() == 1) return terms.get(0);
            StringBuilder sb = new StringBuilder(terms.size() * 16);
            for (int i = 0; i < terms.size(); i++) {
                if (i > 0) sb.append(" AND ");
                sb.append('(').append(terms.get(i)).append(')');
            }
            return sb.toString();
        }
    }
}
