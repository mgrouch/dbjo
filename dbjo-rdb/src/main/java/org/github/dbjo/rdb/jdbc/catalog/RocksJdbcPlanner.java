package org.github.dbjo.rdb.jdbc.catalog;

import java.sql.SQLException;
import java.util.*;

/**
 * Chooses the best access path:
 *  - IndexEq for col = literal
 *  - IndexIn for col IN (...)
 *  - IndexRange for <,<=,>,>=,BETWEEN
 *  - FullScan otherwise
 *
 * We only use an index if the column is the FIRST column of the index.
 */
public final class RocksJdbcPlanner {
    private RocksJdbcPlanner() {}

    public sealed interface Access permits FullScan, IndexEq, IndexIn, IndexRange {}

    public record FullScan() implements Access {}

    public record IndexEq(String indexName, byte[] valueBytesRaw) implements Access {}

    public record IndexIn(String indexName, List<byte[]> valuesBytesRaw) implements Access {}

    public record IndexRange(String indexName,
                             byte[] fromBytesRaw, boolean fromInclusive,
                             byte[] toBytesRaw, boolean toInclusive) implements Access {}

    public static Access plan(RocksJdbcTable table, RocksJdbcWhereParser.Expr where) throws SQLException {
        if (where == null) return new FullScan();
        // find best single predicate (AND choose best; OR-of-eq on same col -> IN)
        Candidate c = bestCandidate(table, where);
        return (c == null) ? new FullScan() : c.access;
    }

    private record Candidate(int score, Access access) {}

    private static Candidate bestCandidate(RocksJdbcTable table, RocksJdbcWhereParser.Expr e) throws SQLException {
        if (e instanceof RocksJdbcWhereParser.True) return null;
        if (e instanceof RocksJdbcWhereParser.And a) {
            Candidate l = bestCandidate(table, a.left());
            Candidate r = bestCandidate(table, a.right());
            return better(l, r);
        }
        if (e instanceof RocksJdbcWhereParser.Or o) {
            // OR-of-Eq on same col -> IndexIn
            List<RocksJdbcWhereParser.Cmp> eqs = new ArrayList<>();
            if (collectOrEq(o, eqs) && !eqs.isEmpty()) {
                String col = eqs.get(0).col();
                for (RocksJdbcWhereParser.Cmp c : eqs) {
                    if (c.op() != RocksJdbcWhereParser.Op.EQ) return null;
                    if (!col.equalsIgnoreCase(c.col())) return null;
                }
                RocksJdbcIndex ix = firstIndexOn(table, col);
                if (ix == null) return null;

                ArrayList<byte[]> vs = new ArrayList<>();
                for (RocksJdbcWhereParser.Cmp c : eqs) {
                    Object v = c.lit().value();
                    if (v == null) return null;
                    byte[] vb = RocksJdbcValueEncoder.encodeForColumn(columnFor(table, c.col()), v);
                    if (vb == null) return null;
                    vs.add(vb);
                }
                return new Candidate(900 - vs.size(), new IndexIn(ix.indexName(), vs));
            }
            return null;
        }
        if (e instanceof RocksJdbcWhereParser.Not) return null;

        if (e instanceof RocksJdbcWhereParser.Cmp c) {
            RocksJdbcIndex ix = firstIndexOn(table, c.col());
            if (ix == null) return null;

            Object v = c.lit().value();
            if (v == null) return null;

            byte[] vb = RocksJdbcValueEncoder.encodeForColumn(columnFor(table, c.col()), v);
            if (vb == null) return null;

            return switch (c.op()) {
                case EQ -> new Candidate(1000, new IndexEq(ix.indexName(), vb));
                case LT -> new Candidate(600, new IndexRange(ix.indexName(), null, false, vb, false));
                case LE -> new Candidate(600, new IndexRange(ix.indexName(), null, false, vb, true));
                case GT -> new Candidate(600, new IndexRange(ix.indexName(), vb, false, null, false));
                case GE -> new Candidate(600, new IndexRange(ix.indexName(), vb, true,  null, false));
                case NE -> null;
            };
        }

        if (e instanceof RocksJdbcWhereParser.In in) {
            if (in.negated()) return null;
            RocksJdbcIndex ix = firstIndexOn(table, in.col());
            if (ix == null) return null;

            ArrayList<byte[]> vs = new ArrayList<>();
            for (var l : in.values()) {
                Object v = l.value();
                if (v == null) return null;
                byte[] vb = RocksJdbcValueEncoder.encodeForColumn(columnFor(table, in.col()), v);
                if (vb == null) return null;
                vs.add(vb);
            }
            if (vs.isEmpty()) return null;
            return new Candidate(900 - vs.size(), new IndexIn(ix.indexName(), vs));
        }

        if (e instanceof RocksJdbcWhereParser.Between b) {
            if (b.negated()) return null;
            RocksJdbcIndex ix = firstIndexOn(table, b.col());
            if (ix == null) return null;
            Object lo = b.lo().value();
            Object hi = b.hi().value();
            if (lo == null && hi == null) return null;
            byte[] l = (lo == null) ? null : RocksJdbcValueEncoder.encodeForColumn(columnFor(table, b.col()), lo);
            byte[] h = (hi == null) ? null : RocksJdbcValueEncoder.encodeForColumn(columnFor(table, b.col()), hi);
            if ((lo != null && l == null) || (hi != null && h == null)) return null;
            return new Candidate(650, new IndexRange(ix.indexName(), l, true, h, true));
        }

        return null;
    }

    private static Candidate better(Candidate a, Candidate b) {
        if (a == null) return b;
        if (b == null) return a;
        return (b.score > a.score) ? b : a;
    }

    private static boolean collectOrEq(RocksJdbcWhereParser.Expr e, List<RocksJdbcWhereParser.Cmp> out) {
        if (e instanceof RocksJdbcWhereParser.Cmp c) {
            if (c.op() == RocksJdbcWhereParser.Op.EQ) out.add(c);
            return c.op() == RocksJdbcWhereParser.Op.EQ;
        }
        if (e instanceof RocksJdbcWhereParser.Or o) {
            return collectOrEq(o.left(), out) && collectOrEq(o.right(), out);
        }
        return false;
    }

    private static RocksJdbcIndex firstIndexOn(RocksJdbcTable table, String col) {
        if (col == null) return null;
        String want = col.trim().toUpperCase(Locale.ROOT);

        for (RocksJdbcIndex ix : table.indexes()) {
            if (ix == null) continue;
            String[] cols = ix.columnNames();
            if (cols == null || cols.length == 0) continue;
            if (cols[0] != null && cols[0].trim().toUpperCase(Locale.ROOT).equals(want)) {
                return ix;
            }
        }
        return null;
    }

    private static RocksJdbcColumn columnFor(RocksJdbcTable table, String col) {
        if (table == null || col == null) return null;
        String want = col.trim().toLowerCase(Locale.ROOT);
        if (want.isEmpty()) return null;
        for (RocksJdbcColumn c : table.columns()) {
            if (c != null && c.name().trim().equalsIgnoreCase(want)) {
                return c;
            }
        }
        return null;
    }
}
