package org.github.dbjo.rdb.jdbc.catalog;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Minimal SQL parser for DataGrip-style queries:
 *
 * Supported:
 *  - SELECT * FROM t [WHERE ...] [LIMIT n]
 *  - SELECT c1,c2 FROM t [WHERE ...] [LIMIT n]
 *  - SELECT COUNT(*) FROM t [WHERE ...]
 */
public final class RocksJdbcSqlParser {
    private RocksJdbcSqlParser() {}

    public sealed interface SelectItem permits SelectColumn, SelectAgg {}
    public record SelectColumn(String name) implements SelectItem {}
    public record SelectAgg(AggFn fn, String column, boolean countStar) implements SelectItem {}
    public enum AggFn { COUNT, MIN, MAX, SUM }

    public record Parsed(
            String tableName,
            List<SelectItem> selectItems,
            boolean selectAll,
            String whereSql,             // null => none
            List<String> groupByColumns,
            Integer limit                // null => none
    ) {}

    public static Parsed parse(String sql) throws SQLException {
        if (sql == null) throw new SQLException("SQL is null");
        String s = stripTrailingSemicolon(sql.trim());
        if (s.isEmpty()) throw new SQLException("Empty SQL");

        // normalize spaces but keep WHERE chunk intact later
        String u = s.toUpperCase(Locale.ROOT);

        if (!u.startsWith("SELECT ")) {
            throw new SQLException("Only SELECT is supported");
        }

        int fromIdx = indexOfKeyword(u, " FROM ");
        if (fromIdx < 0) throw new SQLException("Missing FROM");

        String selectPart = s.substring("SELECT ".length(), fromIdx).trim();
        String tail = s.substring(fromIdx + " FROM ".length()).trim();

        // split tail into table, optional WHERE, optional LIMIT
        String tailU = tail.toUpperCase(Locale.ROOT);

        String table;
        String where = null;
        Integer limit = null;

        int whereIdx = indexOfKeyword(tailU, " WHERE ");
        int groupIdx = indexOfKeyword(tailU, " GROUP BY ");
        int limitIdx = indexOfKeyword(tailU, " LIMIT ");

        int tableEnd = firstPos(tail.length(), whereIdx, groupIdx, limitIdx);
        table = tail.substring(0, tableEnd).trim();

        if (whereIdx >= 0) {
            int whereStart = whereIdx + " WHERE ".length();
            int whereEnd = firstPos(tail.length(), nextPosAfter(whereIdx, groupIdx), nextPosAfter(whereIdx, limitIdx));
            where = tail.substring(whereStart, whereEnd).trim();
            if (where.isBlank()) where = null;
        }

        List<String> groupBy = new ArrayList<>();
        if (groupIdx >= 0) {
            int groupStart = groupIdx + " GROUP BY ".length();
            int groupEnd = firstPos(tail.length(), nextPosAfter(groupIdx, limitIdx));
            String groupPart = tail.substring(groupStart, groupEnd).trim();
            if (!groupPart.isBlank()) {
                for (String g : groupPart.split(",")) {
                    String gg = g.trim();
                    if (!gg.isEmpty()) groupBy.add(stripIdentifierQuotes(gg));
                }
            }
        }

        if (limitIdx >= 0) {
            int limitStart = limitIdx + " LIMIT ".length();
            String lim = tail.substring(limitStart).trim();
            limit = parseLimit(lim);
        }

        table = stripSchema(table);

        boolean selectAll = selectPart.equals("*");
        List<SelectItem> items = new ArrayList<>();

        if (!selectAll) {
            for (String part : splitSelect(selectPart)) {
                String cc = part.trim();
                if (cc.isEmpty()) continue;
                SelectItem item = parseSelectItem(cc);
                items.add(item);
            }
        }

        return new Parsed(stripIdentifierQuotes(table), items, selectAll, where, groupBy, limit);
    }

    private static SelectItem parseSelectItem(String token) {
        String trimmed = token.trim();
        int lp = trimmed.indexOf('(');
        int rp = trimmed.lastIndexOf(')');
        if (lp > 0 && rp > lp) {
            String fn = trimmed.substring(0, lp).trim().toUpperCase(Locale.ROOT);
            String arg = trimmed.substring(lp + 1, rp).trim();
            AggFn agg = switch (fn) {
                case "COUNT" -> AggFn.COUNT;
                case "MIN" -> AggFn.MIN;
                case "MAX" -> AggFn.MAX;
                case "SUM" -> AggFn.SUM;
                default -> null;
            };
            if (agg != null) {
                String argNorm = stripIdentifierQuotes(arg);
                boolean countStar = argNorm.equals("*") || argNorm.equals("1");
                String col = countStar ? null : argNorm;
                return new SelectAgg(agg, col, countStar);
            }
        }
        return new SelectColumn(stripIdentifierQuotes(trimmed));
    }

    private static Integer parseLimit(String lim) throws SQLException {
        if (lim == null || lim.isBlank()) return null;
        // allow trailing tokens
        String[] parts = lim.trim().split("\\s+");
        try {
            int v = Integer.parseInt(parts[0]);
            return Math.max(0, v);
        } catch (Exception e) {
            throw new SQLException("Bad LIMIT: " + lim);
        }
    }

    private static String stripTrailingSemicolon(String s) {
        while (s.endsWith(";")) s = s.substring(0, s.length() - 1).trim();
        return s;
    }

    private static int indexOfKeyword(String upper, String kw) {
        return upper.indexOf(kw);
    }

    private static int firstPos(int fallback, int... positions) {
        int best = fallback;
        for (int p : positions) {
            if (p >= 0 && p < best) best = p;
        }
        return best;
    }

    private static int nextPosAfter(int base, int candidate) {
        if (candidate < 0) return -1;
        return candidate > base ? candidate : -1;
    }

    private static List<String> splitSelect(String selectPart) {
        ArrayList<String> out = new ArrayList<>();
        int depth = 0;
        int start = 0;
        for (int i = 0; i < selectPart.length(); i++) {
            char c = selectPart.charAt(i);
            if (c == '(') depth++;
            if (c == ')') depth = Math.max(0, depth - 1);
            if (c == ',' && depth == 0) {
                out.add(selectPart.substring(start, i));
                start = i + 1;
            }
        }
        out.add(selectPart.substring(start));
        return out;
    }

    private static String stripSchema(String table) {
        // PUBLIC.CLIENT -> CLIENT
        int dot = table.lastIndexOf('.');
        if (dot >= 0 && dot + 1 < table.length()) return table.substring(dot + 1).trim();
        return table;
    }

    private static String stripIdentifierQuotes(String s) {
        if (s == null) return null;
        s = s.trim();
        if (s.length() >= 2) {
            char a = s.charAt(0), b = s.charAt(s.length() - 1);
            if ((a == '"' && b == '"') || (a == '`' && b == '`') || (a == '[' && b == ']')) {
                return s.substring(1, s.length() - 1);
            }
        }
        return s;
    }
}
