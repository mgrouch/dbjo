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

    public record Parsed(
            boolean countStar,
            String tableName,
            List<String> selectColumns,  // empty => *
            String whereSql,             // null => none
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
        int limitIdx = indexOfKeyword(tailU, " LIMIT ");

        if (whereIdx >= 0) {
            table = tail.substring(0, whereIdx).trim();
            String afterWhere = tail.substring(whereIdx + " WHERE ".length()).trim();
            String afterWhereU = afterWhere.toUpperCase(Locale.ROOT);

            int limitIdx2 = indexOfKeyword(afterWhereU, " LIMIT ");
            if (limitIdx2 >= 0) {
                where = afterWhere.substring(0, limitIdx2).trim();
                String lim = afterWhere.substring(limitIdx2 + " LIMIT ".length()).trim();
                limit = parseLimit(lim);
            } else {
                where = afterWhere.isBlank() ? null : afterWhere;
            }
        } else if (limitIdx >= 0) {
            table = tail.substring(0, limitIdx).trim();
            String lim = tail.substring(limitIdx + " LIMIT ".length()).trim();
            limit = parseLimit(lim);
        } else {
            table = tail.trim();
        }

        table = stripSchema(table);

        boolean countStar = isCountStar(selectPart);

        List<String> cols = new ArrayList<>();
        if (!countStar) {
            if (!selectPart.equals("*")) {
                for (String c : selectPart.split(",")) {
                    String cc = c.trim();
                    if (!cc.isEmpty()) cols.add(stripIdentifierQuotes(cc));
                }
            }
        }

        return new Parsed(countStar, stripIdentifierQuotes(table), cols, where, limit);
    }

    private static boolean isCountStar(String selectPart) {
        String u = selectPart.toUpperCase(Locale.ROOT).replace(" ", "");
        return u.equals("COUNT(*)") || u.equals("COUNT(1)");
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
