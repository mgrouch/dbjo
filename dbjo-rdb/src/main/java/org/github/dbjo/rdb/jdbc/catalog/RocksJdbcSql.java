package org.github.dbjo.rdb.jdbc.catalog;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public final class RocksJdbcSql {
    private RocksJdbcSql() {}

    public enum Kind { LIST_TABLES, SELECT, COUNT }

    /** projection == null or empty => "*" */
    public record Parsed(
            Kind kind,
            String tableName,
            List<String> projection,
            String whereSql,
            Integer limit
    ) {}

    public static Parsed parse(String sql) throws SQLException {
        if (sql == null) throw new SQLException("SQL is null");
        String s = stripTrailingSemicolon(sql.trim());
        if (s.isEmpty()) throw new SQLException("SQL is empty");

        String sl = s.toLowerCase(Locale.ROOT).trim();

        // SHOW/LIST TABLES
        if (sl.equals("show tables") || sl.equals("list tables") || sl.equals("tables")) {
            return new Parsed(Kind.LIST_TABLES, null, null, null, null);
        }

        // SELECT ... FROM ...
        if (!sl.startsWith("select ")) {
            throw new SQLException("Only SELECT is supported");
        }

        // split tail LIMIT (top-level)
        Integer limit = null;
        Split tailLimit = splitTailKeywordTopLevel(s, "limit");
        if (tailLimit != null) {
            s = tailLimit.head.trim();
            String lim = tailLimit.tail.trim();
            if (lim.isEmpty()) throw new SQLException("LIMIT without value");
            try {
                limit = Integer.parseInt(lim);
            } catch (NumberFormatException e) {
                throw new SQLException("Bad LIMIT value: " + lim, e);
            }
        }

        // split tail WHERE (top-level)
        String whereSql = null;
        Split tailWhere = splitTailKeywordTopLevel(s, "where");
        if (tailWhere != null) {
            s = tailWhere.head.trim();
            whereSql = tailWhere.tail.trim();
            if (whereSql.isEmpty()) whereSql = null;
        }

        // Now s is "SELECT <proj> FROM <table>"
        int fromPos = indexOfKeywordTopLevel(s, "from");
        if (fromPos < 0) throw new SQLException("Missing FROM");

        String projPart = s.substring(6, fromPos).trim(); // after SELECT
        String fromPart = s.substring(fromPos + 4).trim(); // after FROM
        if (fromPart.isEmpty()) throw new SQLException("Missing table name after FROM");

        String tableName = stripQuotes(firstToken(fromPart));
        if (tableName.isEmpty()) throw new SQLException("Empty table name");

        // Special-case: SELECT * FROM tables
        if (whereSql == null && (projPart.equals("*") || projPart.isEmpty())
                && tableName.equalsIgnoreCase("tables")) {
            return new Parsed(Kind.LIST_TABLES, null, null, null, limit);
        }

        // COUNT(*)
        if (isCountStar(projPart)) {
            return new Parsed(Kind.COUNT, tableName, null, whereSql, limit);
        }

        // Projection list
        List<String> projection = parseProjection(projPart);

        return new Parsed(Kind.SELECT, tableName, projection, whereSql, limit);
    }

    private static boolean isCountStar(String projPart) {
        String p = projPart.toLowerCase(Locale.ROOT).replaceAll("\\s+", "");
        return p.equals("count(*)");
    }

    private static List<String> parseProjection(String projPart) throws SQLException {
        String p = projPart.trim();
        if (p.isEmpty() || p.equals("*")) return null;

        List<String> out = new ArrayList<>();
        int depth = 0;
        boolean inSq = false;
        StringBuilder cur = new StringBuilder();
        for (int i = 0; i < p.length(); i++) {
            char c = p.charAt(i);
            if (c == '\'' && !inSq) { inSq = true; cur.append(c); continue; }
            if (c == '\'' && inSq) { inSq = false; cur.append(c); continue; }

            if (!inSq) {
                if (c == '(') depth++;
                else if (c == ')') depth = Math.max(0, depth - 1);
                else if (c == ',' && depth == 0) {
                    String item = cur.toString().trim();
                    if (!item.isEmpty()) out.add(item);
                    cur.setLength(0);
                    continue;
                }
            }
            cur.append(c);
        }
        String last = cur.toString().trim();
        if (!last.isEmpty()) out.add(last);

        if (out.isEmpty()) return null;
        return out;
    }

    private static String stripTrailingSemicolon(String s) {
        String t = s.trim();
        if (t.endsWith(";")) t = t.substring(0, t.length() - 1).trim();
        return t;
    }

    private static String firstToken(String s) {
        int i = 0;
        while (i < s.length() && !Character.isWhitespace(s.charAt(i))) i++;
        return s.substring(0, i);
    }

    private static String stripQuotes(String s) {
        String t = s.trim();
        if (t.length() >= 2) {
            char a = t.charAt(0);
            char b = t.charAt(t.length() - 1);
            if ((a == '"' && b == '"') || (a == '`' && b == '`')) {
                return t.substring(1, t.length() - 1);
            }
        }
        return t;
    }

    private record Split(String head, String tail) {}

    private static Split splitTailKeywordTopLevel(String s, String keyword) {
        int pos = lastIndexOfKeywordTopLevel(s, keyword);
        if (pos < 0) return null;
        String head = s.substring(0, pos).trim();
        String tail = s.substring(pos + keyword.length()).trim();
        return new Split(head, tail);
    }

    private static int indexOfKeywordTopLevel(String s, String keyword) {
        String sl = s.toLowerCase(Locale.ROOT);
        int depth = 0;
        boolean inSq = false;
        for (int i = 0; i <= sl.length() - keyword.length(); i++) {
            char c = sl.charAt(i);
            if (c == '\'' ) inSq = !inSq;
            if (inSq) continue;

            if (c == '(') depth++;
            else if (c == ')') depth = Math.max(0, depth - 1);

            if (depth == 0 && matchesWord(sl, i, keyword)) return i;
        }
        return -1;
    }

    private static int lastIndexOfKeywordTopLevel(String s, String keyword) {
        String sl = s.toLowerCase(Locale.ROOT);
        int depth = 0;
        boolean inSq = false;
        int last = -1;
        for (int i = 0; i <= sl.length() - keyword.length(); i++) {
            char c = sl.charAt(i);
            if (c == '\'') inSq = !inSq;
            if (inSq) continue;

            if (c == '(') depth++;
            else if (c == ')') depth = Math.max(0, depth - 1);

            if (depth == 0 && matchesWord(sl, i, keyword)) last = i;
        }
        return last;
    }

    private static boolean matchesWord(String sl, int i, String kw) {
        if (!sl.regionMatches(true, i, kw, 0, kw.length())) return false;
        boolean leftOk = (i == 0) || !Character.isLetterOrDigit(sl.charAt(i - 1));
        int j = i + kw.length();
        boolean rightOk = (j >= sl.length()) || !Character.isLetterOrDigit(sl.charAt(j));
        return leftOk && rightOk;
    }
}
