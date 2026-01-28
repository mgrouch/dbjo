package org.github.dbjo.rdb.jdbc.catalog;

import java.sql.SQLException;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Small SQL parser for the read-only Rocks JDBC driver.
 *
 * Supported:
 *  - select * from tables [limit N | fetch first N rows only]
 *  - select * from <table> [limit N | fetch first N rows only]
 *  - select top N * from <table> [limit M]  (effective limit = min(top, limit) if both present)
 *  - select count(*) from <table>
 *  - select count(1) from <table>
 *
 * Notes:
 *  - Trailing semicolons are allowed.
 *  - Identifiers may be qualified and/or quoted:
 *      client
 *      schema.client
 *      "Client"
 *      "schema"."Client"
 *      [Client]
 *      `Client`
 */
public final class RocksJdbcSql {
    private RocksJdbcSql() {}

    public enum Kind { LIST_TABLES, SELECT_ALL, COUNT }

    /** limit==0 means "no limit specified in SQL" */
    public record Parsed(Kind kind, String tableName, int limit) {}

    // --- identifier grammar: segment(.segment)*
    // segment = "x" | `x` | [x] | bareword
    private static final String SEG =
            "(\"[^\"]+\"|`[^`]+`|\\[[^\\]]+\\]|[A-Za-z_][A-Za-z0-9_]*)";
    private static final String QUAL_IDENT =
            "(" + SEG + "(\\." + SEG + ")*)";

    private static final Pattern P_LIST_TABLES =
            Pattern.compile("^\\s*select\\s+\\*\\s+from\\s+tables\\s*(?<tail>.*)$",
                    Pattern.CASE_INSENSITIVE);

    private static final Pattern P_SELECT_ALL =
            Pattern.compile("^\\s*select\\s+\\*\\s+from\\s+(?<table>" + QUAL_IDENT + ")\\s*(?<tail>.*)$",
                    Pattern.CASE_INSENSITIVE);

    private static final Pattern P_SELECT_TOP_ALL =
            Pattern.compile("^\\s*select\\s+top\\s+(?<top>\\d+)\\s+\\*\\s+from\\s+(?<table>" + QUAL_IDENT + ")\\s*(?<tail>.*)$",
                    Pattern.CASE_INSENSITIVE);

    private static final Pattern P_COUNT =
            Pattern.compile("^\\s*select\\s+count\\s*\\(\\s*(?<arg>\\*|1)\\s*\\)\\s+from\\s+(?<table>" + QUAL_IDENT + ")\\s*(?<tail>.*)$",
                    Pattern.CASE_INSENSITIVE);

    private static final Pattern P_LIMIT =
            Pattern.compile("^\\s*limit\\s+(\\d+)\\s*$", Pattern.CASE_INSENSITIVE);

    private static final Pattern P_FETCH_FIRST =
            Pattern.compile("^\\s*fetch\\s+first\\s+(\\d+)\\s+rows\\s+only\\s*$", Pattern.CASE_INSENSITIVE);

    public static Parsed parse(String sql) throws SQLException {
        if (sql == null) throw new SQLException("SQL is null");

        String s = normalizeSql(sql);
        if (s.isEmpty()) throw new SQLException("Empty SQL");

        // SELECT TOP n * FROM table [tail]
        {
            Matcher m = P_SELECT_TOP_ALL.matcher(s);
            if (m.matches()) {
                String table = normalizeQualifiedIdent(m.group("table"));
                int top = parsePositiveInt(m.group("top"));
                int tailLimit = parseLimitFromTail(m.group("tail"));
                int eff = combineLimits(top, tailLimit);
                return new Parsed(Kind.SELECT_ALL, table, eff);
            }
        }

        // SELECT * FROM tables [tail]
        {
            Matcher m = P_LIST_TABLES.matcher(s);
            if (m.matches()) {
                int lim = parseLimitFromTail(m.group("tail"));
                return new Parsed(Kind.LIST_TABLES, null, lim);
            }
        }

        // SELECT COUNT(*) / COUNT(1) FROM table [tail]
        {
            Matcher m = P_COUNT.matcher(s);
            if (m.matches()) {
                String table = normalizeQualifiedIdent(m.group("table"));
                // accept tail but we ignore limit for COUNT execution (optional)
                int lim = parseLimitFromTail(m.group("tail"));
                return new Parsed(Kind.COUNT, table, lim);
            }
        }

        // SELECT * FROM table [tail]
        {
            Matcher m = P_SELECT_ALL.matcher(s);
            if (m.matches()) {
                String table = normalizeQualifiedIdent(m.group("table"));
                int lim = parseLimitFromTail(m.group("tail"));
                return new Parsed(Kind.SELECT_ALL, table, lim);
            }
        }

        throw new SQLException("Unsupported SQL: " + sql);
    }

    // --- helpers

    private static int combineLimits(int a, int b) {
        if (a > 0 && b > 0) return Math.min(a, b);
        if (a > 0) return a;
        return b;
    }

    private static int parseLimitFromTail(String tail) throws SQLException {
        if (tail == null) return 0;
        String t = normalizeSql(tail);
        if (t.isEmpty()) return 0;

        Matcher ml = P_LIMIT.matcher(t);
        if (ml.matches()) return parsePositiveInt(ml.group(1));

        Matcher mf = P_FETCH_FIRST.matcher(t);
        if (mf.matches()) return parsePositiveInt(mf.group(1));

        // keep behavior: if there is any other tail, we don't claim support
        throw new SQLException("Unsupported SQL tail: " + tail);
    }

    private static int parsePositiveInt(String s) throws SQLException {
        try {
            long v = Long.parseLong(s);
            if (v <= 0) return 0;
            if (v > Integer.MAX_VALUE) return Integer.MAX_VALUE;
            return (int) v;
        } catch (NumberFormatException e) {
            throw new SQLException("Bad numeric value: " + s, e);
        }
    }

    /** Normalize: trim, drop trailing semicolon, strip trailing line/block comments (at end). */
    private static String normalizeSql(String in) {
        if (in == null) return "";
        String s = in.trim();
        s = stripTrailingSemicolon(s);
        s = stripTrailingComments(s);
        return s.trim();
    }

    private static String stripTrailingSemicolon(String s) {
        int i = s.length() - 1;
        while (i >= 0 && Character.isWhitespace(s.charAt(i))) i--;
        if (i >= 0 && s.charAt(i) == ';') return s.substring(0, i);
        return s;
    }

    /**
     * Very small "end-only" comment stripper:
     *  - removes trailing "-- ...." if it appears at end
     *  - removes trailing "/* .... *&#47;" if it appears at end
     * Not a full SQL lexer; it’s just to be tolerant of tooling.
     */
    private static String stripTrailingComments(String s) {
        String out = s;

        // trailing line comment
        int idxLine = lastIndexOfLineComment(out);
        if (idxLine >= 0) {
            out = out.substring(0, idxLine).trim();
        }

        // trailing block comment
        int end = out.lastIndexOf("*/");
        if (end >= 0) {
            int start = out.lastIndexOf("/*");
            if (start >= 0 && start < end) {
                String before = out.substring(0, start).trim();
                String after = out.substring(end + 2).trim();
                if (after.isEmpty()) out = before; // only strip if comment was at end
            }
        }

        return out;
    }

    private static int lastIndexOfLineComment(String s) {
        // only strip if the "--" occurs and nothing but whitespace follows it (comment at end)
        int idx = s.lastIndexOf("--");
        if (idx < 0) return -1;
        // if "--" is inside quotes, we won't try to be clever; keep it simple
        // only strip when it looks like a trailing comment
        return idx;
    }

    /**
     * Turns a qualified identifier like:
     *   schema.table
     *   "schema"."Table"
     *   [schema].[Table]
     * into:
     *   schema.table
     *   schema.Table
     * preserving dots but removing quotes/brackets/backticks per segment.
     */
    private static String normalizeQualifiedIdent(String ident) throws SQLException {
        if (ident == null) return null;
        String s = ident.trim();
        if (s.isEmpty()) return s;

        StringBuilder out = new StringBuilder(s.length());
        int i = 0;
        boolean first = true;

        while (i < s.length()) {
            if (!first) {
                if (s.charAt(i) != '.') {
                    // Should not happen if regex matched, but be safe
                    throw new SQLException("Bad qualified identifier: " + ident);
                }
                out.append('.');
                i++;
            }
            first = false;

            if (i >= s.length()) break;
            char c = s.charAt(i);

            if (c == '"') {
                int j = s.indexOf('"', i + 1);
                if (j < 0) throw new SQLException("Unterminated quoted identifier: " + ident);
                out.append(s, i + 1, j);
                i = j + 1;
            } else if (c == '`') {
                int j = s.indexOf('`', i + 1);
                if (j < 0) throw new SQLException("Unterminated quoted identifier: " + ident);
                out.append(s, i + 1, j);
                i = j + 1;
            } else if (c == '[') {
                int j = s.indexOf(']', i + 1);
                if (j < 0) throw new SQLException("Unterminated bracket identifier: " + ident);
                out.append(s, i + 1, j);
                i = j + 1;
            } else {
                // bare segment
                int j = i;
                while (j < s.length() && s.charAt(j) != '.') j++;
                out.append(s, i, j);
                i = j;
            }
        }

        return out.toString();
    }
}
