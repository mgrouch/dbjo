package org.github.dbjo.rdb.jdbc.catalog;

import java.sql.SQLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class RocksJdbcSql {
    private RocksJdbcSql() {}

    public enum Kind { LIST_TABLES, SELECT_ALL, COUNT }

    /** limit==0 means "no SQL limit" */
    public record Parsed(Kind kind, String tableName, String whereSql, int limit) {}

    private static final String SEG =
            "(\"[^\"]+\"|`[^`]+`|\\[[^]]+]|[A-Za-z_]\\w*)";
    private static final String QUAL_IDENT =
            "(" + SEG + "(\\." + SEG + ")*)";

    private static final String TABLES_IDENT =
            "(tables|\"tables\"|`tables`|\\[tables])";

    private static final Pattern P_LIST_TABLES =
            Pattern.compile("^\\s*select\\s+\\*\\s+from\\s+" + TABLES_IDENT + "\\s*(?<tail>.*)$",
                    Pattern.CASE_INSENSITIVE);

    private static final Pattern P_SELECT_TOP_ALL =
            Pattern.compile("^\\s*select\\s+top\\s+(?<top>\\d+)\\s+\\*\\s+from\\s+(?<table>" + QUAL_IDENT + ")\\s*(?<tail>.*)$",
                    Pattern.CASE_INSENSITIVE);

    private static final Pattern P_COUNT =
            Pattern.compile("^\\s*select\\s+count\\s*\\(\\s*(?<arg>[*1])\\s*\\)\\s+from\\s+(?<table>" + QUAL_IDENT + ")\\s*(?<tail>.*)$",
                    Pattern.CASE_INSENSITIVE);

    private static final Pattern P_SELECT_ALL =
            Pattern.compile("^\\s*select\\s+\\*\\s+from\\s+(?<table>" + QUAL_IDENT + ")\\s*(?<tail>.*)$",
                    Pattern.CASE_INSENSITIVE);

    private static final Pattern P_LIMIT =
            Pattern.compile("^\\s*limit\\s+(\\d+)\\s*$", Pattern.CASE_INSENSITIVE);

    private static final Pattern P_FETCH_FIRST =
            Pattern.compile("^\\s*fetch\\s+first\\s+(\\d+)\\s+rows\\s+only\\s*$", Pattern.CASE_INSENSITIVE);

    public static Parsed parse(String sql) throws SQLException {
        if (sql == null) throw new SQLException("SQL is null");

        String s = normalizeSql(sql);
        if (s.isEmpty()) throw new SQLException("Empty SQL");

        // SELECT TOP n * FROM table ...
        {
            Matcher m = P_SELECT_TOP_ALL.matcher(s);
            if (m.matches()) {
                String table = normalizeTableRef(m.group("table"));
                int top = parsePositiveInt(m.group("top"));
                TailParts tp = splitWhereAndTail(m.group("tail"));
                int tailLimit = parseLimitFromTail(tp.tailSql);
                int eff = combineLimits(top, tailLimit);
                return new Parsed(Kind.SELECT_ALL, table, tp.whereSql, eff);
            }
        }

        // SELECT * FROM tables ...
        {
            Matcher m = P_LIST_TABLES.matcher(s);
            if (m.matches()) {
                TailParts tp = splitWhereAndTail(m.group("tail"));
                if (tp.whereSql != null) throw new SQLException("WHERE not supported for pseudo-table TABLES");
                int lim = parseLimitFromTail(tp.tailSql);
                return new Parsed(Kind.LIST_TABLES, null, null, lim);
            }
        }

        // SELECT COUNT(*) FROM table ...
        {
            Matcher m = P_COUNT.matcher(s);
            if (m.matches()) {
                String table = normalizeTableRef(m.group("table"));
                TailParts tp = splitWhereAndTail(m.group("tail"));
                int lim = parseLimitFromTail(tp.tailSql);
                return new Parsed(Kind.COUNT, table, tp.whereSql, lim);
            }
        }

        // SELECT * FROM table ...
        {
            Matcher m = P_SELECT_ALL.matcher(s);
            if (m.matches()) {
                String table = normalizeTableRef(m.group("table"));
                TailParts tp = splitWhereAndTail(m.group("tail"));
                int lim = parseLimitFromTail(tp.tailSql);
                return new Parsed(Kind.SELECT_ALL, table, tp.whereSql, lim);
            }
        }

        throw new SQLException("Unsupported SQL: " + sql);
    }

    /**
     * @param whereSql may be null
     * @param tailSql  may be ""
     */ // -------- tail parsing: [WHERE ...] [LIMIT/FETCH ...]
        private record TailParts(String whereSql, String tailSql) {
            private TailParts(String whereSql, String tailSql) {
                this.whereSql = (whereSql == null || whereSql.isBlank()) ? null : whereSql.trim();
                this.tailSql = (tailSql == null) ? "" : tailSql.trim();
            }
        }

    private static TailParts splitWhereAndTail(String tail) throws SQLException {
        if (tail == null) return new TailParts(null, "");
        String t = normalizeSql(tail).trim();
        if (t.isEmpty()) return new TailParts(null, "");

        if (!startsWithWord(t, "where")) {
            return new TailParts(null, t);
        }

        int i = skipWord(t, 0, "where");
        String rest = t.substring(i).trim();
        if (rest.isEmpty()) throw new SQLException("WHERE without expression");

        int split = findLimitKeywordOutsideQuotes(rest);
        if (split < 0) {
            return new TailParts(rest, "");
        }
        String whereSql = rest.substring(0, split).trim();
        String tailSql = rest.substring(split).trim();
        return new TailParts(whereSql, tailSql);
    }

    private static int findLimitKeywordOutsideQuotes(String s) {
        boolean inSingle = false;
        boolean inDouble = false;
        boolean inBacktick = false;
        boolean inBracket = false;

        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);

            if (!inDouble && !inBacktick && !inBracket && c == '\'') {
                if (inSingle && i + 1 < s.length() && s.charAt(i + 1) == '\'') { i++; continue; }
                inSingle = !inSingle;
                continue;
            }
            if (!inSingle && !inBacktick && !inBracket && c == '"') { inDouble = !inDouble; continue; }
            if (!inSingle && !inDouble && !inBracket && c == '`') { inBacktick = !inBacktick; continue; }
            if (!inSingle && !inDouble && !inBacktick) {
                if (c == '[') { inBracket = true; continue; }
                if (c == ']') { inBracket = false; continue; }
            }

            if (inSingle || inDouble || inBacktick || inBracket) continue;

            if (startsWithWordAt(s, i, "limit")) return i;
            if (startsWithWordAt(s, i, "fetch")) return i;
        }
        return -1;
    }

    // -------- limit parsing
    private static int parseLimitFromTail(String tail) throws SQLException {
        if (tail == null) return 0;
        String t = normalizeSql(tail).trim();
        if (t.isEmpty()) return 0;

        Matcher ml = P_LIMIT.matcher(t);
        if (ml.matches()) return parsePositiveInt(ml.group(1));

        Matcher mf = P_FETCH_FIRST.matcher(t);
        if (mf.matches()) return parsePositiveInt(mf.group(1));

        throw new SQLException("Unsupported SQL tail: " + tail);
    }

    private static int combineLimits(int a, int b) {
        if (a > 0 && b > 0) return Math.min(a, b);
        if (a > 0) return a;
        return b;
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

    // -------- identifiers
    private static String normalizeTableRef(String ident) throws SQLException {
        String q = normalizeQualifiedIdent(ident);
        int dot = q.lastIndexOf('.');
        return (dot >= 0) ? q.substring(dot + 1) : q;
    }

    private static String normalizeQualifiedIdent(String ident) throws SQLException {
        if (ident == null) return null;
        String s = ident.trim();
        if (s.isEmpty()) return s;

        StringBuilder out = new StringBuilder(s.length());
        int i = 0;
        boolean first = true;

        while (i < s.length()) {
            if (!first) {
                if (s.charAt(i) != '.') throw new SQLException("Bad qualified identifier: " + ident);
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
                int j = i;
                while (j < s.length() && s.charAt(j) != '.') j++;
                out.append(s, i, j);
                i = j;
            }
        }

        return out.toString();
    }

    // -------- keywords
    private static boolean startsWithWord(String s, String word) {
        return startsWithWordAt(s, 0, word);
    }

    private static boolean startsWithWordAt(String s, int pos, String word) {
        int n = s.length();
        int w = word.length();
        if (pos < 0 || pos + w > n) return false;
        if (!s.regionMatches(true, pos, word, 0, w)) return false;

        int before = pos - 1;
        int after = pos + w;

        if (before >= 0 && Character.isLetterOrDigit(s.charAt(before))) return false;
        if (after < n && Character.isLetterOrDigit(s.charAt(after))) return false;
        return true;
    }

    private static int skipWord(String s, int pos, String word) throws SQLException {
        if (!startsWithWordAt(s, pos, word)) throw new SQLException("Expected keyword: " + word);
        return pos + word.length();
    }

    // -------- normalization
    private static String normalizeSql(String in) {
        if (in == null) return "";
        String s = in.trim();
        s = stripTrailingSemicolon(s);
        return s.trim();
    }

    private static String stripTrailingSemicolon(String s) {
        int i = s.length() - 1;
        while (i >= 0 && Character.isWhitespace(s.charAt(i))) i--;
        if (i >= 0 && s.charAt(i) == ';') return s.substring(0, i);
        return s;
    }
}
