package org.github.dbjo.rdb.jdbc.catalog;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Small SQL parser for the read-only Rocks JDBC driver.
 *
 * Supported:
 *  - select * from tables [limit N | fetch first N rows only]
 *  - select * from <table> [where <expr>] [limit N | fetch first N rows only]
 *  - select top N <projection> from <table> [where <expr>] [limit M]   (effective limit = min(top, limit) if both)
 *  - select <projection> from <table> [where <expr>] [limit N | fetch first N rows only]
 *  - select count(*) from <table> [where <expr>]
 *  - select count(1) from <table> [where <expr>]
 *
 * Projection:
 *  - "*" OR "col1, col2, ..." (each col can be qualified and/or quoted)
 *  - optional alias: "col as alias" OR "col alias"
 */
public final class RocksJdbcSql {
    private RocksJdbcSql() {}

    public enum Kind { LIST_TABLES, SELECT, COUNT }

    public record SelectedCol(String sourceName, String label) {}

    /** limit==0 means "no SQL limit"; projection==null means "*" */
    public record Parsed(Kind kind, String tableName, String whereSql, int limit, SelectedCol[] projection) {}

    // segment = "x" | `x` | [x] | bareword; allow qualified seg(.seg)*
    private static final String SEG =
            "(\"[^\"]+\"|`[^`]+`|\\[[^\\]]+\\]|[A-Za-z_][A-Za-z0-9_]*)";
    private static final String QUAL_IDENT =
            "(" + SEG + "(\\." + SEG + ")*)";

    // allow quoted "tables" too so tooling can query it
    private static final String TABLES_IDENT =
            "(tables|\"tables\"|`tables`|\\[tables\\])";

    private static final Pattern P_LIST_TABLES =
            Pattern.compile("^\\s*select\\s+\\*\\s+from\\s+" + TABLES_IDENT + "\\s*(?<tail>.*)$",
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

        // SELECT * FROM tables ...
        {
            Matcher m = P_LIST_TABLES.matcher(s);
            if (m.matches()) {
                TailParts tp = splitWhereAndTail(m.group("tail"));
                if (tp.whereSql != null) throw new SQLException("WHERE not supported for pseudo-table TABLES");
                int lim = parseLimitFromTail(tp.tailSql);
                return new Parsed(Kind.LIST_TABLES, null, null, lim, null);
            }
        }

        // SELECT COUNT(*) / COUNT(1) FROM table ...
        {
            Matcher m = P_COUNT.matcher(s);
            if (m.matches()) {
                String table = normalizeTableRef(m.group("table"));
                TailParts tp = splitWhereAndTail(m.group("tail"));
                int lim = parseLimitFromTail(tp.tailSql); // accepted (ignored by execution)
                return new Parsed(Kind.COUNT, table, tp.whereSql, lim, null);
            }
        }

        // SELECT [TOP n] <projection> FROM <table> ...
        return parseSelectWithProjection(s);
    }

    private static Parsed parseSelectWithProjection(String sql) throws SQLException {
        String s = sql.trim();
        if (!startsWithWord(s, "select")) throw new SQLException("Unsupported SQL: " + sql);
        int pos = skipWord(s, 0, "select");
        pos = skipWs(s, pos);

        // Optional TOP n
        int topLimit = 0;
        if (startsWithWordAt(s, pos, "top")) {
            pos = skipWord(s, pos, "top");
            pos = skipWs(s, pos);
            int j = pos;
            while (j < s.length() && Character.isDigit(s.charAt(j))) j++;
            if (j == pos) throw new SQLException("TOP without number");
            topLimit = parsePositiveInt(s.substring(pos, j));
            pos = skipWs(s, j);
        }

        // Find FROM keyword outside quotes
        int fromPos = findKeywordOutsideQuotes(s, pos, "from");
        if (fromPos < 0) throw new SQLException("Missing FROM");
        String selectPart = s.substring(pos, fromPos).trim();
        if (selectPart.isEmpty()) throw new SQLException("Missing projection in SELECT");

        int afterFrom = skipWord(s, fromPos, "from");
        afterFrom = skipWs(s, afterFrom);

        // Parse table identifier token
        IdentTok tableTok = readQualifiedIdentToken(s, afterFrom);
        String table = normalizeTableRef(tableTok.text);

        String tail = s.substring(tableTok.end).trim();

        TailParts tp = splitWhereAndTail(tail);
        int tailLimit = parseLimitFromTail(tp.tailSql);
        int effLimit = combineLimits(topLimit, tailLimit);

        SelectedCol[] proj = parseProjection(selectPart);

        return new Parsed(Kind.SELECT, table, tp.whereSql, effLimit, proj);
    }

    private static SelectedCol[] parseProjection(String selectPart) throws SQLException {
        String p = selectPart.trim();
        if (p.equals("*")) return null;

        // split by commas outside quotes/brackets/strings
        List<String> items = splitCsvOutsideQuotes(p);
        List<SelectedCol> out = new ArrayList<>();

        for (String raw : items) {
            String it = raw.trim();
            if (it.isEmpty()) continue;

            ColWithAlias ca = parseColWithOptionalAlias(it);

            String sourceNorm = normalizeQualifiedIdent(ca.colIdent);
            String base = baseName(sourceNorm);

            String label;
            if (ca.aliasIdent != null && !ca.aliasIdent.isBlank()) {
                label = unquoteIdent(ca.aliasIdent.trim());
            } else {
                label = base;
            }

            out.add(new SelectedCol(base, label));
        }

        if (out.isEmpty()) throw new SQLException("Empty projection list");
        return out.toArray(new SelectedCol[0]);
    }

    private record ColWithAlias(String colIdent, String aliasIdent) {}

    private static ColWithAlias parseColWithOptionalAlias(String item) throws SQLException {
        // Parse: <ident> [AS] <alias>
        // Where <ident> can be qualified/quoted, and alias is single ident token (quoted ok).
        IdentTok col = readQualifiedIdentToken(item, 0);
        int pos = skipWs(item, col.end);
        if (pos >= item.length()) return new ColWithAlias(col.text, null);

        // Optional AS
        if (startsWithWordAt(item, pos, "as")) {
            pos = skipWord(item, pos, "as");
            pos = skipWs(item, pos);
        }

        if (pos >= item.length()) throw new SQLException("Bad alias in projection: " + item);

        IdentTok alias = readSingleIdentToken(item, pos);
        pos = skipWs(item, alias.end);

        if (pos != item.length()) {
            // extra tokens => not supported (keeps it strict)
            throw new SQLException("Unsupported projection item: " + item);
        }

        return new ColWithAlias(col.text, alias.text);
    }

    // -------- tail parsing: [WHERE ...] [LIMIT/FETCH ...]
    private static final class TailParts {
        final String whereSql; // may be null
        final String tailSql;  // may be ""
        TailParts(String whereSql, String tailSql) {
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

    // -------- identifiers (normalize + keep last segment only for tables)
    private static String normalizeTableRef(String ident) throws SQLException {
        String q = normalizeQualifiedIdent(ident);
        return baseName(q);
    }

    private static String baseName(String qualified) {
        int dot = qualified.lastIndexOf('.');
        return (dot >= 0) ? qualified.substring(dot + 1) : qualified;
    }

    private static String unquoteIdent(String ident) {
        String s = ident.trim();
        if (s.startsWith("\"") && s.endsWith("\"") && s.length() >= 2) return s.substring(1, s.length() - 1);
        if (s.startsWith("`") && s.endsWith("`") && s.length() >= 2) return s.substring(1, s.length() - 1);
        if (s.startsWith("[") && s.endsWith("]") && s.length() >= 2) return s.substring(1, s.length() - 1);
        return s;
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

    // -------- keyword scanning / helpers
    private static boolean startsWithWord(String s, String word) { return startsWithWordAt(s, 0, word); }

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

    private static int skipWs(String s, int pos) {
        int i = pos;
        while (i < s.length() && Character.isWhitespace(s.charAt(i))) i++;
        return i;
    }

    private static int findKeywordOutsideQuotes(String s, int start, String word) {
        boolean inSingle = false;
        boolean inDouble = false;
        boolean inBacktick = false;
        boolean inBracket = false;

        for (int i = start; i < s.length(); i++) {
            char c = s.charAt(i);

            if (!inDouble && !inBacktick && !inBracket && c == '\'') {
                if (inSingle && i + 1 < s.length() && s.charAt(i + 1) == '\'') { i++; continue; }
                inSingle = !inSingle; continue;
            }
            if (!inSingle && !inBacktick && !inBracket && c == '"') { inDouble = !inDouble; continue; }
            if (!inSingle && !inDouble && !inBracket && c == '`') { inBacktick = !inBacktick; continue; }
            if (!inSingle && !inDouble && !inBacktick) {
                if (c == '[') { inBracket = true; continue; }
                if (c == ']') { inBracket = false; continue; }
            }

            if (inSingle || inDouble || inBacktick || inBracket) continue;

            if (startsWithWordAt(s, i, word)) return i;
        }
        return -1;
    }

    // -------- CSV splitting for projection list
    private static List<String> splitCsvOutsideQuotes(String s) {
        List<String> out = new ArrayList<>();
        boolean inSingle = false, inDouble = false, inBacktick = false, inBracket = false;
        int last = 0;

        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);

            if (!inDouble && !inBacktick && !inBracket && c == '\'') {
                if (inSingle && i + 1 < s.length() && s.charAt(i + 1) == '\'') { i++; continue; }
                inSingle = !inSingle; continue;
            }
            if (!inSingle && !inBacktick && !inBracket && c == '"') { inDouble = !inDouble; continue; }
            if (!inSingle && !inDouble && !inBracket && c == '`') { inBacktick = !inBacktick; continue; }
            if (!inSingle && !inDouble && !inBacktick) {
                if (c == '[') { inBracket = true; continue; }
                if (c == ']') { inBracket = false; continue; }
            }

            if (inSingle || inDouble || inBacktick || inBracket) continue;

            if (c == ',') {
                out.add(s.substring(last, i));
                last = i + 1;
            }
        }
        out.add(s.substring(last));
        return out;
    }

    // -------- reading identifier tokens
    private record IdentTok(String text, int end) {}

    private static IdentTok readQualifiedIdentToken(String s, int start) throws SQLException {
        int i = skipWs(s, start);
        if (i >= s.length()) throw new SQLException("Expected identifier");

        StringBuilder out = new StringBuilder();
        IdentTok seg = readSingleIdentToken(s, i);
        out.append(seg.text);
        i = seg.end;

        while (true) {
            i = skipWs(s, i);
            if (i < s.length() && s.charAt(i) == '.') {
                out.append('.');
                i++;
                IdentTok seg2 = readSingleIdentToken(s, i);
                out.append(seg2.text);
                i = seg2.end;
            } else break;
        }
        return new IdentTok(out.toString(), i);
    }

    private static IdentTok readSingleIdentToken(String s, int start) throws SQLException {
        int i = skipWs(s, start);
        if (i >= s.length()) throw new SQLException("Expected identifier");

        char c = s.charAt(i);

        if (c == '"') {
            int j = s.indexOf('"', i + 1);
            if (j < 0) throw new SQLException("Unterminated quoted identifier");
            return new IdentTok(s.substring(i, j + 1), j + 1);
        }
        if (c == '`') {
            int j = s.indexOf('`', i + 1);
            if (j < 0) throw new SQLException("Unterminated quoted identifier");
            return new IdentTok(s.substring(i, j + 1), j + 1);
        }
        if (c == '[') {
            int j = s.indexOf(']', i + 1);
            if (j < 0) throw new SQLException("Unterminated bracket identifier");
            return new IdentTok(s.substring(i, j + 1), j + 1);
        }

        int j = i;
        while (j < s.length()) {
            char x = s.charAt(j);
            if (Character.isLetterOrDigit(x) || x == '_') j++;
            else break;
        }
        if (j == i) throw new SQLException("Bad identifier token");
        return new IdentTok(s.substring(i, j), j);
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
