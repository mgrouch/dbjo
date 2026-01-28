package org.github.dbjo.rdb.jdbc.catalog;

import java.sql.SQLException;

public final class RocksJdbcSql {
    private RocksJdbcSql() {}

    public enum Kind { LIST_TABLES, SELECT_ALL, COUNT }

    public record Parsed(Kind kind, String tableName, Integer limit) {}

    public static Parsed parse(String sql) throws SQLException {
        if (sql == null) throw new SQLException("SQL is null");
        String s = stripTrailingSemicolon(sql.trim());
        if (s.isEmpty()) throw new SQLException("SQL is empty");

        Cursor c = new Cursor(s);

        c.skipWs();
        c.expectKeyword("select");
        c.skipWs();

        boolean isCount = false;

        if (c.tryConsume('*')) {
            // ok
        } else if (c.tryKeyword("count")) {
            c.skipWs();
            c.expect('(');
            c.skipWs();
            c.expect('*');
            c.skipWs();
            c.expect(')');
            isCount = true;
        } else {
            throw new SQLException("Unsupported SELECT list (only * or count(*)): " + sql);
        }

        c.skipWs();
        c.expectKeyword("from");
        c.skipWs();

        String rawId = c.readIdentifier();
        if (rawId == null || rawId.isBlank()) throw new SQLException("Missing table name: " + sql);

        String table = lastSegment(rawId);

        c.skipWs();

        Integer limit = null;
        // LIMIT n
        if (c.tryKeyword("limit")) {
            c.skipWs();
            limit = c.readInt();
            if (limit == null) throw new SQLException("Bad LIMIT: " + sql);
            c.skipWs();
        } else if (c.tryKeyword("fetch")) {
            // FETCH FIRST n ROWS ONLY
            c.skipWs();
            c.expectKeyword("first");
            c.skipWs();
            limit = c.readInt();
            if (limit == null) throw new SQLException("Bad FETCH FIRST: " + sql);
            c.skipWs();
            c.expectKeyword("rows");
            c.skipWs();
            c.expectKeyword("only");
            c.skipWs();
        }

        c.skipWs();
        if (!c.eof()) {
            throw new SQLException("Trailing SQL not supported: " + sql);
        }

        if (!isCount) {
            if ("tables".equalsIgnoreCase(table)) return new Parsed(Kind.LIST_TABLES, null, null);
            return new Parsed(Kind.SELECT_ALL, table, limit);
        } else {
            return new Parsed(Kind.COUNT, table, null);
        }
    }

    private static String stripTrailingSemicolon(String s) {
        String t = s;
        while (t.endsWith(";")) t = t.substring(0, t.length() - 1).trim();
        return t;
    }

    private static String lastSegment(String id) {
        String x = id.trim();
        int dot = x.lastIndexOf('.');
        if (dot >= 0 && dot + 1 < x.length()) return x.substring(dot + 1);
        return x;
    }

    private static final class Cursor {
        private final String s;
        private int i = 0;

        Cursor(String s) { this.s = s; }

        boolean eof() { return i >= s.length(); }

        void skipWs() {
            while (!eof()) {
                char ch = s.charAt(i);
                if (ch == ' ' || ch == '\t' || ch == '\r' || ch == '\n') i++;
                else break;
            }
        }

        boolean tryConsume(char ch) {
            if (!eof() && s.charAt(i) == ch) { i++; return true; }
            return false;
        }

        void expect(char ch) throws SQLException {
            if (eof() || s.charAt(i) != ch) throw new SQLException("Expected '" + ch + "' at pos " + i + " in: " + s);
            i++;
        }

        boolean tryKeyword(String kw) {
            int save = i;
            try {
                expectKeyword(kw);
                return true;
            } catch (SQLException e) {
                i = save;
                return false;
            }
        }

        void expectKeyword(String kw) throws SQLException {
            skipWs();
            int n = kw.length();
            if (i + n > s.length()) throw new SQLException("Expected keyword " + kw + " in: " + s);
            String sub = s.substring(i, i + n);
            if (!sub.equalsIgnoreCase(kw)) throw new SQLException("Expected keyword " + kw + " at pos " + i + " in: " + s);

            // keyword boundary
            int j = i + n;
            if (j < s.length()) {
                char ch = s.charAt(j);
                if (Character.isLetterOrDigit(ch) || ch == '_') {
                    throw new SQLException("Expected keyword boundary for " + kw + " at pos " + i + " in: " + s);
                }
            }
            i += n;
        }

        String readIdentifier() throws SQLException {
            skipWs();
            if (eof()) return null;

            char ch = s.charAt(i);

            // quoted identifier: "MyTable" or `MyTable`
            if (ch == '"' || ch == '`') {
                i++;
                StringBuilder sb = new StringBuilder();
                while (!eof()) {
                    char c = s.charAt(i++);
                    if (c == ch) {
                        // allow doubled quote escape: "" => "
                        if (!eof() && s.charAt(i) == ch) {
                            sb.append(ch);
                            i++;
                            continue;
                        }
                        return sb.toString();
                    }
                    sb.append(c);
                }
                throw new SQLException("Unterminated quoted identifier in: " + s);
            }

            // unquoted: allow schema.table and underscores
            int start = i;
            while (!eof()) {
                char c = s.charAt(i);
                if (Character.isLetterOrDigit(c) || c == '_' || c == '.') {
                    i++;
                } else {
                    break;
                }
            }
            if (i == start) return null;
            return s.substring(start, i);
        }

        Integer readInt() {
            skipWs();
            int start = i;
            while (!eof() && Character.isDigit(s.charAt(i))) i++;
            if (i == start) return null;
            try {
                return Integer.parseInt(s.substring(start, i));
            } catch (NumberFormatException e) {
                return null;
            }
        }
    }
}
