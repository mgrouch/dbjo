package org.github.dbjo.rdb.jdbc.catalog;

import java.sql.SQLException;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class RocksJdbcSql {
    private RocksJdbcSql() {}

    public enum Kind { LIST_TABLES, SELECT_ALL, COUNT }

    public record Parsed(Kind kind, String tableName) {}

    private static final Pattern P_LIST_TABLES =
            Pattern.compile("^\\s*select\\s+\\*\\s+from\\s+tables\\s*;?\\s*$", Pattern.CASE_INSENSITIVE);

    private static final Pattern P_SELECT_ALL =
            Pattern.compile("^\\s*select\\s+\\*\\s+from\\s+([A-Za-z0-9_]+)\\s*;?\\s*$", Pattern.CASE_INSENSITIVE);

    private static final Pattern P_COUNT =
            Pattern.compile("^\\s*select\\s+count\\s*\\(\\s*\\*\\s*\\)\\s+from\\s+([A-Za-z0-9_]+)\\s*;?\\s*$",
                    Pattern.CASE_INSENSITIVE);

    public static Parsed parse(String sql) throws SQLException {
        if (sql == null) throw new SQLException("SQL is null");
        String s = sql.trim();
        if (s.isEmpty()) throw new SQLException("SQL is empty");

        if (P_LIST_TABLES.matcher(s).matches()) {
            return new Parsed(Kind.LIST_TABLES, null);
        }

        Matcher mc = P_COUNT.matcher(s);
        if (mc.matches()) {
            String t = mc.group(1);
            return new Parsed(Kind.COUNT, t);
        }

        Matcher ma = P_SELECT_ALL.matcher(s);
        if (ma.matches()) {
            String t = ma.group(1);
            if ("tables".equalsIgnoreCase(t)) {
                return new Parsed(Kind.LIST_TABLES, null);
            }
            return new Parsed(Kind.SELECT_ALL, t);
        }

        String hint = s.toLowerCase(Locale.ROOT).startsWith("select")
                ? "Supported: SELECT * FROM tables | SELECT * FROM <table> | SELECT COUNT(*) FROM <table>"
                : "Only SELECT is supported";
        throw new SQLException("Unsupported SQL: " + sql + " (" + hint + ")");
    }
}
