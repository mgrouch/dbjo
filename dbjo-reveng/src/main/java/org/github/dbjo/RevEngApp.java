package org.github.dbjo;

import java.sql.*;
import java.util.*;

public final class RevEngApp {

    // Default: embedded in-memory DB. Override via args (recommended for real use).
    // Example for server: jdbc:hsqldb:hsql://localhost:9001/dbjo
    private static final String DEFAULT_URL  = "jdbc:hsqldb:mem:dbjo";
    private static final String DEFAULT_USER = "SA";
    private static final String DEFAULT_PASS = "";

    public static void main(String[] args) throws Exception {
        String url  = args.length > 0 ? args[0] : DEFAULT_URL;
        String user = args.length > 1 ? args[1] : DEFAULT_USER;
        String pass = args.length > 2 ? args[2] : DEFAULT_PASS;

        try (Connection conn = DriverManager.getConnection(url, user, pass)) {
            printAllTablesAndColumns(conn);
        }
    }

    private static void printAllTablesAndColumns(Connection conn) throws SQLException {
        DatabaseMetaData meta = conn.getMetaData();

        // Collect schemas first (lets us print per-schema cleanly)
        List<String> schemas = new ArrayList<>();
        try (ResultSet rs = meta.getSchemas()) {
            while (rs.next()) {
                String s = rs.getString("TABLE_SCHEM");
                if (s != null) schemas.add(s);
            }
        }
        schemas.sort(String.CASE_INSENSITIVE_ORDER);

        // If driver returns no schemas, fall back to "PUBLIC" (HSQLDB default)
        if (schemas.isEmpty()) schemas.add("PUBLIC");

        for (String schema : schemas) {
            if (isSystemSchema(schema)) continue;

            List<String> tables = new ArrayList<>();
            try (ResultSet rs = meta.getTables(null, schema, "%", new String[] {"TABLE"})) {
                while (rs.next()) {
                    tables.add(rs.getString("TABLE_NAME"));
                }
            }

            tables.sort(String.CASE_INSENSITIVE_ORDER);
            if (tables.isEmpty()) continue;

            System.out.println();
            System.out.println("Schema: " + schema);

            for (String table : tables) {
                Set<String> pkCols = getPrimaryKeyColumns(meta, schema, table);

                List<Column> cols = new ArrayList<>();
                try (ResultSet crs = meta.getColumns(null, schema, table, "%")) {
                    while (crs.next()) {
                        cols.add(new Column(
                                crs.getInt("ORDINAL_POSITION"),
                                crs.getString("COLUMN_NAME"),
                                crs.getString("TYPE_NAME"),
                                crs.getInt("COLUMN_SIZE"),
                                crs.getInt("DECIMAL_DIGITS"),
                                crs.getInt("NULLABLE"),
                                safeGet(crs, "IS_AUTOINCREMENT"),
                                safeGet(crs, "COLUMN_DEF")
                        ));
                    }
                }

                cols.sort(Comparator.comparingInt(c -> c.pos));

                System.out.println();
                System.out.println("  " + table);
                for (Column c : cols) {
                    String type = formatType(c.typeName, c.size, c.scale);
                    boolean notNull = (c.nullable == DatabaseMetaData.columnNoNulls);
                    boolean isPk = pkCols.contains(c.name.toUpperCase(Locale.ROOT));
                    boolean isAi = "YES".equalsIgnoreCase(c.isAutoIncrement);

                    System.out.printf(
                            "    - %-30s %-22s %s%s%s%n",
                            c.name,
                            type,
                            notNull ? "NOT NULL" : "NULL",
                            isPk ? "  PK" : "",
                            isAi ? "  AI" : ""
                    );

                    // If you want defaults printed too, uncomment:
                    // if (c.defaultValue != null) System.out.println("        default: " + c.defaultValue);
                }
            }
        }
    }

    private static Set<String> getPrimaryKeyColumns(DatabaseMetaData meta, String schema, String table)
            throws SQLException {
        Set<String> pk = new HashSet<>();
        try (ResultSet rs = meta.getPrimaryKeys(null, schema, table)) {
            while (rs.next()) {
                String col = rs.getString("COLUMN_NAME");
                if (col != null) pk.add(col.toUpperCase(Locale.ROOT));
            }
        }
        return pk;
    }

    private static boolean isSystemSchema(String schema) {
        String s = schema.toUpperCase(Locale.ROOT);
        // HSQLDB commonly has INFORMATION_SCHEMA; other DBs have additional system schemas.
        return s.equals("INFORMATION_SCHEMA")
                || s.startsWith("SYSTEM")
                || s.startsWith("SYS");
    }

    private static String formatType(String typeName, int size, int scale) {
        if (typeName == null) return "UNKNOWN";
        String t = typeName.toUpperCase(Locale.ROOT);

        // Add (size) for character-like types
        if (t.contains("CHAR") || t.contains("BINARY") || t.contains("VAR")) {
            return t + "(" + size + ")";
        }

        // Add (precision,scale) for numeric precision types
        if (t.contains("DECIMAL") || t.contains("NUMERIC")) {
            return t + "(" + size + "," + scale + ")";
        }

        return t;
    }

    private static String safeGet(ResultSet rs, String col) {
        try {
            return rs.getString(col);
        } catch (SQLException ignored) {
            return null;
        }
    }

    private record Column(
            int pos,
            String name,
            String typeName,
            int size,
            int scale,
            int nullable,
            String isAutoIncrement,
            String defaultValue
    ) {}
}
