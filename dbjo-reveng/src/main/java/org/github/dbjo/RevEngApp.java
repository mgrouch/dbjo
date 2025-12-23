package org.github.dbjo;

import java.sql.*;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public final class RevEngApp {

    private static final String JDBC_URL = "jdbc:hsqldb:mem:dbjo"; // keep simple; no shutdown=true
    private static final String USER = "SA";
    private static final String PASS = "";

    public static void main(String[] args) throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL, USER, PASS)) {
            conn.setAutoCommit(true);
            printSchema(conn, "PUBLIC"); // HSQLDB default schema is PUBLIC
        }
    }

    private static void printSchema(Connection conn, String schema) throws SQLException {
        DatabaseMetaData meta = conn.getMetaData();

        List<String> tables = new ArrayList<>();
        try (ResultSet rs = meta.getTables(null, schema, "%", new String[]{"TABLE"})) {
            while (rs.next()) tables.add(rs.getString("TABLE_NAME"));
        }

        // Fallback if schema filter didn’t work for some reason
        if (tables.isEmpty()) {
            try (ResultSet rs = meta.getTables(null, null, "%", new String[]{"TABLE"})) {
                while (rs.next()) {
                    String s = rs.getString("TABLE_SCHEM");
                    if (schema == null || schema.equalsIgnoreCase(s)) {
                        tables.add(rs.getString("TABLE_NAME"));
                    }
                }
            }
        }

        tables.sort(String.CASE_INSENSITIVE_ORDER);

        System.out.println("Tables in schema " + schema + ":");
        for (String table : tables) {
            System.out.println();
            System.out.println(table);

            List<Col> cols = new ArrayList<>();
            try (ResultSet crs = meta.getColumns(null, schema, table, "%")) {
                while (crs.next()) {
                    String colName = crs.getString("COLUMN_NAME");
                    String typeName = crs.getString("TYPE_NAME");
                    int size = crs.getInt("COLUMN_SIZE");
                    int scale = crs.getInt("DECIMAL_DIGITS");
                    int nullable = crs.getInt("NULLABLE");
                    int pos = crs.getInt("ORDINAL_POSITION");

                    cols.add(new Col(colName, typeName, size, scale, nullable, pos));
                }
            }

            cols.sort(Comparator.comparingInt(c -> c.pos));
            for (Col c : cols) {
                String type = formatType(c.typeName, c.size, c.scale);
                String nulls = (c.nullable == DatabaseMetaData.columnNoNulls) ? "NOT NULL" : "NULL";
                System.out.printf("  - %-30s %-20s %s%n", c.name, type, nulls);
            }
        }
    }

    private static String formatType(String typeName, int size, int scale) {
        String t = typeName == null ? "UNKNOWN" : typeName.toUpperCase();

        // keep it readable; add (size[,scale]) for common cases
        if (t.contains("CHAR") || t.contains("BINARY") || t.contains("VAR")) {
            return t + "(" + size + ")";
        }
        if (t.contains("DECIMAL") || t.contains("NUMERIC")) {
            return t + "(" + size + "," + scale + ")";
        }
        return t;
    }

    private record Col(String name, String typeName, int size, int scale, int nullable, int pos) {}
}
