package org.github.dbjo;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.sql.*;
import java.util.*;

/**
 * Connects to a running HSQLDB (server) and generates Java beans (POJOs)
 * for each user table into: target/dbjo-generated
 *
 * Default URL assumes HSQLDB Server:
 *   jdbc:hsqldb:hsql://localhost:9001/dbjo
 *
 * Run:
 *   mvn -q exec:java -Dexec.mainClass=org.github.dbjo.DbBeanGenerator
 * Or override:
 *   mvn -q exec:java -Dexec.mainClass=org.github.dbjo.DbBeanGenerator \
 *     -Dexec.args="jdbc:hsqldb:hsql://localhost:9001/dbjo SA"
 */
public final class DbBeanGenerator {

    // Existing running DB on localhost:
    private static final String DEFAULT_URL  = "jdbc:hsqldb:hsql://localhost:9001/dbjo";
    private static final String DEFAULT_USER = "SA";
    private static final String DEFAULT_PASS = "";

    // Where to generate:
    private static final Path OUT_BASE = Paths.get("target", "dbjo-generated");

    // Package for generated beans:
    private static final String GEN_PACKAGE = "org.github.dbjo.generated";

    public static void main(String[] args) throws Exception {
        String url  = args.length > 0 ? args[0] : DEFAULT_URL;
        String user = args.length > 1 ? args[1] : DEFAULT_USER;
        String pass = args.length > 2 ? args[2] : DEFAULT_PASS;

        // Make driver load explicit (clearer error if dependency missing)
        Class.forName("org.hsqldb.jdbc.JDBCDriver");

        try (Connection conn = DriverManager.getConnection(url, user, pass)) {
            DatabaseMetaData md = conn.getMetaData();
            System.out.println("Connected:");
            System.out.println("  url     = " + url);
            System.out.println("  db      = " + md.getDatabaseProductName() + " " + md.getDatabaseProductVersion());
            System.out.println("  driver  = " + md.getDriverName() + " " + md.getDriverVersion());
            System.out.println();

            int generated = generateBeans(conn);
            System.out.println("Generated " + generated + " bean(s) under: " + OUT_BASE.toAbsolutePath());
        }
    }

    private static int generateBeans(Connection conn) throws SQLException, IOException {
        DatabaseMetaData meta = conn.getMetaData();

        List<TableRef> tables = listUserTables(meta);
        if (tables.isEmpty()) {
            System.out.println("No user tables found (non-system schemas). Nothing to generate.");
            return 0;
        }

        // target/dbjo-generated/org/github/dbjo/generated
        Path pkgDir = OUT_BASE.resolve(GEN_PACKAGE.replace('.', '/'));
        Files.createDirectories(pkgDir);

        int count = 0;
        for (TableRef t : tables) {
            List<Col> cols = listColumns(meta, t.schema, t.table);
            if (cols.isEmpty()) continue;

            Set<String> pkCols = getPrimaryKeyColumns(meta, t.schema, t.table);

            String className = toClassName(t.table);
            String java = renderBeanSource(GEN_PACKAGE, className, t, cols, pkCols);

            Path outFile = pkgDir.resolve(className + ".java");
            Files.writeString(outFile, java, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);

            System.out.println("Wrote: " + outFile);
            count++;
        }
        return count;
    }

    // -------------------- Metadata reading --------------------

    private static List<TableRef> listUserTables(DatabaseMetaData meta) throws SQLException {
        List<TableRef> out = new ArrayList<>();
        try (ResultSet rs = meta.getTables(null, null, "%", new String[]{"TABLE"})) {
            while (rs.next()) {
                String schema = rs.getString("TABLE_SCHEM");
                String table  = rs.getString("TABLE_NAME");
                if (schema == null || table == null) continue;
                if (isSystemSchema(schema)) continue;
                out.add(new TableRef(schema, table));
            }
        }

        out.sort(Comparator
                .comparing((TableRef t) -> t.schema.toUpperCase(Locale.ROOT))
                .thenComparing(t -> t.table.toUpperCase(Locale.ROOT)));
        return out;
    }

    private static List<Col> listColumns(DatabaseMetaData meta, String schema, String table) throws SQLException {
        List<Col> cols = new ArrayList<>();
        try (ResultSet crs = meta.getColumns(null, schema, table, "%")) {
            while (crs.next()) {
                cols.add(new Col(
                        crs.getInt("ORDINAL_POSITION"),
                        crs.getString("COLUMN_NAME"),
                        crs.getInt("DATA_TYPE"),       // java.sql.Types
                        crs.getString("TYPE_NAME"),    // DB type name (for comments)
                        crs.getInt("COLUMN_SIZE"),
                        crs.getInt("DECIMAL_DIGITS"),
                        crs.getInt("NULLABLE"),
                        safeGet(crs, "IS_AUTOINCREMENT"),
                        safeGet(crs, "COLUMN_DEF")
                ));
            }
        }
        cols.sort(Comparator.comparingInt(c -> c.pos));
        return cols;
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
        return s.equals("INFORMATION_SCHEMA") || s.startsWith("SYSTEM") || s.startsWith("SYS");
    }

    private static String safeGet(ResultSet rs, String col) {
        try {
            return rs.getString(col);
        } catch (SQLException ignored) {
            return null;
        }
    }

    // -------------------- Source generation --------------------

    private static String renderBeanSource(
            String pkg,
            String className,
            TableRef table,
            List<Col> cols,
            Set<String> pkColsUpper
    ) {
        // Determine needed imports
        Set<String> imports = new TreeSet<>();
        imports.add("java.util.Objects");

        // Fields
        List<Field> fields = new ArrayList<>();
        for (Col c : cols) {
            String javaType = mapSqlTypeToJava(c.sqlType, imports);
            String fieldName = toFieldName(c.colName);
            fieldName = sanitizeJavaIdentifier(fieldName);
            boolean isPk = pkColsUpper.contains(c.colName.toUpperCase(Locale.ROOT));
            fields.add(new Field(fieldName, javaType, c, isPk));
        }

        StringBuilder sb = new StringBuilder(8_192);
        sb.append("package ").append(pkg).append(";\n\n");

        // imports
        for (String imp : imports) sb.append("import ").append(imp).append(";\n");
        if (!imports.isEmpty()) sb.append("\n");

        sb.append("/**\n");
        sb.append(" * Auto-generated from ").append(table.schema).append(".").append(table.table).append("\n");
        sb.append(" */\n");
        sb.append("public class ").append(className).append(" {\n\n");

        // fields
        for (Field f : fields) {
            sb.append("  /** DB: ").append(f.col.typeName);
            if (f.isPk) sb.append(" (PK)");
            if ("YES".equalsIgnoreCase(f.col.isAutoIncrement)) sb.append(" (AI)");
            sb.append(" */\n");
            sb.append("  private ").append(f.javaType).append(" ").append(f.name).append(";\n\n");
        }

        // no-arg constructor
        sb.append("  public ").append(className).append("() {}\n\n");

        // all-args constructor
        sb.append("  public ").append(className).append("(");
        for (int i = 0; i < fields.size(); i++) {
            Field f = fields.get(i);
            if (i > 0) sb.append(", ");
            sb.append(f.javaType).append(" ").append(f.name);
        }
        sb.append(") {\n");
        for (Field f : fields) {
            sb.append("    this.").append(f.name).append(" = ").append(f.name).append(";\n");
        }
        sb.append("  }\n\n");

        // getters/setters
        for (Field f : fields) {
            String cap = capitalize(f.name);

            sb.append("  public ").append(f.javaType).append(" get").append(cap).append("() {\n")
                    .append("    return ").append(f.name).append(";\n")
                    .append("  }\n\n");

            sb.append("  public void set").append(cap).append("(").append(f.javaType).append(" ").append(f.name).append(") {\n")
                    .append("    this.").append(f.name).append(" = ").append(f.name).append(";\n")
                    .append("  }\n\n");
        }

        // equals/hashCode/toString based on all fields
        sb.append("  @Override\n");
        sb.append("  public boolean equals(Object o) {\n");
        sb.append("    if (this == o) return true;\n");
        sb.append("    if (o == null || getClass() != o.getClass()) return false;\n");
        sb.append("    ").append(className).append(" that = (").append(className).append(") o;\n");
        sb.append("    return ");
        for (int i = 0; i < fields.size(); i++) {
            Field f = fields.get(i);
            if (i > 0) sb.append("\n        && ");
            sb.append("Objects.equals(").append(f.name).append(", that.").append(f.name).append(")");
        }
        if (fields.isEmpty()) sb.append("true");
        sb.append(";\n");
        sb.append("  }\n\n");

        sb.append("  @Override\n");
        sb.append("  public int hashCode() {\n");
        sb.append("    return Objects.hash(");
        for (int i = 0; i < fields.size(); i++) {
            if (i > 0) sb.append(", ");
            sb.append(fields.get(i).name);
        }
        sb.append(");\n");
        sb.append("  }\n\n");

        sb.append("  @Override\n");
        sb.append("  public String toString() {\n");
        sb.append("    return \"").append(className).append("{\" +\n");
        for (int i = 0; i < fields.size(); i++) {
            Field f = fields.get(i);
            String prefix = (i == 0) ? "        \"" : "        \", ";
            sb.append(prefix).append(f.name).append("=\" + ").append(f.name);
            if (i < fields.size() - 1) sb.append(" +\n");
            else sb.append(" +\n");
        }
        sb.append("        \"}\";\n");
        sb.append("  }\n");

        sb.append("}\n");
        return sb.toString();
    }

    private static String mapSqlTypeToJava(int sqlType, Set<String> imports) {
        // Use wrapper types to allow NULLs easily
        return switch (sqlType) {
            case Types.TINYINT, Types.SMALLINT -> "Short";
            case Types.INTEGER -> "Integer";
            case Types.BIGINT -> "Long";
            case Types.FLOAT, Types.REAL -> "Float";
            case Types.DOUBLE -> "Double";
            case Types.DECIMAL, Types.NUMERIC -> {
                imports.add("java.math.BigDecimal");
                yield "BigDecimal";
            }
            case Types.BIT, Types.BOOLEAN -> "Boolean";
            case Types.CHAR, Types.VARCHAR, Types.LONGVARCHAR,
                    Types.NCHAR, Types.NVARCHAR, Types.LONGNVARCHAR -> "String";
            case Types.DATE -> {
                imports.add("java.sql.Date");
                yield "Date";
            }
            case Types.TIME, Types.TIME_WITH_TIMEZONE -> {
                imports.add("java.sql.Time");
                yield "Time";
            }
            case Types.TIMESTAMP, Types.TIMESTAMP_WITH_TIMEZONE -> {
                imports.add("java.sql.Timestamp");
                yield "Timestamp";
            }
            case Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY, Types.BLOB -> "byte[]";
            default -> "Object";
        };
    }

    // -------------------- Naming helpers --------------------

    private static String toClassName(String tableName) {
        // USERS -> Users, user_order -> UserOrder
        String camel = toCamelCase(tableName, true);
        // Avoid illegal class name starting with digit
        if (!camel.isEmpty() && Character.isDigit(camel.charAt(0))) camel = "_" + camel;
        return camel.isEmpty() ? "Table" : camel;
    }

    private static String toFieldName(String columnName) {
        // ORDERED_AT -> orderedAt
        String camel = toCamelCase(columnName, false);
        if (!camel.isEmpty() && Character.isDigit(camel.charAt(0))) camel = "_" + camel;
        return camel.isEmpty() ? "field" : camel;
    }

    private static String toCamelCase(String s, boolean capFirst) {
        String[] parts = s == null ? new String[0] : s.split("[^A-Za-z0-9]+");
        StringBuilder out = new StringBuilder();
        for (String p : parts) {
            if (p.isEmpty()) continue;
            String lower = p.toLowerCase(Locale.ROOT);
            String part = Character.toUpperCase(lower.charAt(0)) + lower.substring(1);
            out.append(part);
        }
        if (!capFirst && out.length() > 0) {
            out.setCharAt(0, Character.toLowerCase(out.charAt(0)));
        }
        return out.toString();
    }

    private static String capitalize(String s) {
        if (s == null || s.isEmpty()) return s;
        return Character.toUpperCase(s.charAt(0)) + s.substring(1);
    }

    private static String sanitizeJavaIdentifier(String name) {
        // Handle Java keywords by appending underscore
        return switch (name) {
            case "class", "public", "private", "protected", "static", "final", "void",
                    "int", "long", "float", "double", "boolean", "byte", "short", "char",
                    "return", "package", "import", "new", "null", "true", "false",
                    "this", "super", "interface", "enum", "extends", "implements",
                    "switch", "case", "default", "break", "continue", "for", "while", "do",
                    "if", "else", "try", "catch", "finally", "throw", "throws", "instanceof" -> name + "_";
            default -> name;
        };
    }

    // -------------------- Small records --------------------

    private record TableRef(String schema, String table) {}

    private record Col(
            int pos,
            String colName,
            int sqlType,
            String typeName,
            int size,
            int scale,
            int nullable,
            String isAutoIncrement,
            String defaultValue
    ) {}

    private record Field(String name, String javaType, Col col, boolean isPk) {}
}
