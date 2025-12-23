package org.github.dbjo;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.sql.*;
import java.util.*;

/**
 * Generates:
 *  - Bean per table:      org.github.dbjo.generated.<TableClass>
 *  - Meta per table:      org.github.dbjo.meta.entity.<TableClass>Meta
 *
 * Output root:
 *  target/dbjo-generated
 *
 * Requires your base meta types already exist in src:
 *  org.github.dbjo.meta.entity.EntityMeta / PropertyMeta / PropGetter / PropSetter / ...
 */
public final class DbjoEntityCodegen {

    // Connect to an existing running HSQLDB Server on localhost.
    private static final String DEFAULT_URL  = "jdbc:hsqldb:hsql://localhost:9001/dbjo";
    private static final String DEFAULT_USER = "SA";
    private static final String DEFAULT_PASS = "";

    private static final Path OUT_BASE = Paths.get("target", "dbjo-generated");

    private static final String BEAN_PKG = "org.github.dbjo.generated";
    private static final String META_PKG = "org.github.dbjo.meta.entity"; // per your request

    public static void main(String[] args) throws Exception {
        String url  = args.length > 0 ? args[0] : DEFAULT_URL;
        String user = args.length > 1 ? args[1] : DEFAULT_USER;
        String pass = args.length > 2 ? args[2] : DEFAULT_PASS;

        Class.forName("org.hsqldb.jdbc.JDBCDriver");

        try (Connection conn = DriverManager.getConnection(url, user, pass)) {
            DatabaseMetaData md = conn.getMetaData();
            System.out.println("Connected:");
            System.out.println("  url     = " + url);
            System.out.println("  db      = " + md.getDatabaseProductName() + " " + md.getDatabaseProductVersion());
            System.out.println();

            int n = generateAll(conn);
            System.out.println("Generated " + n + " table(s) into: " + OUT_BASE.toAbsolutePath());
        }
    }

    private static int generateAll(Connection conn) throws SQLException, IOException {
        DatabaseMetaData meta = conn.getMetaData();

        List<TableRef> tables = listUserTables(meta);
        if (tables.isEmpty()) {
            System.out.println("No user tables found (non-system schemas).");
            return 0;
        }

        Path beanDir = OUT_BASE.resolve(BEAN_PKG.replace('.', '/'));
        Path metaDir = OUT_BASE.resolve(META_PKG.replace('.', '/'));
        Files.createDirectories(beanDir);
        Files.createDirectories(metaDir);

        int count = 0;

        for (TableRef t : tables) {
            List<Col> cols = listColumns(meta, t.schema, t.table);
            if (cols.isEmpty()) continue;

            Set<String> pkColsUpper = getPrimaryKeyColumns(meta, t.schema, t.table);

            String beanClass = toClassName(t.table);
            String beanSrc   = renderBean(BEAN_PKG, beanClass, t, cols, pkColsUpper);

            Path beanFile = beanDir.resolve(beanClass + ".java");
            write(beanFile, beanSrc);

            String metaClass = beanClass + "Meta";
            String metaSrc   = renderMeta(META_PKG, BEAN_PKG, beanClass, metaClass, t, cols, pkColsUpper);

            Path metaFile = metaDir.resolve(metaClass + ".java");
            write(metaFile, metaSrc);

            System.out.println("Wrote: " + beanFile);
            System.out.println("Wrote: " + metaFile);
            count++;
        }

        return count;
    }

    // -------------------- DB Introspection --------------------

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
                        crs.getInt("DATA_TYPE"),    // java.sql.Types
                        crs.getString("TYPE_NAME"), // DB type name
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

    private static Set<String> getPrimaryKeyColumns(DatabaseMetaData meta, String schema, String table) throws SQLException {
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
        try { return rs.getString(col); }
        catch (SQLException ignored) { return null; }
    }

    // -------------------- Bean Generation (Serializable) --------------------

    private static String renderBean(
            String pkg,
            String className,
            TableRef table,
            List<Col> cols,
            Set<String> pkColsUpper
    ) {
        Set<String> imports = new TreeSet<>();
        imports.add("java.io.Serializable");
        imports.add("java.util.Objects");

        List<Field> fields = new ArrayList<>();
        for (Col c : cols) {
            JavaType jt = mapSqlTypeToJava(c.sqlType, imports);
            String fieldName = sanitizeJavaIdentifier(toFieldName(c.colName));
            boolean isPk = pkColsUpper.contains(c.colName.toUpperCase(Locale.ROOT));
            fields.add(new Field(fieldName, jt.javaType, jt.classLiteral, c, isPk));
        }

        StringBuilder sb = new StringBuilder(10_000);
        sb.append("package ").append(pkg).append(";\n\n");
        for (String imp : imports) sb.append("import ").append(imp).append(";\n");
        sb.append("\n");

        sb.append("/**\n");
        sb.append(" * Auto-generated bean for ").append(table.schema).append(".").append(table.table).append("\n");
        sb.append(" */\n");
        sb.append("public class ").append(className).append(" implements Serializable {\n\n");
        sb.append("  private static final long serialVersionUID = 1L;\n\n");

        for (Field f : fields) {
            sb.append("  /** DB: ").append(f.col.typeName);
            if (f.isPk) sb.append(" (PK)");
            if ("YES".equalsIgnoreCase(f.col.isAutoIncrement)) sb.append(" (AI)");
            sb.append(" */\n");
            sb.append("  private ").append(f.javaType).append(" ").append(f.name).append(";\n\n");
        }

        sb.append("  public ").append(className).append("() {}\n\n");

        sb.append("  public ").append(className).append("(");
        for (int i = 0; i < fields.size(); i++) {
            if (i > 0) sb.append(", ");
            sb.append(fields.get(i).javaType).append(" ").append(fields.get(i).name);
        }
        sb.append(") {\n");
        for (Field f : fields) sb.append("    this.").append(f.name).append(" = ").append(f.name).append(";\n");
        sb.append("  }\n\n");

        for (Field f : fields) {
            String cap = capitalize(f.name);
            sb.append("  public ").append(f.javaType).append(" get").append(cap).append("() {\n")
                    .append("    return ").append(f.name).append(";\n")
                    .append("  }\n\n");
            sb.append("  public void set").append(cap).append("(").append(f.javaType).append(" ").append(f.name).append(") {\n")
                    .append("    this.").append(f.name).append(" = ").append(f.name).append(";\n")
                    .append("  }\n\n");
        }

        sb.append("  @Override\n");
        sb.append("  public boolean equals(Object o) {\n");
        sb.append("    if (this == o) return true;\n");
        sb.append("    if (o == null || getClass() != o.getClass()) return false;\n");
        sb.append("    ").append(className).append(" that = (").append(className).append(") o;\n");
        if (fields.isEmpty()) {
            sb.append("    return true;\n");
        } else {
            sb.append("    return ");
            for (int i = 0; i < fields.size(); i++) {
                Field f = fields.get(i);
                if (i > 0) sb.append("\n        && ");
                sb.append("Objects.equals(").append(f.name).append(", that.").append(f.name).append(")");
            }
            sb.append(";\n");
        }
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
            sb.append("        \"").append(i == 0 ? "" : ", ").append(f.name).append("=\" + ").append(f.name);
            sb.append(i < fields.size() - 1 ? " +\n" : " +\n");
        }
        sb.append("        \"}\";\n");
        sb.append("  }\n");

        sb.append("}\n");
        return sb.toString();
    }

    // -------------------- Meta Generation (EntityMeta + PropertyMeta constants) --------------------

    private static String renderMeta(
            String metaPkg,
            String beanPkg,
            String beanClass,
            String metaClass,
            TableRef table,
            List<Col> cols,
            Set<String> pkColsUpper
    ) {
        // We generate into org.github.dbjo.meta.entity so it sees EntityMeta/PropertyMeta without imports.
        // But we still import the bean class and java.util.List / java.io.Serializable.
        Set<String> imports = new TreeSet<>();
        imports.add("java.io.Serializable");
        imports.add("java.util.List");
        imports.add(beanPkg + "." + beanClass);

        // property metas
        List<MetaProp> props = new ArrayList<>();
        for (Col c : cols) {
            String propName = sanitizeJavaIdentifier(toFieldName(c.colName));
            JavaType jt = mapSqlTypeToJava(c.sqlType, new TreeSet<>()); // only need type + class literal
            boolean isPk = pkColsUpper.contains(c.colName.toUpperCase(Locale.ROOT));

            String constName = toUpperSnake(propName);
            props.add(new MetaProp(constName, propName, jt.javaType, jt.classLiteral, c, isPk));
        }

        StringBuilder sb = new StringBuilder(12_000);
        sb.append("package ").append(metaPkg).append(";\n\n");
        for (String imp : imports) sb.append("import ").append(imp).append(";\n");
        sb.append("\n");

        sb.append("/**\n");
        sb.append(" * Auto-generated entity meta for ").append(table.schema).append(".").append(table.table).append("\n");
        sb.append(" */\n");
        sb.append("public final class ").append(metaClass).append(" {\n\n");
        sb.append("  private ").append(metaClass).append("() {}\n\n");

        // One PropertyMeta constant per property (typed with its real V)
        for (MetaProp p : props) {
            sb.append("  public static final PropertyMeta<").append(beanClass).append(", ").append(p.javaType).append("> ")
                    .append(p.constName).append(" = new PropertyMeta<>(")
                    .append(" \"").append(p.propName).append("\",")
                    .append(" ").append(p.classLiteral).append(",")
                    .append(" ").append(beanClass).append("::get").append(capitalize(p.propName)).append(",")
                    .append(" ").append(beanClass).append("::set").append(capitalize(p.propName))
                    .append(");");
            sb.append("  /** DB: ").append(p.col.typeName);
            if (p.isPk) sb.append(" (PK)");
            if ("YES".equalsIgnoreCase(p.col.isAutoIncrement)) sb.append(" (AI)");
            sb.append(" */\n");
        }

        // Lists
        sb.append("\n  public static final List<String> ALL_PROP_NAMES = List.of(\n");
        for (int i = 0; i < props.size(); i++) {
            sb.append("      \"").append(props.get(i).propName).append("\"");
            sb.append(i < props.size() - 1 ? ",\n" : "\n");
        }
        sb.append("  );\n\n");

        sb.append("  public static final List<Class<?>> ALL_PROP_TYPES = List.of(\n");
        for (int i = 0; i < props.size(); i++) {
            sb.append("      ").append(props.get(i).classLiteral);
            sb.append(i < props.size() - 1 ? ",\n" : "\n");
        }
        sb.append("  );\n\n");

        // EntityMeta wants List<PropertyMeta<B, Serializable>> in your definition, so we cast once.
        sb.append("  @SuppressWarnings({\"rawtypes\", \"unchecked\"})\n");
        sb.append("  public static final List<PropertyMeta<").append(beanClass).append(", Serializable>> ALL_PROPERTY_METAS =\n");
        sb.append("      (List) List.of(\n");
        for (int i = 0; i < props.size(); i++) {
            sb.append("          ").append(props.get(i).constName);
            sb.append(i < props.size() - 1 ? ",\n" : "\n");
        }
        sb.append("      );\n\n");

        sb.append("  public static final EntityMeta<").append(beanClass).append("> META = new EntityMeta<>(\n");
        sb.append("      ALL_PROPERTY_METAS,\n");
        sb.append("      ALL_PROP_NAMES,\n");
        sb.append("      ALL_PROP_TYPES\n");
        sb.append("  );\n");

        sb.append("}\n");
        return sb.toString();
    }

    // -------------------- Type Mapping (Serializable-friendly) --------------------

    private static JavaType mapSqlTypeToJava(int sqlType, Set<String> imports) {
        // Wrapper types (nullable) + Serializable-friendly defaults
        return switch (sqlType) {
            case Types.TINYINT, Types.SMALLINT -> new JavaType("Short", "Short.class");
            case Types.INTEGER -> new JavaType("Integer", "Integer.class");
            case Types.BIGINT -> new JavaType("Long", "Long.class");
            case Types.FLOAT, Types.REAL -> new JavaType("Float", "Float.class");
            case Types.DOUBLE -> new JavaType("Double", "Double.class");
            case Types.DECIMAL, Types.NUMERIC -> {
                if (imports != null) imports.add("java.math.BigDecimal");
                yield new JavaType("BigDecimal", "BigDecimal.class");
            }
            case Types.BIT, Types.BOOLEAN -> new JavaType("Boolean", "Boolean.class");

            case Types.CHAR, Types.VARCHAR, Types.LONGVARCHAR,
                    Types.NCHAR, Types.NVARCHAR, Types.LONGNVARCHAR -> new JavaType("String", "String.class");

            case Types.DATE -> {
                if (imports != null) imports.add("java.sql.Date");
                yield new JavaType("Date", "Date.class");
            }
            case Types.TIME, Types.TIME_WITH_TIMEZONE -> {
                if (imports != null) imports.add("java.sql.Time");
                yield new JavaType("Time", "Time.class");
            }
            case Types.TIMESTAMP, Types.TIMESTAMP_WITH_TIMEZONE -> {
                if (imports != null) imports.add("java.sql.Timestamp");
                yield new JavaType("Timestamp", "Timestamp.class");
            }

            case Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY, Types.BLOB -> new JavaType("byte[]", "byte[].class");

            default ->
                // keep generated code compiling with your Serializable-based meta API
                    new JavaType("String", "String.class");
        };
    }

    // -------------------- Naming helpers --------------------

    private static String toClassName(String tableName) {
        String camel = toCamelCase(tableName, true);
        if (!camel.isEmpty() && Character.isDigit(camel.charAt(0))) camel = "_" + camel;
        return camel.isEmpty() ? "Table" : camel;
    }

    private static String toFieldName(String columnName) {
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
            out.append(Character.toUpperCase(lower.charAt(0))).append(lower.substring(1));
        }
        if (!capFirst && out.length() > 0) out.setCharAt(0, Character.toLowerCase(out.charAt(0)));
        return out.toString();
    }

    private static String toUpperSnake(String camel) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < camel.length(); i++) {
            char ch = camel.charAt(i);
            if (Character.isUpperCase(ch) && i > 0) sb.append('_');
            sb.append(Character.toUpperCase(ch));
        }
        return sanitizeJavaIdentifier(sb.toString());
    }

    private static String capitalize(String s) {
        if (s == null || s.isEmpty()) return s;
        return Character.toUpperCase(s.charAt(0)) + s.substring(1);
    }

    private static String sanitizeJavaIdentifier(String name) {
        if (name == null || name.isEmpty()) return name;
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

    private static void write(Path file, String content) throws IOException {
        Files.createDirectories(file.getParent());
        Files.writeString(file, content, StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
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

    private record JavaType(String javaType, String classLiteral) {}

    private record Field(String name, String javaType, String classLiteral, Col col, boolean isPk) {}

    private record MetaProp(String constName, String propName, String javaType, String classLiteral, Col col, boolean isPk) {}
}
