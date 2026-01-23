package org.github.dbjo.codegen.db;

import org.github.dbjo.codegen.Config;
import org.github.dbjo.codegen.util.FilesUtil;
import org.github.dbjo.codegen.util.Naming;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.*;

/**
 * Generates Java enum classes from database "enum tables".
 *
 * Enum table naming conventions supported:
 *   - *_enum (case-insensitive)
 *   - *_ENUM
 *   - *Enum where the preceding character is lowercase (e.g. "countryEnum")
 *
 * Foreign keys from one enum table to another become enum-typed fields in the referencing enum.
 *
 * Assumptions:
 *   - enum tables have a SINGLE-COLUMN primary key
 *   - rows in enum tables are stable lookup values
 */
public final class DbEnumCodeGenerator {

    private static final Set<String> ENUM_RESERVED_METHODS = Set.of(
            // java.lang.Enum instance methods / commonly generated names to avoid
            "name", "ordinal", "compareTo", "equals", "hashCode", "toString",
            "getDeclaringClass", "describeConstable"
    );

    private final Config cfg;

    public DbEnumCodeGenerator(Config cfg) {
        this.cfg = Objects.requireNonNull(cfg, "cfg");
    }

    public int generateAll(Connection con) throws SQLException, IOException {
        Objects.requireNonNull(con, "con");

        if (!cfg.enumEnabled()) {
            System.out.println("DbEnumCodeGenerator: disabled (enumEnabled=false)");
            return 0;
        }

        validatePackageName(cfg.enumPkg(), "enumPkg");

        DatabaseMetaData md = con.getMetaData();

        String quote = md.getIdentifierQuoteString();
        if (quote == null) quote = "\"";
        quote = quote.trim();
        if (quote.isEmpty()) quote = "\"";

        // 1) Discover enum tables
        List<TableRef> enumTables = discoverEnumTables(md, con.getCatalog(), cfg.enumIncludeViews());

        if (enumTables.isEmpty()) {
            System.out.println("DbEnumCodeGenerator: no enum tables found.");
            return 0;
        }

        System.out.println("DbEnumCodeGenerator: discovered enum tables: " + enumTables.size());
        for (TableRef t : enumTables) {
            System.out.println("  - " + t.fqn());
        }

        // Map for quick lookup by (schema, name) case-insensitive
        Map<String, TableRef> enumTableByKey = new HashMap<>();
        for (TableRef t : enumTables) {
            enumTableByKey.put(t.key(), t);
        }

        // 2) Build models + FK relationships
        Map<String, EnumTableModel> models = new HashMap<>();
        for (TableRef t : enumTables) {
            EnumTableModel m = introspectEnumTable(md, t, enumTableByKey);
            if (m != null) models.put(t.key(), m);
        }

        if (models.isEmpty()) {
            System.out.println("DbEnumCodeGenerator: all discovered enum tables were skipped (see logs above).");
            return 0;
        }

        // 3) Topo-sort by enum FK dependencies (so referenced enums generated first)
        List<EnumTableModel> sorted = topoSort(models);

        // 4) Generate
        Path pkgDir = cfg.codegenOutJava().resolve(cfg.enumPkg().replace('.', '/'));
        Files.createDirectories(pkgDir);

        int count = 0;
        for (EnumTableModel m : sorted) {
            String src = renderEnum(con, md, m, quote);
            Path outFile = pkgDir.resolve(m.className + ".java");

            FilesUtil.writeString(outFile, src, cfg.overwrite());

            System.out.println("DbEnumCodeGenerator: wrote " + outFile);
            count++;
        }

        return count;
    }

    // ----------------------------
    // Discovery + Introspection
    // ----------------------------

    private static List<TableRef> discoverEnumTables(DatabaseMetaData md, String catalog, boolean includeViews)
            throws SQLException {

        List<TableRef> out = new ArrayList<>();
        String[] types = includeViews ? new String[]{"TABLE", "VIEW"} : new String[]{"TABLE"};

        try (ResultSet rs = md.getTables(catalog, null, "%", types)) {
            while (rs.next()) {
                String schema = rs.getString("TABLE_SCHEM");
                String name = rs.getString("TABLE_NAME");
                if (name == null) continue;

                if (isEnumTableName(name)) {
                    out.add(new TableRef(catalog, schema, name));
                }
            }
        }

        out.sort(Comparator
                .comparing((TableRef t) -> nz(t.schema), String.CASE_INSENSITIVE_ORDER)
                .thenComparing(t -> t.name, String.CASE_INSENSITIVE_ORDER));

        return out;
    }

    private static boolean isEnumTableName(String tableName) {
        if (tableName == null) return false;

        String n = tableName;
        String low = n.toLowerCase(Locale.ROOT);

        if (low.endsWith("_enum")) return true;
        if (n.endsWith("_ENUM")) return true;

        if (n.endsWith("Enum") && n.length() > 4) {
            char before = n.charAt(n.length() - 5);
            return Character.isLowerCase(before);
        }

        return false;
    }

    private EnumTableModel introspectEnumTable(DatabaseMetaData md, TableRef t, Map<String, TableRef> enumTableByKey)
            throws SQLException {

        List<Col> cols = readColumns(md, t);
        if (cols.isEmpty()) {
            System.out.println("DbEnumCodeGenerator: SKIP " + t.fqn() + " (no columns)");
            return null;
        }

        List<String> pkCols = readPrimaryKeyColumns(md, t);
        if (pkCols.size() != 1) {
            System.out.println("DbEnumCodeGenerator: SKIP " + t.fqn()
                    + " (expected 1-column PK, got " + pkCols.size() + ": " + pkCols + ")");
            return null;
        }
        String pk = pkCols.get(0);

        Map<String, FkRef> fkToEnum = new HashMap<>();
        try (ResultSet rs = md.getImportedKeys(t.catalog, t.schema, t.name)) {
            while (rs.next()) {
                String fkCol = rs.getString("FKCOLUMN_NAME");
                String pkTable = rs.getString("PKTABLE_NAME");
                String pkSchema = rs.getString("PKTABLE_SCHEM");
                String pkCol = rs.getString("PKCOLUMN_NAME");

                if (fkCol == null || pkTable == null) continue;

                TableRef pkRef = enumTableByKey.get(TableRef.key(t.catalog, pkSchema, pkTable));
                if (pkRef == null) continue;

                String refClass = toEnumClassName(pkRef.name);
                fkToEnum.put(fkCol, new FkRef(pkRef, pkCol, refClass));
            }
        }

        String className = toEnumClassName(t.name);

        cols.sort(Comparator.comparingInt(c -> c.ordinal));

        return new EnumTableModel(t, className, pk, cols, fkToEnum);
    }

    private static List<Col> readColumns(DatabaseMetaData md, TableRef t) throws SQLException {
        List<Col> cols = new ArrayList<>();
        try (ResultSet rs = md.getColumns(t.catalog, t.schema, t.name, "%")) {
            while (rs.next()) {
                String name = rs.getString("COLUMN_NAME");
                int dataType = rs.getInt("DATA_TYPE");
                int nullable = rs.getInt("NULLABLE");
                int ordinal = rs.getInt("ORDINAL_POSITION");
                if (name == null) continue;

                cols.add(new Col(name, dataType, nullable == DatabaseMetaData.columnNullable, ordinal));
            }
        }
        return cols;
    }

    private static List<String> readPrimaryKeyColumns(DatabaseMetaData md, TableRef t) throws SQLException {
        List<PkCol> tmp = new ArrayList<>();
        try (ResultSet rs = md.getPrimaryKeys(t.catalog, t.schema, t.name)) {
            while (rs.next()) {
                String name = rs.getString("COLUMN_NAME");
                short seq = rs.getShort("KEY_SEQ");
                if (name != null) tmp.add(new PkCol(seq, name));
            }
        }
        tmp.sort(Comparator.comparingInt(a -> a.seq));
        List<String> out = new ArrayList<>();
        for (PkCol p : tmp) out.add(p.name);
        return out;
    }

    private static List<EnumTableModel> topoSort(Map<String, EnumTableModel> models) {
        Map<String, Set<String>> deps = new HashMap<>();
        for (EnumTableModel m : models.values()) {
            Set<String> d = new HashSet<>();
            for (FkRef fk : m.fkToEnum.values()) {
                String k = fk.pkTable.key();
                if (models.containsKey(k) && !k.equals(m.table.key())) d.add(k);
            }
            deps.put(m.table.key(), d);
        }

        Deque<String> q = new ArrayDeque<>();
        Map<String, Integer> indeg = new HashMap<>();
        for (String k : deps.keySet()) indeg.put(k, 0);
        for (Map.Entry<String, Set<String>> e : deps.entrySet()) {
            for (String d : e.getValue()) indeg.put(d, indeg.get(d) + 1);
        }
        for (Map.Entry<String, Integer> e : indeg.entrySet()) if (e.getValue() == 0) q.add(e.getKey());

        List<String> orderKeys = new ArrayList<>();
        while (!q.isEmpty()) {
            String k = q.removeFirst();
            orderKeys.add(k);
            for (String d : deps.getOrDefault(k, Set.of())) {
                int v = indeg.get(d) - 1;
                indeg.put(d, v);
                if (v == 0) q.add(d);
            }
        }

        if (orderKeys.size() != models.size()) {
            orderKeys = new ArrayList<>(models.keySet());
            orderKeys.sort(String::compareToIgnoreCase);
        }

        List<EnumTableModel> out = new ArrayList<>();
        for (String k : orderKeys) out.add(models.get(k));
        return out;
    }

    // ----------------------------
    // Rendering
    // ----------------------------

    private String renderEnum(Connection con, DatabaseMetaData md, EnumTableModel m, String quote) throws SQLException {
        String qTable = qualify(m.table, quote);

        // ORDER BY sort_order if present
        Col sortCol = cfg.enumOrderBySortOrderIfPresent() ? m.findColumn("sort_order") : null;

        String orderBy;
        if (sortCol != null) {
            orderBy = " ORDER BY " + ident(sortCol.name, quote) + " ASC, " + ident(m.pkColumn, quote) + " ASC";
        } else {
            orderBy = " ORDER BY " + ident(m.pkColumn, quote) + " ASC";
        }

        String sql = "SELECT * FROM " + qTable + orderBy;

        List<Row> rows = new ArrayList<>();
        try (Statement st = con.createStatement();
             ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) rows.add(readRow(rs, m));
        }

        // Build fields and keep names unique + Enum-safe
        List<Field> fields = new ArrayList<>();
        Set<String> imports = new TreeSet<>();
        Set<String> usedNames = new HashSet<>();

        for (Col c : m.columns) {
            boolean isFkEnum = m.fkToEnum.containsKey(c.name) && !c.name.equalsIgnoreCase(m.pkColumn);

            String javaName = uniqueEnumMemberName(c.name, usedNames);

            String javaType;
            if (isFkEnum) {
                javaType = m.fkToEnum.get(c.name).refEnumClass;
            } else {
                javaType = sqlTypeToJavaSimple(c.sqlType, c.nullable, imports);
            }

            fields.add(new Field(c, javaName, javaType, isFkEnum));
        }

        // PK type uses raw SQL type
        Col pkCol = m.findColumn(m.pkColumn);
        if (pkCol == null) throw new SQLException("PK column not found in column list: " + m.pkColumn);

        String pkJavaType = sqlTypeToJavaSimple(pkCol.sqlType, false, imports);

        // Find generated pk field name (don’t recompute; use actual field)
        String pkFieldName = null;
        for (Field f : fields) {
            if (f.col.name.equalsIgnoreCase(pkCol.name)) {
                pkFieldName = f.javaName;
                break;
            }
        }
        if (pkFieldName == null) throw new SQLException("PK field name not resolved for " + m.table.fqn() + "." + m.pkColumn);

        String byMapName = "BY_" + toUpperSnake(pkCol.name);

        StringBuilder sb = new StringBuilder(64_000);
        sb.append("package ").append(cfg.enumPkg()).append(";\n\n");

        sb.append("import java.util.*;\n");
        for (String imp : imports) {
            if (imp.startsWith("java.util.")) continue;
            sb.append("import ").append(imp).append(";\n");
        }
        sb.append("\n");

        sb.append("/**\n");
        sb.append(" * Auto-generated from table ").append(m.table.fqn()).append(".\n");
        sb.append(" * Do not edit by hand.\n");
        sb.append(" */\n");
        sb.append("public enum ").append(m.className).append(" {\n\n");

        // Constants
        for (int i = 0; i < rows.size(); i++) {
            Row r = rows.get(i);
            String constName = toEnumConstName(String.valueOf(r.pkValue));

            sb.append("    ").append(constName).append("(");
            for (int j = 0; j < fields.size(); j++) {
                Field f = fields.get(j);
                Object val = r.values.get(f.col.name);

                String expr;
                if (f.isFkEnum) {
                    FkRef fk = m.fkToEnum.get(f.col.name);
                    // assume referenced enum PK is String (typical)
                    String lit = literal(val, "String");
                    expr = f.col.nullable ? fk.refEnumClass + ".ofNullable(" + lit + ")" : fk.refEnumClass + ".of(" + lit + ")";
                } else {
                    expr = literal(val, f.javaType);
                }

                if (j > 0) sb.append(", ");
                sb.append(expr);
            }
            sb.append(")");
            sb.append(i == rows.size() - 1 ? ";\n\n" : ",\n");
        }

        // Fields
        for (Field f : fields) {
            sb.append("    private final ").append(f.javaType).append(" ").append(f.javaName).append(";\n");
        }
        sb.append("\n");

        // Ctor
        sb.append("    ").append(m.className).append("(");
        for (int i = 0; i < fields.size(); i++) {
            Field f = fields.get(i);
            if (i > 0) sb.append(", ");
            sb.append(f.javaType).append(" ").append(f.javaName);
        }
        sb.append(") {\n");
        for (Field f : fields) {
            sb.append("        this.").append(f.javaName).append(" = ").append(f.javaName).append(";\n");
        }
        sb.append("    }\n\n");

        // Getters (Enum-safe names)
        for (Field f : fields) {
            sb.append("    public ").append(f.javaType).append(" ").append(f.javaName)
                    .append("() { return ").append(f.javaName).append("; }\n");
        }
        sb.append("\n");

        // Lookup map
        sb.append("    private static final Map<").append(pkJavaType).append(", ").append(m.className).append("> ").append(byMapName).append(";\n");
        sb.append("    static {\n");
        sb.append("        Map<").append(pkJavaType).append(", ").append(m.className).append("> m = new HashMap<>();\n");
        sb.append("        for (").append(m.className).append(" e : values()) {\n");
        sb.append("            m.put(e.").append(pkFieldName).append(", e);\n");
        sb.append("        }\n");
        sb.append("        ").append(byMapName).append(" = Collections.unmodifiableMap(m);\n");
        sb.append("    }\n\n");

        sb.append("    public static ").append(m.className).append(" of(").append(pkJavaType).append(" id) {\n");
        sb.append("        ").append(m.className).append(" e = ").append(byMapName).append(".get(id);\n");
        sb.append("        if (e == null) throw new IllegalArgumentException(\"Unknown ").append(m.className).append(" id: \" + id);\n");
        sb.append("        return e;\n");
        sb.append("    }\n\n");

        sb.append("    public static ").append(m.className).append(" ofNullable(").append(pkJavaType).append(" id) {\n");
        sb.append("        if (id == null) return null;\n");
        sb.append("        return ").append(byMapName).append(".get(id);\n");
        sb.append("    }\n\n");

        sb.append("    @Override public String toString() { return String.valueOf(").append(pkFieldName).append("); }\n");

        sb.append("}\n");
        return sb.toString();
    }

    private static Row readRow(ResultSet rs, EnumTableModel m) throws SQLException {
        Map<String, Object> vals = new HashMap<>();
        Object pkVal = null;

        for (Col c : m.columns) {
            Object v = rs.getObject(c.name);
            vals.put(c.name, v);
            if (c.name.equalsIgnoreCase(m.pkColumn)) pkVal = v;
        }
        if (pkVal == null) throw new SQLException("Row has null PK for " + m.table.fqn() + "." + m.pkColumn);
        return new Row(pkVal, vals);
    }

    // ----------------------------
    // Naming + Literals
    // ----------------------------

    private static String toEnumClassName(String tableName) {
        String base = stripEnumSuffix(tableName);
        String cls = Naming.toClassName(base);
        if (!cls.endsWith("Enum")) cls = cls + "Enum";
        return cls;
    }

    private static String stripEnumSuffix(String tableName) {
        if (tableName == null) return "";
        String n = tableName;
        String low = n.toLowerCase(Locale.ROOT);
        if (low.endsWith("_enum")) return n.substring(0, n.length() - 5);
        if (n.endsWith("_ENUM")) return n.substring(0, n.length() - 5);
        if (n.endsWith("Enum")) return n.substring(0, n.length() - 4);
        return n;
    }

    private static String toEnumConstName(String pkValue) {
        if (pkValue == null) return "_NULL";
        String s = pkValue.trim();
        if (s.isEmpty()) return "_EMPTY";

        StringBuilder b = new StringBuilder(s.length() + 8);
        for (int i = 0; i < s.length(); i++) {
            char ch = s.charAt(i);
            if (Character.isLetterOrDigit(ch)) b.append(Character.toUpperCase(ch));
            else b.append('_');
        }
        String out = b.toString();

        if (!out.isEmpty() && Character.isDigit(out.charAt(0))) out = "_" + out;
        out = out.replaceAll("_+", "_");
        if (Naming.JAVA_KEYWORDS.contains(out)) out = out + "_";
        return out;
    }

    private static String toUpperSnake(String name) {
        if (name == null) return "";
        String s = name.replaceAll("([a-z0-9])([A-Z])", "$1_$2");
        s = s.replace('-', '_').replace(' ', '_');
        return s.toUpperCase(Locale.ROOT);
    }

    private static String safeJavaIdent(String s) {
        if (s == null || s.isEmpty()) return "_";
        String out = s;
        if (!Character.isJavaIdentifierStart(out.charAt(0))) out = "_" + out;

        StringBuilder b = new StringBuilder(out.length());
        for (int i = 0; i < out.length(); i++) {
            char ch = out.charAt(i);
            b.append(Character.isJavaIdentifierPart(ch) ? ch : '_');
        }
        out = b.toString();
        if (Naming.JAVA_KEYWORDS.contains(out)) out = out + "_";
        return out;
    }

    private static String baseEnumMemberName(String dbColumnName) {
        String base = Naming.toFieldName(dbColumnName);
        String ident = safeJavaIdent(base);

        // hard rule you asked for
        if ("name".equals(ident)) return "nameInDB";

        // also avoid other Enum method collisions
        if (ENUM_RESERVED_METHODS.contains(ident)) return ident + "InDB";

        return ident;
    }

    private static String uniqueEnumMemberName(String dbColumnName, Set<String> used) {
        String n = baseEnumMemberName(dbColumnName);
        if (used.add(n)) return n;

        // de-dupe
        int k = 2;
        while (true) {
            String cand = n + k;
            if (used.add(cand)) return cand;
            k++;
        }
    }

    private static String sqlTypeToJavaSimple(int sqlType, boolean nullable, Set<String> imports) {
        return switch (sqlType) {
            case Types.CHAR, Types.VARCHAR, Types.LONGVARCHAR, Types.NCHAR, Types.NVARCHAR, Types.LONGNVARCHAR -> "String";
            case Types.BOOLEAN, Types.BIT -> nullable ? "Boolean" : "boolean";
            case Types.TINYINT, Types.SMALLINT, Types.INTEGER -> nullable ? "Integer" : "int";
            case Types.BIGINT -> nullable ? "Long" : "long";
            case Types.REAL, Types.FLOAT -> nullable ? "Float" : "float";
            case Types.DOUBLE -> nullable ? "Double" : "double";
            case Types.DECIMAL, Types.NUMERIC -> {
                if (imports != null) imports.add("java.math.BigDecimal");
                yield "BigDecimal";
            }
            case Types.DATE -> {
                if (imports != null) imports.add("java.time.LocalDate");
                yield "LocalDate";
            }
            case Types.TIMESTAMP -> {
                if (imports != null) imports.add("java.time.LocalDateTime");
                yield "LocalDateTime";
            }
            case Types.TIMESTAMP_WITH_TIMEZONE -> {
                if (imports != null) imports.add("java.time.OffsetDateTime");
                yield "OffsetDateTime";
            }
            default -> "Object";
        };
    }

    private static String literal(Object v, String javaType) {
        if (v == null) return "null";

        return switch (javaType) {
            case "String" -> "\"" + escapeJava(String.valueOf(v)) + "\"";

            case "boolean", "Boolean" -> {
                if (v instanceof Boolean b) yield b ? "true" : "false";
                yield Boolean.parseBoolean(String.valueOf(v)) ? "true" : "false";
            }

            case "int", "Integer" -> String.valueOf(((Number) coerceNumber(v)).intValue());
            case "long", "Long" -> String.valueOf(((Number) coerceNumber(v)).longValue()) + "L";
            case "float", "Float" -> String.valueOf(((Number) coerceNumber(v)).floatValue()) + "f";
            case "double", "Double" -> String.valueOf(((Number) coerceNumber(v)).doubleValue());

            case "BigDecimal" -> "new java.math.BigDecimal(\"" + escapeJava(String.valueOf(v)) + "\")";

            case "LocalDate" -> {
                if (v instanceof java.sql.Date d) yield "LocalDate.parse(\"" + d.toString() + "\")";
                yield "LocalDate.parse(\"" + escapeJava(String.valueOf(v)) + "\")";
            }

            case "LocalDateTime" -> {
                if (v instanceof java.sql.Timestamp ts) {
                    yield "LocalDateTime.parse(\"" + ts.toString().replace(' ', 'T') + "\")";
                }
                yield "LocalDateTime.parse(\"" + escapeJava(String.valueOf(v)).replace(" ", "T") + "\")";
            }

            case "OffsetDateTime" -> "OffsetDateTime.parse(\"" + escapeJava(String.valueOf(v)) + "\")";

            default -> {
                if (v instanceof Number) yield String.valueOf(v);
                yield "\"" + escapeJava(String.valueOf(v)) + "\"";
            }
        };
    }

    private static Number coerceNumber(Object v) {
        if (v instanceof Number n) return n;
        if (v instanceof Boolean b) return b ? 1 : 0;
        try {
            return new java.math.BigDecimal(String.valueOf(v));
        } catch (Exception e) {
            return 0;
        }
    }

    private static String escapeJava(String s) {
        return s.replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\r", "\\r")
                .replace("\n", "\\n")
                .replace("\t", "\\t");
    }

    private static String qualify(TableRef t, String quote) {
        if (t.schema != null && !t.schema.isEmpty()) {
            return ident(t.schema, quote) + "." + ident(t.name, quote);
        }
        return ident(t.name, quote);
    }

    private static String ident(String name, String quote) {
        if (name == null) return "";
        String q = quote;
        String esc = name.replace(q, q + q);
        return q + esc + q;
    }

    private static String nz(String s) {
        return s == null ? "" : s;
    }

    // ----------------------------
    // Package validation
    // ----------------------------

    private static void validatePackageName(String pkg, String fieldName) {
        if (pkg == null || pkg.isBlank()) {
            throw new IllegalArgumentException(fieldName + " is blank");
        }
        String[] parts = pkg.split("\\.");
        for (String p : parts) {
            if (p.isEmpty()) throw new IllegalArgumentException("Invalid " + fieldName + ": " + pkg);
            if (Naming.JAVA_KEYWORDS.contains(p)) {
                throw new IllegalArgumentException(
                        "Invalid " + fieldName + " '" + pkg + "': segment '" + p + "' is a Java keyword."
                );
            }
            if (!Character.isJavaIdentifierStart(p.charAt(0))) {
                throw new IllegalArgumentException("Invalid " + fieldName + " '" + pkg + "': segment '" + p + "' is not a valid identifier");
            }
            for (int i = 1; i < p.length(); i++) {
                if (!Character.isJavaIdentifierPart(p.charAt(i))) {
                    throw new IllegalArgumentException("Invalid " + fieldName + " '" + pkg + "': segment '" + p + "' is not a valid identifier");
                }
            }
        }
    }

    // ----------------------------
    // Models
    // ----------------------------

    private static final class TableRef {
        final String catalog;
        final String schema;
        final String name;

        TableRef(String catalog, String schema, String name) {
            this.catalog = catalog;
            this.schema = schema;
            this.name = name;
        }

        String key() { return key(catalog, schema, name); }

        static String key(String catalog, String schema, String name) {
            return (nz(schema) + "." + name).toLowerCase(Locale.ROOT);
        }

        String fqn() {
            if (schema == null || schema.isBlank()) return name;
            return schema + "." + name;
        }
    }

    private static final class Col {
        final String name;
        final int sqlType;
        final boolean nullable;
        final int ordinal;

        Col(String name, int sqlType, boolean nullable, int ordinal) {
            this.name = name;
            this.sqlType = sqlType;
            this.nullable = nullable;
            this.ordinal = ordinal;
        }
    }

    private static final class PkCol {
        final int seq;
        final String name;
        PkCol(int seq, String name) { this.seq = seq; this.name = name; }
    }

    private static final class FkRef {
        final TableRef pkTable;
        final String pkColumn;
        final String refEnumClass;
        FkRef(TableRef pkTable, String pkColumn, String refEnumClass) {
            this.pkTable = pkTable;
            this.pkColumn = pkColumn;
            this.refEnumClass = refEnumClass;
        }
    }

    private static final class EnumTableModel {
        final TableRef table;
        final String className;
        final String pkColumn;
        final List<Col> columns;
        final Map<String, FkRef> fkToEnum;

        EnumTableModel(TableRef table, String className, String pkColumn, List<Col> columns, Map<String, FkRef> fkToEnum) {
            this.table = table;
            this.className = className;
            this.pkColumn = pkColumn;
            this.columns = columns;
            this.fkToEnum = fkToEnum;
        }

        Col findColumn(String name) {
            for (Col c : columns) if (c.name.equalsIgnoreCase(name)) return c;
            return null;
        }
    }

    private static final class Field {
        final Col col;
        final String javaName;
        final String javaType;
        final boolean isFkEnum;

        Field(Col col, String javaName, String javaType, boolean isFkEnum) {
            this.col = col;
            this.javaName = javaName;
            this.javaType = javaType;
            this.isFkEnum = isFkEnum;
        }
    }

    private static final class Row {
        final Object pkValue;
        final Map<String, Object> values;
        Row(Object pkValue, Map<String, Object> values) { this.pkValue = pkValue; this.values = values; }
    }
}
