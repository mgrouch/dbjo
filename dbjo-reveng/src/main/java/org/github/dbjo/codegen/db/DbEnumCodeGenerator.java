package org.github.dbjo.codegen.db;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;
import java.sql.Date;
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

    public static final class Cfg {
        public final Path outJavaDir;
        public final String enumPackage;
        public final boolean includeViews;
        public final boolean orderBySortOrderIfPresent;

        public Cfg(Path outJavaDir, String enumPackage) {
            this(outJavaDir, enumPackage, false, true);
        }

        public Cfg(Path outJavaDir, String enumPackage, boolean includeViews, boolean orderBySortOrderIfPresent) {
            this.outJavaDir = Objects.requireNonNull(outJavaDir, "outJavaDir");
            this.enumPackage = Objects.requireNonNull(enumPackage, "enumPackage");
            this.includeViews = includeViews;
            this.orderBySortOrderIfPresent = orderBySortOrderIfPresent;
        }
    }

    private final Cfg cfg;

    public DbEnumCodeGenerator(Cfg cfg) {
        this.cfg = cfg;
    }

    public int generateAll(Connection con) throws SQLException, IOException {
        Objects.requireNonNull(con, "con");

        DatabaseMetaData md = con.getMetaData();
        String quote = md.getIdentifierQuoteString();
        if (quote == null) quote = "\"";
        quote = quote.trim();
        if (quote.isEmpty()) quote = "\"";

        // 1) Discover enum tables
        List<TableRef> enumTables = discoverEnumTables(md, con.getCatalog(), quote, cfg.includeViews);

        // Map for quick lookup by (schema, name) case-insensitive
        Map<String, TableRef> enumTableByKey = new HashMap<>();
        for (TableRef t : enumTables) {
            enumTableByKey.put(t.key(), t);
        }

        // 2) Build models + FK relationships
        Map<String, EnumTableModel> models = new HashMap<>();
        for (TableRef t : enumTables) {
            EnumTableModel m = introspectEnumTable(md, t, enumTableByKey);
            if (m != null) {
                models.put(t.key(), m);
            }
        }

        // 3) Topo-sort by enum FK dependencies (so referenced enums generated first)
        List<EnumTableModel> sorted = topoSort(models);

        // 4) Generate
        Path pkgDir = cfg.outJavaDir.resolve(cfg.enumPackage.replace('.', '/'));
        Files.createDirectories(pkgDir);

        int count = 0;
        for (EnumTableModel m : sorted) {
            String src = renderEnum(con, md, m, quote);
            Path outFile = pkgDir.resolve(m.className + ".java");
            Files.write(outFile, src.getBytes(StandardCharsets.UTF_8));
            count++;
        }

        return count;
    }

    // ----------------------------
    // Discovery + Introspection
    // ----------------------------

    private static List<TableRef> discoverEnumTables(DatabaseMetaData md, String catalog, String quote, boolean includeViews)
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

        // deterministic order
        out.sort(Comparator.comparing((TableRef t) -> nz(t.schema)).thenComparing(t -> t.name, String.CASE_INSENSITIVE_ORDER));
        return out;
    }

    private static boolean isEnumTableName(String tableName) {
        if (tableName == null) return false;
        // *_enum (any case)
        if (tableName.toLowerCase(Locale.ROOT).endsWith("_enum")) return true;
        // explicit *_ENUM
        if (tableName.endsWith("_ENUM")) return true;
        // *Enum where prior char is lowercase
        if (tableName.endsWith("Enum") && tableName.length() > 4) {
            char before = tableName.charAt(tableName.length() - 5);
            return Character.isLowerCase(before);
        }
        return false;
    }

    private static EnumTableModel introspectEnumTable(DatabaseMetaData md, TableRef t, Map<String, TableRef> enumTableByKey)
            throws SQLException {

        List<Col> cols = readColumns(md, t);
        if (cols.isEmpty()) return null;

        List<String> pkCols = readPrimaryKeyColumns(md, t);
        if (pkCols.size() != 1) {
            // enum tables should have single-column PK
            return null;
        }
        String pk = pkCols.get(0);

        // Map imported keys: fkColumn -> referenced enum class (if referenced table is enum table)
        Map<String, FkRef> fkToEnum = new HashMap<>();
        try (ResultSet rs = md.getImportedKeys(t.catalog, t.schema, t.name)) {
            while (rs.next()) {
                String fkCol = rs.getString("FKCOLUMN_NAME");
                String pkTable = rs.getString("PKTABLE_NAME");
                String pkSchema = rs.getString("PKTABLE_SCHEM");
                String pkCol = rs.getString("PKCOLUMN_NAME");

                if (fkCol == null || pkTable == null) continue;

                TableRef pkRef = enumTableByKey.get(TableRef.key(t.catalog, pkSchema, pkTable));
                if (pkRef == null) continue; // not an enum table reference

                String refClass = toEnumClassName(pkRef.name);
                fkToEnum.put(fkCol, new FkRef(pkRef, pkCol, refClass));
            }
        }

        String className = toEnumClassName(t.name);

        // Keep columns in ordinal order
        cols.sort(Comparator.comparingInt(c -> c.ordinal));

        return new EnumTableModel(t, className, pk, cols, fkToEnum);
    }

    private static List<Col> readColumns(DatabaseMetaData md, TableRef t) throws SQLException {
        List<Col> cols = new ArrayList<>();
        try (ResultSet rs = md.getColumns(t.catalog, t.schema, t.name, "%")) {
            while (rs.next()) {
                String name = rs.getString("COLUMN_NAME");
                int dataType = rs.getInt("DATA_TYPE"); // java.sql.Types
                String typeName = rs.getString("TYPE_NAME");
                int nullable = rs.getInt("NULLABLE");
                int ordinal = rs.getInt("ORDINAL_POSITION");
                int size = rs.getInt("COLUMN_SIZE");
                int scale = rs.getInt("DECIMAL_DIGITS");

                if (name == null) continue;
                cols.add(new Col(name, dataType, typeName, nullable == DatabaseMetaData.columnNullable, ordinal, size, scale));
            }
        }
        return cols;
    }

    private static List<String> readPrimaryKeyColumns(DatabaseMetaData md, TableRef t) throws SQLException {
        // PK rs has KEY_SEQ; preserve ordering
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
        // Build adjacency: A -> B when A depends on B (FK to enum B)
        Map<String, Set<String>> deps = new HashMap<>();
        for (EnumTableModel m : models.values()) {
            Set<String> d = new HashSet<>();
            for (FkRef fk : m.fkToEnum.values()) {
                String k = fk.pkTable.key();
                if (models.containsKey(k) && !k.equals(m.table.key())) d.add(k);
            }
            deps.put(m.table.key(), d);
        }

        // Kahn
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

        // If cycle (rare), fall back to deterministic order
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

        // ORDER BY sort_order if present, else by pk
        boolean hasSortOrder = cfg.orderBySortOrderIfPresent && m.hasColumnIgnoreCase("sort_order");
        String orderBy;
        if (hasSortOrder) {
            orderBy = " ORDER BY " + ident("sort_order", quote) + " ASC, " + ident(m.pkColumn, quote) + " ASC";
        } else {
            orderBy = " ORDER BY " + ident(m.pkColumn, quote) + " ASC";
        }

        String sql = "SELECT * FROM " + qTable + orderBy;

        List<Row> rows = new ArrayList<>();
        try (Statement st = con.createStatement();
             ResultSet rs = st.executeQuery(sql)) {

            while (rs.next()) {
                rows.add(readRow(rs, m));
            }
        }

        // Build Java field list (in column order). FK cols become enum types (unless FK col is PK col).
        List<Field> fields = new ArrayList<>();
        for (Col c : m.columns) {
            boolean isFkEnum = m.fkToEnum.containsKey(c.name) && !c.name.equalsIgnoreCase(m.pkColumn);
            String javaName = safeJavaIdent(toLowerCamel(c.name));
            String javaType;
            if (isFkEnum) {
                javaType = m.fkToEnum.get(c.name).refEnumClass;
            } else {
                javaType = sqlTypeToJava(c.sqlType, c.nullable);
            }
            fields.add(new Field(c, javaName, javaType, isFkEnum));
        }

        // Map key uses PK column java type (raw sql type even if PK is FK)
        Col pkCol = m.findColumn(m.pkColumn);
        if (pkCol == null) throw new SQLException("PK column not found in column list: " + m.pkColumn);
        String pkJavaType = sqlTypeToJava(pkCol.sqlType, false);

        String pkFieldName = safeJavaIdent(toLowerCamel(pkCol.name));
        String byMapName = "BY_" + toUpperSnake(pkCol.name);

        StringBuilder sb = new StringBuilder(64_000);
        sb.append("package ").append(cfg.enumPackage).append(";\n\n");
        sb.append("import java.util.*;\n\n");
        sb.append("/**\n");
        sb.append(" * Auto-generated from table ").append(m.table.name).append(".\n");
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
                    // FK value becomes <RefEnum>.of(...) or ofNullable(...)
                    FkRef fk = m.fkToEnum.get(f.col.name);
                    String lit = literal(val, sqlTypeToJava(f.col.sqlType, true));
                    if (f.col.nullable) {
                        expr = fk.refEnumClass + ".ofNullable(" + lit + ")";
                    } else {
                        expr = fk.refEnumClass + ".of(" + lit + ")";
                    }
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

        // Getters
        for (Field f : fields) {
            sb.append("    public ").append(f.javaType).append(" ").append(f.javaName).append("() { return ").append(f.javaName).append("; }\n");
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

        // of / ofNullable
        sb.append("    public static ").append(m.className).append(" of(").append(pkJavaType).append(" id) {\n");
        sb.append("        ").append(m.className).append(" e = ").append(byMapName).append(".get(id);\n");
        sb.append("        if (e == null) throw new IllegalArgumentException(\"Unknown ").append(m.className).append(" id: \" + id);\n");
        sb.append("        return e;\n");
        sb.append("    }\n\n");

        sb.append("    public static ").append(m.className).append(" ofNullable(").append(pkJavaType).append(" id) {\n");
        sb.append("        if (id == null) return null;\n");
        sb.append("        return ").append(byMapName).append(".get(id);\n");
        sb.append("    }\n\n");

        // toString: PK value
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
        if (pkVal == null) throw new SQLException("Row has null PK for " + m.table.name + "." + m.pkColumn);
        return new Row(pkVal, vals);
    }

    // ----------------------------
    // Naming + Literals
    // ----------------------------

    private static String toEnumClassName(String tableName) {
        String base = tableName;
        if (base.toLowerCase(Locale.ROOT).endsWith("_enum")) {
            base = base.substring(0, base.length() - 5);
        } else if (base.endsWith("_ENUM")) {
            base = base.substring(0, base.length() - 5);
        } else if (base.endsWith("Enum")) {
            base = base.substring(0, base.length() - 4);
        }
        String cls = toUpperCamel(base);
        if (!cls.endsWith("Enum")) cls = cls + "Enum";
        return cls;
    }

    private static String toEnumConstName(String pkValue) {
        if (pkValue == null) return "_NULL";
        String s = pkValue.trim();
        if (s.isEmpty()) return "_EMPTY";

        // Replace non [A-Za-z0-9] with underscore, uppercase
        StringBuilder b = new StringBuilder(s.length() + 8);
        for (int i = 0; i < s.length(); i++) {
            char ch = s.charAt(i);
            if (Character.isLetterOrDigit(ch)) b.append(Character.toUpperCase(ch));
            else b.append('_');
        }
        String out = b.toString();

        // Must not start with digit
        if (!out.isEmpty() && Character.isDigit(out.charAt(0))) out = "_" + out;

        // Collapse "__"
        out = out.replaceAll("_+", "_");

        // Avoid keywords (very rare for PKs but safe)
        if (JAVA_KEYWORDS.contains(out)) out = out + "_";

        return out;
    }

    private static String toLowerCamel(String name) {
        if (name == null || name.isEmpty()) return name;
        if (name.indexOf('_') < 0) {
            return Character.toLowerCase(name.charAt(0)) + name.substring(1);
        }
        String[] parts = name.toLowerCase(Locale.ROOT).split("_+");
        StringBuilder sb = new StringBuilder(name.length());
        for (int i = 0; i < parts.length; i++) {
            if (parts[i].isEmpty()) continue;
            if (i == 0) sb.append(parts[i]);
            else sb.append(Character.toUpperCase(parts[i].charAt(0))).append(parts[i].substring(1));
        }
        return sb.toString();
    }

    private static String toUpperCamel(String name) {
        if (name == null || name.isEmpty()) return name;
        String[] parts = name.split("_+");
        StringBuilder sb = new StringBuilder(name.length());
        for (String p : parts) {
            if (p.isEmpty()) continue;
            sb.append(Character.toUpperCase(p.charAt(0)));
            if (p.length() > 1) sb.append(p.substring(1).toLowerCase(Locale.ROOT));
        }
        String out = sb.toString();
        if (out.isEmpty()) out = "X";
        // If already camel, keep letters but ensure first is uppercase
        if (name.indexOf('_') < 0 && Character.isLetter(name.charAt(0))) {
            out = Character.toUpperCase(name.charAt(0)) + name.substring(1);
        }
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
        if (JAVA_KEYWORDS.contains(out)) out = out + "_";
        return out;
    }

    private static String sqlTypeToJava(int sqlType, boolean nullable) {
        switch (sqlType) {
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.NCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR:
                return "String";
            case Types.BOOLEAN:
            case Types.BIT:
                return nullable ? "Boolean" : "boolean";
            case Types.TINYINT:
            case Types.SMALLINT:
                return nullable ? "Integer" : "int";
            case Types.INTEGER:
                return nullable ? "Integer" : "int";
            case Types.BIGINT:
                return nullable ? "Long" : "long";
            case Types.REAL:
            case Types.FLOAT:
                return nullable ? "Float" : "float";
            case Types.DOUBLE:
                return nullable ? "Double" : "double";
            case Types.DECIMAL:
            case Types.NUMERIC:
                return "java.math.BigDecimal";
            case Types.DATE:
                return "java.time.LocalDate";
            case Types.TIMESTAMP:
                return "java.time.LocalDateTime";
            case Types.TIMESTAMP_WITH_TIMEZONE:
                return "java.time.OffsetDateTime";
            default:
                // fallback
                return "Object";
        }
    }

    private static String literal(Object v, String javaType) {
        if (v == null) return "null";

        switch (javaType) {
            case "String":
                return "\"" + escapeJava(String.valueOf(v)) + "\"";

            case "boolean":
            case "Boolean":
                // rs.getObject may return Boolean or numeric; normalize
                if (v instanceof Boolean) return ((Boolean) v) ? "true" : "false";
                return Boolean.parseBoolean(String.valueOf(v)) ? "true" : "false";

            case "int":
            case "Integer":
                return String.valueOf(((Number) coerceNumber(v)).intValue());

            case "long":
            case "Long":
                return String.valueOf(((Number) coerceNumber(v)).longValue()) + "L";

            case "float":
            case "Float":
                return String.valueOf(((Number) coerceNumber(v)).floatValue()) + "f";

            case "double":
            case "Double":
                return String.valueOf(((Number) coerceNumber(v)).doubleValue());

            case "java.math.BigDecimal":
                // Use string ctor for exactness
                return "new java.math.BigDecimal(\"" + escapeJava(String.valueOf(v)) + "\")";

            case "java.time.LocalDate":
                if (v instanceof Date) {
                    return "java.time.LocalDate.parse(\"" + v.toString() + "\")";
                }
                return "java.time.LocalDate.parse(\"" + escapeJava(String.valueOf(v)) + "\")";

            case "java.time.LocalDateTime":
                if (v instanceof Timestamp) {
                    return "java.time.LocalDateTime.parse(\"" + v.toString().replace(' ', 'T') + "\")";
                }
                return "java.time.LocalDateTime.parse(\"" + escapeJava(String.valueOf(v)).replace(" ", "T") + "\")";

            case "java.time.OffsetDateTime":
                return "java.time.OffsetDateTime.parse(\"" + escapeJava(String.valueOf(v)) + "\")";

            default:
                // best-effort: strings as quoted, numbers as is, otherwise toString quoted
                if (v instanceof Number) return String.valueOf(v);
                return "\"" + escapeJava(String.valueOf(v)) + "\"";
        }
    }

    private static Number coerceNumber(Object v) {
        if (v instanceof Number) return (Number) v;
        if (v instanceof Boolean) return ((Boolean) v) ? 1 : 0;
        try {
            return new BigDecimal(String.valueOf(v));
        } catch (Exception e) {
            return 0;
        }
    }

    private static String escapeJava(String s) {
        return s
                .replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\r", "\\r")
                .replace("\n", "\\n")
                .replace("\t", "\\t");
    }

    private static String qualify(TableRef t, String quote) {
        // Use schema if present (HSQL default schema is PUBLIC)
        if (t.schema != null && !t.schema.isEmpty()) {
            return ident(t.schema, quote) + "." + ident(t.name, quote);
        }
        return ident(t.name, quote);
    }

    private static String ident(String name, String quote) {
        if (name == null) return "";
        // quote identifiers; safer across DBs
        String q = quote;
        String esc = name.replace(q, q + q);
        return q + esc + q;
    }

    private static String nz(String s) {
        return s == null ? "" : s;
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

        String key() {
            return key(catalog, schema, name);
        }

        static String key(String catalog, String schema, String name) {
            return (nz(schema) + "." + name).toLowerCase(Locale.ROOT);
        }
    }

    private static final class Col {
        final String name;
        final int sqlType;
        final String sqlTypeName;
        final boolean nullable;
        final int ordinal;
        final int size;
        final int scale;

        Col(String name, int sqlType, String sqlTypeName, boolean nullable, int ordinal, int size, int scale) {
            this.name = name;
            this.sqlType = sqlType;
            this.sqlTypeName = sqlTypeName;
            this.nullable = nullable;
            this.ordinal = ordinal;
            this.size = size;
            this.scale = scale;
        }
    }

    private static final class PkCol {
        final int seq;
        final String name;

        PkCol(int seq, String name) {
            this.seq = seq;
            this.name = name;
        }
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
        final Map<String, FkRef> fkToEnum; // fkColumn -> enum reference

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

        boolean hasColumnIgnoreCase(String name) {
            return findColumn(name) != null;
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

        Row(Object pkValue, Map<String, Object> values) {
            this.pkValue = pkValue;
            this.values = values;
        }
    }

    private static final Set<String> JAVA_KEYWORDS = Set.of(
            "abstract","continue","for","new","switch",
            "assert","default","goto","package","synchronized",
            "boolean","do","if","private","this",
            "break","double","implements","protected","throw",
            "byte","else","import","public","throws",
            "case","enum","instanceof","return","transient",
            "catch","extends","int","short","try",
            "char","final","interface","static","void",
            "class","finally","long","strictfp","volatile",
            "const","float","native","super","while",
            "true","false","null"
    );
}
