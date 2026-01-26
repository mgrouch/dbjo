package org.github.dbjo.codegen.db;

import org.github.dbjo.codegen.Config;
import org.github.dbjo.codegen.util.FilesUtil;
import org.github.dbjo.codegen.util.Naming;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;
import java.util.*;

public final class DbEnumCodeGenerator {

    private final Config cfg;

    public DbEnumCodeGenerator(Config cfg) {
        this.cfg = Objects.requireNonNull(cfg, "cfg");
    }

    public int generateAll(Connection con) throws SQLException, IOException {
        Objects.requireNonNull(con, "con");

        if (!cfg.enumEnabled() && !cfg.runMode().runEnums()) {
            return 0;
        }

        validatePackageName(cfg.enumPkg(), "enumPkg");

        DatabaseMetaData md = con.getMetaData();
        String quote = md.getIdentifierQuoteString();
        if (quote == null) quote = "\"";
        quote = quote.trim();
        if (quote.isEmpty()) quote = "\"";

        List<TableRef> enumTables = discoverEnumTables(md, con.getCatalog(), cfg.enumIncludeViews());
        if (enumTables.isEmpty()) return 0;

        Map<String, TableRef> byKey = new HashMap<>();
        for (TableRef t : enumTables) byKey.put(t.key(), t);

        Map<String, EnumTableModel> models = new HashMap<>();
        for (TableRef t : enumTables) {
            EnumTableModel m = introspectEnumTable(md, t, byKey);
            if (m != null) models.put(t.key(), m);
        }
        if (models.isEmpty()) return 0;

        List<EnumTableModel> sorted = topoSort(models);

        Path pkgDir = cfg.codegenOutJava().resolve(cfg.enumPkg().replace('.', '/'));
        Files.createDirectories(pkgDir);

        int count = 0;
        for (EnumTableModel m : sorted) {
            String src = renderEnum(con, m, quote);
            Path outFile = pkgDir.resolve(m.className + ".java");
            FilesUtil.writeString(outFile, src, cfg.overwrite());
            count++;
        }
        return count;
    }

    // discovery

    private static List<TableRef> discoverEnumTables(DatabaseMetaData md, String catalog, boolean includeViews) throws SQLException {
        List<TableRef> out = new ArrayList<>();
        String[] types = includeViews ? new String[]{"TABLE", "VIEW"} : new String[]{"TABLE"};

        try (ResultSet rs = md.getTables(catalog, null, "%", types)) {
            while (rs.next()) {
                String schema = rs.getString("TABLE_SCHEM");
                String name = rs.getString("TABLE_NAME");
                if (name == null) continue;
                if (isEnumTableName(name)) out.add(new TableRef(catalog, schema, name));
            }
        }

        out.sort(Comparator
                .comparing((TableRef t) -> nz(t.schema), String.CASE_INSENSITIVE_ORDER)
                .thenComparing(t -> t.name, String.CASE_INSENSITIVE_ORDER));
        return out;
    }

    private static boolean isEnumTableName(String tableName) {
        if (tableName == null) return false;
        String low = tableName.toLowerCase(Locale.ROOT);

        if (low.endsWith("_enum")) return true;
        if (tableName.endsWith("_ENUM")) return true;

        if (tableName.endsWith("Enum") && tableName.length() > 4) {
            char before = tableName.charAt(tableName.length() - 5);
            return Character.isLowerCase(before);
        }
        return false;
    }

    private EnumTableModel introspectEnumTable(DatabaseMetaData md, TableRef t, Map<String, TableRef> enumTableByKey) throws SQLException {
        List<Col> cols = readColumns(md, t);
        if (cols.isEmpty()) return null;

        String pk = readSinglePk(md, t);
        if (pk == null) return null;

        // FK->enum
        Map<String, FkRef> fkToEnum = new HashMap<>();
        try (ResultSet rs = md.getImportedKeys(t.catalog, t.schema, t.name)) {
            while (rs.next()) {
                String fkCol = rs.getString("FKCOLUMN_NAME");
                String pkTable = rs.getString("PKTABLE_NAME");
                String pkSchema = rs.getString("PKTABLE_SCHEM");
                String pkCol = rs.getString("PKCOLUMN_NAME");
                if (fkCol == null || pkTable == null) continue;

                TableRef pkRef = enumTableByKey.get(TableRef.key(pkSchema, pkTable));
                if (pkRef == null) continue;
                fkToEnum.put(fkCol, new FkRef(pkRef, pkCol, toEnumClassName(pkRef.name)));
            }
        }

        cols.sort(Comparator.comparingInt(c -> c.ordinal));

        // unique single-cols (for byX lookup maps)
        Set<String> uniqueSingleColsUpper = readUniqueSingleCols(md, t);
        uniqueSingleColsUpper.add(pk.toUpperCase(Locale.ROOT));

        return new EnumTableModel(t, toEnumClassName(t.name), pk, cols, fkToEnum, uniqueSingleColsUpper);
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

    private static String readSinglePk(DatabaseMetaData md, TableRef t) throws SQLException {
        List<String> pkCols = new ArrayList<>();
        try (ResultSet rs = md.getPrimaryKeys(t.catalog, t.schema, t.name)) {
            while (rs.next()) {
                String name = rs.getString("COLUMN_NAME");
                if (name != null) pkCols.add(name);
            }
        }
        return pkCols.size() == 1 ? pkCols.get(0) : null;
    }

    private static Set<String> readUniqueSingleCols(DatabaseMetaData md, TableRef t) throws SQLException {
        Map<String, List<String>> idxCols = new HashMap<>();
        Map<String, Boolean> idxUnique = new HashMap<>();

        try (ResultSet rs = md.getIndexInfo(t.catalog, t.schema, t.name, false, false)) {
            while (rs.next()) {
                String idx = rs.getString("INDEX_NAME");
                String col = rs.getString("COLUMN_NAME");
                boolean nonUnique = rs.getBoolean("NON_UNIQUE");
                if (idx == null || col == null) continue;
                idxCols.computeIfAbsent(idx, k -> new ArrayList<>()).add(col);
                idxUnique.put(idx, !nonUnique);
            }
        }

        Set<String> out = new HashSet<>();
        for (var e : idxCols.entrySet()) {
            if (!Boolean.TRUE.equals(idxUnique.get(e.getKey()))) continue;
            if (e.getValue().size() == 1) out.add(e.getValue().get(0).toUpperCase(Locale.ROOT));
        }
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
        for (var e : deps.entrySet()) for (String d : e.getValue()) indeg.put(d, indeg.get(d) + 1);
        for (var e : indeg.entrySet()) if (e.getValue() == 0) q.add(e.getKey());

        List<String> order = new ArrayList<>();
        while (!q.isEmpty()) {
            String k = q.removeFirst();
            order.add(k);
            for (String d : deps.getOrDefault(k, Set.of())) {
                int v = indeg.get(d) - 1;
                indeg.put(d, v);
                if (v == 0) q.add(d);
            }
        }

        if (order.size() != models.size()) {
            order = new ArrayList<>(models.keySet());
            order.sort(String::compareToIgnoreCase);
        }

        List<EnumTableModel> out = new ArrayList<>();
        for (String k : order) out.add(models.get(k));
        return out;
    }

    // render

    private String renderEnum(Connection con, EnumTableModel m, String quote) throws SQLException {
        String qTable = qualify(m.table, quote);

        // ORDER BY sort_order if present using actual column name (avoid quoted "sort_order" vs "SORT_ORDER")
        Col sortCol = cfg.enumOrderBySortOrderIfPresent() ? m.findColumn("sort_order") : null;

        String orderBy = (sortCol != null)
                ? " ORDER BY " + ident(sortCol.name, quote) + " ASC, " + ident(m.pkColumn, quote) + " ASC"
                : " ORDER BY " + ident(m.pkColumn, quote) + " ASC";

        String sql = "SELECT * FROM " + qTable + orderBy;

        List<Row> rows = new ArrayList<>();
        try (Statement st = con.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) rows.add(readRow(rs, m));
        }

        // fields + imports
        List<Field> fields = new ArrayList<>();
        Set<String> imports = new TreeSet<>();

        for (Col c : m.columns) {
            boolean isFkEnum = m.fkToEnum.containsKey(c.name) && !c.name.equalsIgnoreCase(m.pkColumn);

            String rawField = Naming.toFieldName(c.name);
            String javaField = avoidEnumMethodName(rawField); // name -> nameInDb, ordinal -> ordinalInDb, etc.

            String javaType;
            if (isFkEnum) {
                javaType = m.fkToEnum.get(c.name).refEnumClass;
            } else {
                javaType = sqlTypeToJavaSimple(c.sqlType, c.nullable, imports);
            }

            fields.add(new Field(c, javaField, javaType, isFkEnum));
        }

        Col pkCol = m.findColumn(m.pkColumn);
        if (pkCol == null) throw new SQLException("PK column not found: " + m.pkColumn);

        Set<String> pkImports = new TreeSet<>();
        String pkJavaType = sqlTypeToJavaSimple(pkCol.sqlType, false, pkImports);
        imports.addAll(pkImports);

        String pkFieldName = avoidEnumMethodName(Naming.toFieldName(pkCol.name));
        String byIdMapName = "BY_ID";

        // unique lookup columns (single-col unique) => maps + methods
        List<Col> uniqueCols = new ArrayList<>();
        for (Col c : m.columns) {
            if (m.uniqueSingleColsUpper.contains(c.name.toUpperCase(Locale.ROOT))
                    && !c.name.equalsIgnoreCase(m.pkColumn)) {
                uniqueCols.add(c);
            }
        }

        StringBuilder sb = new StringBuilder(64_000);
        sb.append("package ").append(cfg.enumPkg()).append(";\n\n");
        sb.append("import java.util.*;\n");
        for (String imp : imports) if (!imp.startsWith("java.util.")) sb.append("import ").append(imp).append(";\n");
        sb.append("\n");
        sb.append("public enum ").append(m.className).append(" {\n\n");

        // constants
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
                    String lit = literal(val, "String"); // safe default for enum FK keys
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

        // fields
        for (Field f : fields) sb.append("    private final ").append(f.javaType).append(" ").append(f.javaName).append(";\n");
        sb.append("\n");

        // ctor
        sb.append("    ").append(m.className).append("(");
        for (int i = 0; i < fields.size(); i++) {
            Field f = fields.get(i);
            if (i > 0) sb.append(", ");
            sb.append(f.javaType).append(" ").append(f.javaName);
        }
        sb.append(") {\n");
        for (Field f : fields) sb.append("        this.").append(f.javaName).append(" = ").append(f.javaName).append(";\n");
        sb.append("    }\n\n");

        // id() (PK)
        sb.append("    public ").append(pkJavaType).append(" id() { return ").append(pkFieldName).append("; }\n\n");

        // getters
        for (Field f : fields) {
            sb.append("    public ").append(f.javaType).append(" ").append(f.javaName).append("() { return ").append(f.javaName).append("; }\n");
        }
        sb.append("\n");

        // BY_ID + of/ofNullable
        sb.append("    private static final Map<").append(pkJavaType).append(", ").append(m.className).append("> ").append(byIdMapName).append(";\n");
        sb.append("    static {\n");
        sb.append("        Map<").append(pkJavaType).append(", ").append(m.className).append("> m = new HashMap<>();\n");
        sb.append("        for (").append(m.className).append(" e : values()) m.put(e.").append(pkFieldName).append(", e);\n");
        sb.append("        ").append(byIdMapName).append(" = Collections.unmodifiableMap(m);\n");
        sb.append("    }\n\n");

        sb.append("    public static ").append(m.className).append(" of(").append(pkJavaType).append(" id) {\n");
        sb.append("        ").append(m.className).append(" e = ").append(byIdMapName).append(".get(id);\n");
        sb.append("        if (e == null) throw new IllegalArgumentException(\"Unknown ").append(m.className).append(" id: \" + id);\n");
        sb.append("        return e;\n");
        sb.append("    }\n\n");

        sb.append("    public static ").append(m.className).append(" ofNullable(").append(pkJavaType).append(" id) {\n");
        sb.append("        if (id == null) return null;\n");
        sb.append("        return ").append(byIdMapName).append(".get(id);\n");
        sb.append("    }\n\n");

        // unique by<Col>
        for (Col uc : uniqueCols) {
            String rawField = Naming.toFieldName(uc.name);
            String getterField = avoidEnumMethodName(rawField);

            Set<String> tmpImp = new TreeSet<>();
            String keyType = sqlTypeToJavaSimple(uc.sqlType, false, tmpImp);
            for (String imp : tmpImp) if (!imp.startsWith("java.util.")) imports.add(imp);

            String methodSuffix = "name".equalsIgnoreCase(uc.name)
                    ? "Name"
                    : Naming.toClassName(uc.name.toLowerCase(Locale.ROOT));
            String mapName = "BY_" + toUpperSnake(uc.name);

            sb.append("    private static final Map<").append(keyType).append(", ").append(m.className).append("> ").append(mapName).append(";\n");
            sb.append("    static {\n");
            sb.append("        Map<").append(keyType).append(", ").append(m.className).append("> m = new HashMap<>();\n");
            sb.append("        for (").append(m.className).append(" e : values()) m.put(e.").append(getterField).append(", e);\n");
            sb.append("        ").append(mapName).append(" = Collections.unmodifiableMap(m);\n");
            sb.append("    }\n\n");

            sb.append("    public static ").append(m.className).append(" by").append(methodSuffix).append("(").append(keyType).append(" v) {\n");
            sb.append("        ").append(m.className).append(" e = ").append(mapName).append(".get(v);\n");
            sb.append("        if (e == null) throw new IllegalArgumentException(\"Unknown ").append(m.className).append(" ").append(uc.name).append(": \" + v);\n");
            sb.append("        return e;\n");
            sb.append("    }\n\n");

            sb.append("    public static ").append(m.className).append(" by").append(methodSuffix).append("Nullable(").append(keyType).append(" v) {\n");
            sb.append("        if (v == null) return null;\n");
            sb.append("        return ").append(mapName).append(".get(v);\n");
            sb.append("    }\n\n");
        }

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

    // naming / types

    private static String toEnumClassName(String enumTableName) {
        String base = stripEnumSuffix(enumTableName).toLowerCase(Locale.ROOT);
        String cls = Naming.toClassName(base);
        if (!cls.endsWith("Enum")) cls += "Enum";
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

        // Avoid keywords
        if (Naming.JAVA_KEYWORDS.contains(out)) out = out + "_";

        return out;
    }

    private static String stripEnumSuffix(String tableName) {
        if (tableName == null) return "";
        String low = tableName.toLowerCase(Locale.ROOT);
        String substring = tableName.substring(0, tableName.length() - 5);
        if (low.endsWith("_enum")) return substring;
        if (tableName.endsWith("_ENUM")) return substring;
        if (tableName.endsWith("Enum")) return tableName.substring(0, tableName.length() - 4);
        return tableName;
    }

    private static String avoidEnumMethodName(String field) {
        if ("name".equals(field)) return "nameInDb";
        if (RESERVED_ENUM_METHODS.contains(field)) return field + "InDb";
        return field;
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
                yield "java.math.BigDecimal";
            }
            case Types.DATE -> {
                if (imports != null) imports.add("java.time.LocalDate");
                yield "java.time.LocalDate";
            }
            case Types.TIMESTAMP -> {
                if (imports != null) imports.add("java.time.LocalDateTime");
                yield "java.time.LocalDateTime";
            }
            case Types.TIMESTAMP_WITH_TIMEZONE -> {
                if (imports != null) imports.add("java.time.OffsetDateTime");
                yield "java.time.OffsetDateTime";
            }
            default -> "Object";
        };
    }

    private static String literal(Object v, String javaType) {
        if (v == null) return "null";
        return switch (javaType) {
            case "boolean", "Boolean" -> (Boolean.parseBoolean(String.valueOf(v)) ? "true" : "false");
            case "int", "Integer" -> String.valueOf(coerceNumber(v).intValue());
            case "long", "Long" -> coerceNumber(v).longValue() + "L";
            case "float", "Float" -> coerceNumber(v).floatValue() + "f";
            case "double", "Double" -> String.valueOf(coerceNumber(v).doubleValue());
            case "java.math.BigDecimal" -> "new java.math.BigDecimal(\"" + escapeJava(String.valueOf(v)) + "\")";
            case "java.time.LocalDate" -> "java.time.LocalDate.parse(\"" + escapeJava(String.valueOf(v)) + "\")";
            case "java.time.LocalDateTime" -> "java.time.LocalDateTime.parse(\"" + escapeJava(String.valueOf(v)).replace(" ", "T") + "\")";
            case "java.time.OffsetDateTime" -> "java.time.OffsetDateTime.parse(\"" + escapeJava(String.valueOf(v)) + "\")";
            default -> "\"" + escapeJava(String.valueOf(v)) + "\"";
        };
    }

    private static Number coerceNumber(Object v) {
        if (v instanceof Number n) return n;
        try { return new java.math.BigDecimal(String.valueOf(v)); } catch (Exception e) { return 0; }
    }

    private static String escapeJava(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\r", "\\r").replace("\n", "\\n").replace("\t", "\\t");
    }

    private static String qualify(TableRef t, String quote) {
        if (t.schema != null && !t.schema.isEmpty()) return ident(t.schema, quote) + "." + ident(t.name, quote);
        return ident(t.name, quote);
    }

    private static String ident(String name, String quote) {
        String esc = name.replace(quote, quote + quote);
        return quote + esc + quote;
    }

    private static String toUpperSnake(String name) {
        if (name == null) return "";
        String s = name.replaceAll("([a-z\\d])([A-Z])", "$1_$2");
        s = s.replace('-', '_').replace(' ', '_');
        return s.toUpperCase(Locale.ROOT);
    }

    private static String nz(String s) { return s == null ? "" : s; }

    private static void validatePackageName(String pkg, String fieldName) {
        if (pkg == null || pkg.isBlank()) throw new IllegalArgumentException(fieldName + " is blank");
        for (String p : pkg.split("\\.")) {
            if (p.isEmpty()) throw new IllegalArgumentException("Invalid " + fieldName + ": " + pkg);
            if (Naming.JAVA_KEYWORDS.contains(p)) throw new IllegalArgumentException("Invalid " + fieldName + ": keyword segment " + p);
        }
    }

    // models

    private record TableRef(String catalog, String schema, String name) {
        String key() {
            return key(schema, name);
        }

        static String key(String schema, String name) {
                return (nz(schema) + "." + name).toLowerCase(Locale.ROOT);
        }

        String fqn() {
            return (schema == null || schema.isBlank()) ? name : (schema + "." + name);
        }
    }

    private record Col(String name, int sqlType, boolean nullable, int ordinal) {
    }

    private record FkRef(TableRef pkTable, String pkColumn, String refEnumClass) {
    }

    private record EnumTableModel(TableRef table, String className, String pkColumn, List<Col> columns,
                                  Map<String, FkRef> fkToEnum, Set<String> uniqueSingleColsUpper) {
        Col findColumn(String name) {
            for (Col c : columns) if (c.name.equalsIgnoreCase(name)) return c;
            return null;
        }
    }

    private record Field(Col col, String javaName, String javaType, boolean isFkEnum) {
    }

    private record Row(Object pkValue, Map<String, Object> values) {
    }

    private static final Set<String> RESERVED_ENUM_METHODS = Set.of(
            "name", "ordinal", "values", "valueOf", "getDeclaringClass", "describeConstable"
    );
}
