package org.github.dbjo.codegen.db;

import org.github.dbjo.codegen.Config;
import org.github.dbjo.codegen.util.Naming;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.sql.*;
import java.util.*;

public final class EnumOverrideIndex {

    public record OverrideSpec(
            String tableSchema, // may be null/blank meaning "any"
            String tableName,
            String columnName,
            String enumSchema,  // may be null/blank meaning "use same/default"
            String enumTableName,
            String byColumnOrNull // null => PK
    ) {}

    public record EnumTableInfo(
            String schema,
            String table,
            String pkColumn,                 // single-column PK
            Set<String> uniqueSingleColsUpper // includes PK col upper
    ) {}

    public record Binding(
            OverrideSpec spec,
            String enumJavaSimple,       // e.g. GlobalRegionEnum
            String enumJavaFqn,          // cfg.enumPkg + "." + GlobalRegionEnum
            String keyGetterMethod,      // "id" OR getter method name on enum (e.g. "nameInDb")
            String lookupNullableMethod  // "ofNullable" OR "byNameNullable"/"byNumericCodeNullable"
    ) {}

    private final Map<String, Binding> byKeyLower = new HashMap<>();

    private EnumOverrideIndex() {}

    public static EnumOverrideIndex loadAndValidate(Config cfg, Connection con) throws SQLException, IOException {
        EnumOverrideIndex idx = new EnumOverrideIndex();
        if (cfg.enumOverridesFile() == null) return idx;
        if (!Files.isRegularFile(cfg.enumOverridesFile())) return idx;

        Properties p = new Properties();
        try (InputStream in = Files.newInputStream(cfg.enumOverridesFile())) {
            p.load(in);
        }

        DatabaseMetaData md = con.getMetaData();
        Map<String, EnumTableInfo> enumInfoCache = new HashMap<>();

        for (String k : p.stringPropertyNames()) {
            String v = p.getProperty(k);
            if (v == null) continue;

            OverrideSpec spec = parseSpec(k.trim(), v.trim());

            EnumTableInfo enumInfo = enumInfoCache.computeIfAbsent(
                    keyTable(spec.enumSchema(), spec.enumTableName()),
                    kk -> {
                        try {
                            return loadEnumTableInfo(md, spec);
                        } catch (SQLException e) {
                            throw new RuntimeException(e);
                        }
                    }
            );

            // resolve by-column (default PK)
            String by = spec.byColumnOrNull();
            if (by == null || by.isBlank()) by = enumInfo.pkColumn();

            if (cfg.enumStrictUnique()) {
                String byUpper = by.toUpperCase(Locale.ROOT);
                if (!enumInfo.uniqueSingleColsUpper().contains(byUpper)) {
                    throw new SQLException("Enum override requires UNIQUE column but it is not unique: " +
                            spec.enumTableName() + "." + by + " (set strictUnique=false to allow)");
                }
            }

            String enumSimple = toEnumClassName(spec.enumTableName());
            String enumFqn = cfg.enumPkg() + "." + enumSimple;

            String keyGetter;
            String lookupMethodNullable;
            if (spec.byColumnOrNull() == null || spec.byColumnOrNull().isBlank() || by.equalsIgnoreCase(enumInfo.pkColumn())) {
                keyGetter = "id";
                lookupMethodNullable = "ofNullable";
            } else {
                keyGetter = enumGetterNameForColumn(by);
                lookupMethodNullable = enumLookupNullableMethodForColumn(by);
            }

            Binding b = new Binding(
                    new OverrideSpec(spec.tableSchema(), spec.tableName(), spec.columnName(),
                            enumInfo.schema(), enumInfo.table(), spec.byColumnOrNull()),
                    enumSimple, enumFqn, keyGetter, lookupMethodNullable
            );

            idx.byKeyLower.put(keyColumn(spec.tableSchema(), spec.tableName(), spec.columnName()), b);
        }

        return idx;
    }

    public Binding find(String tableSchema, String tableName, String columnName) {
        String k1 = keyColumn(tableSchema, tableName, columnName);
        Binding b = byKeyLower.get(k1);
        if (b != null) return b;

        String k2 = keyColumn("", tableName, columnName);
        return byKeyLower.get(k2);
    }

    // parsing

    // key: [schema.]table.column
    // value: table:enum_table[#by:col] or table:schema.enum_table[#by:col]
    private static OverrideSpec parseSpec(String key, String value) {
        String[] kp = key.split("\\.");
        if (kp.length != 2 && kp.length != 3) {
            throw new IllegalArgumentException("Bad override key '" + key + "'. Use table.column or schema.table.column");
        }
        String schema = (kp.length == 3) ? kp[0].trim() : "";
        String table  = (kp.length == 3) ? kp[1].trim() : kp[0].trim();
        String col    = (kp.length == 3) ? kp[2].trim() : kp[1].trim();

        if (!value.startsWith("table:")) {
            throw new IllegalArgumentException("Bad override value '" + value + "'. Must start with table:");
        }
        String rest = value.substring("table:".length()).trim();

        String by = null;
        int hash = rest.indexOf('#');
        if (hash >= 0) {
            String tail = rest.substring(hash + 1).trim();
            rest = rest.substring(0, hash).trim();

            if (tail.startsWith("by:")) {
                by = tail.substring("by:".length()).trim();
                if (by.isEmpty()) by = null;
            } else {
                throw new IllegalArgumentException("Bad override suffix '" + tail + "'. Use #by:<col>");
            }
        }

        String enumSchema = "";
        String enumTable = rest;
        int dot = rest.indexOf('.');
        if (dot >= 0) {
            enumSchema = rest.substring(0, dot).trim();
            enumTable = rest.substring(dot + 1).trim();
        }

        return new OverrideSpec(schema, table, col, enumSchema, enumTable, by);
    }

    private static EnumTableInfo loadEnumTableInfo(DatabaseMetaData md, OverrideSpec spec) throws SQLException {
        // choose schema: explicit enum schema OR spec schema OR dbjo.defaultSchema
        String schema = nz(spec.enumSchema());
        if (schema.isEmpty()) schema = nz(spec.tableSchema());
        if (schema.isEmpty()) schema = nz(System.getProperty("dbjo.defaultSchema", ""));

        String table = nz(spec.enumTableName());

        // Normalize for metadata calls (HSQL/H2 store unquoted identifiers uppercase; Postgres lowercase, etc.)
        String schemaMeta = schema.isEmpty() ? "" : normalizeForMeta(md, schema);
        String tableMeta  = normalizeForMeta(md, table);

        // verify table exists + read PK
        String pk = readSinglePk(md, schemaMeta, tableMeta);
        if (pk == null) {
            throw new SQLException("Enum override table must have single-column PK: " + schema + "." + spec.enumTableName());
        }

        Set<String> unique = readUniqueSingleCols(md, schemaMeta, tableMeta);
        unique.add(pk.toUpperCase(Locale.ROOT));

        // Store normalized names so later SQL generation matches JDBC names
        String outSchema = schemaMeta.isEmpty() ? schema : schemaMeta;
        String outTable  = tableMeta;

        return new EnumTableInfo(outSchema, outTable, pk, unique);
    }

    private static String readSinglePk(DatabaseMetaData md, String schema, String table) throws SQLException {
        List<String> pkCols = new ArrayList<>();
        try (ResultSet rs = md.getPrimaryKeys(null, schemaOrNull(schema), table)) {
            while (rs.next()) {
                String c = rs.getString("COLUMN_NAME");
                if (c != null) pkCols.add(c);
            }
        }
        if (pkCols.size() != 1) return null;
        return pkCols.get(0);
    }

    private static Set<String> readUniqueSingleCols(DatabaseMetaData md, String schema, String table) throws SQLException {
        Map<String, List<String>> idxCols = new HashMap<>();
        Map<String, Boolean> idxUnique = new HashMap<>();

        try (ResultSet rs = md.getIndexInfo(null, schemaOrNull(schema), table, false, false)) {
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
            List<String> cols = e.getValue();
            if (cols.size() == 1) out.add(cols.get(0).toUpperCase(Locale.ROOT));
        }
        return out;
    }

    private static String normalizeForMeta(DatabaseMetaData md, String ident) throws SQLException {
        String z = nz(ident);
        if (z.isEmpty()) return z;

        if (md.storesUpperCaseIdentifiers()) return z.toUpperCase(Locale.ROOT);
        if (md.storesLowerCaseIdentifiers()) return z.toLowerCase(Locale.ROOT);

        // mixed-case / unknown: keep as-is
        return z;
    }

    private static String enumGetterNameForColumn(String dbCol) {
        String field = Naming.toFieldName(dbCol);
        if ("name".equals(field)) return "nameInDb";
        if (RESERVED_ENUM_METHODS.contains(field)) return field + "InDb";
        return field;
    }

    private static String enumLookupNullableMethodForColumn(String dbCol) {
        String c = dbCol.trim().toLowerCase(Locale.ROOT);
        String suffix;
        if ("name".equals(c)) {
            suffix = "Name";
        } else {
            suffix = Naming.toClassName(c);
        }
        return "by" + suffix + "Nullable";
    }

    private static String toEnumClassName(String enumTableName) {
        String base = stripEnumSuffix(enumTableName);
        base = base.toLowerCase(Locale.ROOT);
        String cls = Naming.toClassName(base);
        if (!cls.endsWith("Enum")) cls = cls + "Enum";
        return cls;
    }

    private static String stripEnumSuffix(String tableName) {
        if (tableName == null) return "";
        String low = tableName.toLowerCase(Locale.ROOT);
        if (low.endsWith("_enum")) return tableName.substring(0, tableName.length() - 5);
        if (tableName.endsWith("_ENUM")) return tableName.substring(0, tableName.length() - 5);
        if (tableName.endsWith("Enum")) return tableName.substring(0, tableName.length() - 4);
        return tableName;
    }

    private static String keyTable(String schema, String table) {
        return (nz(schema) + "." + nz(table)).toLowerCase(Locale.ROOT);
    }

    private static String keyColumn(String schema, String table, String col) {
        return (nz(schema) + "." + nz(table) + "." + nz(col)).toLowerCase(Locale.ROOT);
    }

    private static String nz(String s) { return s == null ? "" : s.trim(); }

    private static String schemaOrNull(String s) {
        String z = nz(s);
        return z.isEmpty() ? null : z;
    }

    private static final Set<String> RESERVED_ENUM_METHODS = Set.of(
            "name", "ordinal", "values", "valueOf", "getDeclaringClass", "describeConstable"
    );
}
