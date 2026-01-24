package org.github.dbjo.codegen.util;

import org.github.dbjo.codegen.model.Col;
import org.github.dbjo.codegen.model.TableModel;

import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Types;
import java.util.*;

/**
 * Builds an index of enum tables from the already-introspected TableModel list,
 * and resolves enum bindings for normal table columns using:
 *
 *   1) explicit overrides (recommended)
 *   2) optional heuristic resolution by column name (off by default in find(...))
 *
 * Enum table naming conventions supported:
 *   - *_enum (any case)
 *   - *_ENUM
 *   - *Enum where the preceding character is lowercase (e.g. "countryEnum")
 *
 * Override file format (no extra deps):
 *
 *   # left: schema.table.column OR table.column
 *   # right: enumRef[:enumKeyColumn][;strictUnique=true|false]
 *
 *   public.country.global_region = public.global_region_enum:code;strictUnique=true
 *   country_enum.global_region   = global_region_enum:code
 *
 * enumRef may be enum table name ("global_region_enum") or base name ("global_region").
 * enumKeyColumn defaults to enum PK if omitted.
 */
public final class EnumIndex {

    /** Information about an enum table + its columns */
    public record EnumInfo(
            String schema,
            String baseName,
            String enumTableName,
            String enumClassName,
            String pkColumn,
            int pkSqlType,
            Map<String, Integer> sqlTypeByColLower,
            Set<String> uniqueColsLower // may be empty if TableModel doesn't expose it
    ) {}

    /**
     * What the generators actually need:
     * - which enum maps to a specific (schema.table.column)
     * - which enum key column is used (pk by default)
     * - lookup methods (of/ofNullable or byX/byXNullable)
     * - key getter on enum (e.g. code(), nameInDB())
     */
    public record Binding(
            String tableSchema,
            String tableName,
            String columnName,
            EnumInfo enumInfo,
            String enumKeyColumn,
            int enumKeySqlType,
            boolean strictUnique,
            String enumJavaPackage // optional
    ) {
        public String enumJavaSimple() {
            return enumInfo.enumClassName();
        }

        public String enumJavaFqn() {
            if (enumJavaPackage == null || enumJavaPackage.isBlank()) return enumJavaSimple();
            return enumJavaPackage + "." + enumJavaSimple();
        }

        /** Getter method on enum instance returning the chosen key. */
        public String keyGetterMethod() {
            return enumPropertyNameForColumn(enumKeyColumn);
        }

        /** Lookup method name without Nullable suffix. */
        public String lookupMethod() {
            if (isPkKey()) return "of";
            return "by" + Naming.capitalize(enumPropertyNameForColumn(enumKeyColumn));
        }

        /** Lookup method name with Nullable behavior. */
        public String lookupNullableMethod() {
            if (isPkKey()) return "ofNullable";
            return "by" + Naming.capitalize(enumPropertyNameForColumn(enumKeyColumn)) + "Nullable";
        }

        public boolean isPkKey() {
            return enumKeyColumn.equalsIgnoreCase(enumInfo.pkColumn());
        }
    }

    /** Parsed override entry */
    public record OverrideSpec(
            String tableSchema,
            String tableName,
            String columnName,
            String enumSchema,     // may be null => default to table schema
            String enumRef,        // enum table/base/class-ish
            String enumKeyColumn,  // may be null => default pk
            boolean strictUnique
    ) {}

    private final Map<String, EnumInfo> bySchemaAndBase = new HashMap<>();
    private final Map<String, EnumInfo> uniqueByBase = new HashMap<>();
    private final Set<String> ambiguousBases = new HashSet<>();

    // explicit overrides: schema.table.column -> OverrideSpec
    private final Map<String, OverrideSpec> overrides = new HashMap<>();

    // cache: schema.table.column + sqlType -> Binding
    private final Map<String, Binding> bindingCache = new HashMap<>();

    private String enumJavaPackage; // optional, used by Binding.enumJavaFqn()

    private EnumIndex() {}

    public static EnumIndex fromTables(List<TableModel> tables) {
        EnumIndex idx = new EnumIndex();

        // deterministic: schema/name sort
        List<TableModel> copy = new ArrayList<>(tables);
        copy.sort(Comparator
                .comparing((TableModel tm) -> nz(tm.table().schema()).toLowerCase(Locale.ROOT))
                .thenComparing(tm -> nz(tm.table().table()).toLowerCase(Locale.ROOT)));

        for (TableModel tm : copy) {
            String schema = tm.table().schema();
            String table = tm.table().table();
            if (table == null) continue;

            if (!isEnumTableName(table)) continue;

            String base = stripEnumSuffix(table);
            if (base.isBlank()) continue;

            // enum PK must be single-column for our mappings
            String pkCol = findSinglePkColumnName(tm);
            if (pkCol == null) continue;

            Map<String, Integer> sqlTypes = new HashMap<>();
            for (Col c : tm.cols()) {
                if (c.colName() == null) continue;
                sqlTypes.put(normalize(c.colName()), c.sqlType());
            }

            Integer pkSqlType = sqlTypes.get(normalize(pkCol));
            if (pkSqlType == null) continue;

            Set<String> uniqueColsLower = new HashSet<>();
            for (String u : tryGetUniqueColsUpper(tm)) {
                uniqueColsLower.add(normalize(u));
            }

            String baseKey = normalize(base);
            String schemaKey = normalizeSchema(schema);

            String enumClass = Naming.toClassName(base) + "Enum";

            EnumInfo info = new EnumInfo(
                    schema,
                    base,
                    table,
                    enumClass,
                    pkCol,
                    pkSqlType,
                    Collections.unmodifiableMap(sqlTypes),
                    Collections.unmodifiableSet(uniqueColsLower)
            );

            // ✅ FIX: instance fields must be accessed through idx.*
            idx.bySchemaAndBase.put(key(schemaKey, baseKey), info);

            // track uniqueness across schemas
            if (!idx.ambiguousBases.contains(baseKey)) {
                EnumInfo prev = idx.uniqueByBase.putIfAbsent(baseKey, info);
                if (prev != null && !Objects.equals(normalizeSchema(prev.schema()), schemaKey)) {
                    idx.ambiguousBases.add(baseKey);
                    idx.uniqueByBase.remove(baseKey);
                }
            }
        }

        return idx;
    }

    /** Optional: makes Binding.enumJavaFqn() available for generators. */
    public EnumIndex withEnumJavaPackage(String enumJavaPackage) {
        this.enumJavaPackage = enumJavaPackage;
        return this;
    }

    /** Load overrides from file. */
    public EnumIndex loadOverrides(Path file) throws IOException {
        if (file == null) return this;
        if (!Files.exists(file)) return this;

        List<String> lines = Files.readAllLines(file, StandardCharsets.UTF_8);
        for (int i = 0; i < lines.size(); i++) {
            String raw = lines.get(i);
            String line = stripComment(raw).trim();
            if (line.isEmpty()) continue;

            int eq = line.indexOf('=');
            if (eq < 0) {
                throw new IllegalArgumentException("Enum overrides: expected '=' at " + file + ":" + (i + 1) + " -> " + raw);
            }

            String left = line.substring(0, eq).trim();
            String right = line.substring(eq + 1).trim();

            OverrideSpec spec = parseOverrideLine(left, right, file, i + 1);
            putOverride(spec);
        }

        bindingCache.clear();
        return this;
    }

    /** Add/replace one override programmatically. */
    public EnumIndex putOverride(OverrideSpec spec) {
        Objects.requireNonNull(spec, "spec");
        String k = columnKey(spec.tableSchema(), spec.tableName(), spec.columnName());
        overrides.put(k, spec);
        bindingCache.clear();
        return this;
    }

    /**
     * Generator-facing resolver (recommended): explicit override only.
     * Returns null if not overridden.
     */
    public Binding find(String tableSchema, String tableName, String columnName, int columnSqlType) {
        return find(tableSchema, tableName, columnName, columnSqlType, false);
    }

    /**
     * Resolver with optional heuristic fallback (name-based) if no override exists.
     * Heuristic fallback is type-checked.
     */
    public Binding find(String tableSchema, String tableName, String columnName, int columnSqlType, boolean allowHeuristicFallback) {
        if (columnName == null || columnName.isBlank()) return null;

        String cacheKey = columnKey(tableSchema, tableName, columnName) + "::" + columnSqlType + "::" + allowHeuristicFallback;
        Binding cached = bindingCache.get(cacheKey);
        if (cached != null) return cached;

        // 1) explicit override
        OverrideSpec ov = overrides.get(columnKey(tableSchema, tableName, columnName));
        if (ov != null) {
            Binding b = resolveOverride(ov, columnSqlType);
            bindingCache.put(cacheKey, b);
            return b;
        }

        // 2) heuristic fallback (optional)
        if (allowHeuristicFallback) {
            EnumInfo e = resolveByColumnName(tableSchema, columnName);
            if (e != null && sqlTypeCompatible(columnSqlType, e.pkSqlType())) {
                Binding b = new Binding(tableSchema, tableName, columnName, e, e.pkColumn(), e.pkSqlType(), false, enumJavaPackage);
                bindingCache.put(cacheKey, b);
                return b;
            }
        }
        return null;
    }

    // Convenience overloads for generators that don't pass sqlType.
    // Uses Types.OTHER as "unknown" (treated as wildcard in sqlTypeCompatible).
    public Binding find(String tableSchema, String tableName, String columnName) {
        return find(tableSchema, tableName, columnName, java.sql.Types.OTHER, false);
    }

    public Binding find(String tableSchema, String tableName, String columnName, boolean allowHeuristicFallback) {
        return find(tableSchema, tableName, columnName, java.sql.Types.OTHER, allowHeuristicFallback);
    }

    // Convenience overload if you *do* have the codegen Col
    public Binding find(String tableSchema, String tableName, org.github.dbjo.codegen.model.Col col) {
        if (col == null) return null;
        return find(tableSchema, tableName, col.colName(), col.sqlType(), false);
    }

    /** Old behavior: resolve by column name only (no table), no type check. */
    public EnumInfo resolve(String tableSchema, String columnName) {
        return resolveByColumnName(tableSchema, columnName);
    }

    // ---------------- core resolution ----------------

    private Binding resolveOverride(OverrideSpec ov, int columnSqlType) {
        String tableSchema = ov.tableSchema();
        String tableName   = ov.tableName();
        String columnName  = ov.columnName();

        String enumSchema = (ov.enumSchema() == null || ov.enumSchema().isBlank())
                ? tableSchema
                : ov.enumSchema();

        EnumInfo enumInfo = resolveEnumRef(enumSchema, ov.enumRef(), tableSchema);
        if (enumInfo == null) {
            throw new IllegalArgumentException("Enum override cannot resolve enumRef '" + ov.enumRef()
                    + "' (enumSchema=" + enumSchema + ") for " + tableSchema + "." + tableName + "." + columnName);
        }

        String keyCol = (ov.enumKeyColumn() == null || ov.enumKeyColumn().isBlank())
                ? enumInfo.pkColumn()
                : ov.enumKeyColumn();

        Integer keyType = enumInfo.sqlTypeByColLower().get(normalize(keyCol));
        if (keyType == null) {
            throw new IllegalArgumentException("Enum override key column '" + keyCol + "' not found in enum table "
                    + enumInfo.schema() + "." + enumInfo.enumTableName());
        }

        if (!sqlTypeCompatible(columnSqlType, keyType)) {
            throw new IllegalArgumentException("Enum override type mismatch for " + tableSchema + "." + tableName + "." + columnName
                    + ": column sqlType=" + columnSqlType
                    + " not compatible with enum key sqlType=" + keyType
                    + " (enum=" + enumInfo.schema() + "." + enumInfo.enumTableName() + ", keyCol=" + keyCol + ")");
        }

        if (ov.strictUnique() && !keyCol.equalsIgnoreCase(enumInfo.pkColumn())) {
            String kLower = normalize(keyCol);
            if (!enumInfo.uniqueColsLower().contains(kLower)) {
                throw new IllegalArgumentException("Enum override strictUnique=true but key column '" + keyCol + "' is not known unique "
                        + "(enum=" + enumInfo.schema() + "." + enumInfo.enumTableName()
                        + "). Either:\n"
                        + "  - set enumKeyColumn to the PK, or\n"
                        + "  - teach your introspector/TableModel to expose unique columns (e.g. uniqueColsUpper()), or\n"
                        + "  - set strictUnique=false");
            }
        }

        return new Binding(tableSchema, tableName, columnName, enumInfo, keyCol, keyType, ov.strictUnique(), enumJavaPackage);
    }

    private EnumInfo resolveEnumRef(String enumSchema, String enumRef, String fallbackTableSchema) {
        if (enumRef == null || enumRef.isBlank()) return null;

        // allow "schema.name" in enumRef
        String refSchema = enumSchema;
        String refName = enumRef.trim();
        int dot = refName.indexOf('.');
        if (dot > 0) {
            refSchema = refName.substring(0, dot).trim();
            refName = refName.substring(dot + 1).trim();
        }

        String schemaKey = normalizeSchema(refSchema);

        String base1 = stripEnumSuffix(refName);
        EnumInfo e = bySchemaAndBase.get(key(schemaKey, normalize(base1)));
        if (e != null) return e;

        e = bySchemaAndBase.get(key(schemaKey, normalize(refName)));
        if (e != null) return e;

        if (refName.endsWith("Enum")) {
            String dec = decamelize(refName.substring(0, refName.length() - 4));
            e = bySchemaAndBase.get(key(schemaKey, normalize(dec)));
            if (e != null) return e;
        }

        // if unique across schemas, allow fallback
        String baseKey = normalize(base1);
        if (!ambiguousBases.contains(baseKey)) {
            EnumInfo u = uniqueByBase.get(baseKey);
            if (u != null) return u;
        }

        // last attempt: fallback schema
        if (refSchema == null || refSchema.isBlank()) {
            String fb = normalizeSchema(fallbackTableSchema);
            e = bySchemaAndBase.get(key(fb, normalize(base1)));
            if (e != null) return e;
        }

        return null;
    }

    private EnumInfo resolveByColumnName(String tableSchema, String columnName) {
        if (columnName == null || columnName.isBlank()) return null;

        String schemaKey = normalizeSchema(tableSchema);

        List<String> candidates = new ArrayList<>(4);
        String c0 = normalize(columnName);
        candidates.add(c0);

        if (c0.endsWith("_id")) candidates.add(c0.substring(0, c0.length() - 3));
        if (c0.endsWith("_code")) candidates.add(c0.substring(0, c0.length() - 5));
        if (c0.endsWith("_cd")) candidates.add(c0.substring(0, c0.length() - 3));

        for (String b : candidates) {
            EnumInfo e = bySchemaAndBase.get(key(schemaKey, b));
            if (e != null) return e;
        }

        for (String b : candidates) {
            EnumInfo e = uniqueByBase.get(b);
            if (e != null) return e;
        }

        return null;
    }

    // ---------------- override parsing ----------------

    private static OverrideSpec parseOverrideLine(String left, String right, Path file, int lineNo) {
        String[] lp = left.split("\\.");
        String tableSchema;
        String tableName;
        String columnName;

        if (lp.length == 3) {
            tableSchema = lp[0].trim();
            tableName = lp[1].trim();
            columnName = lp[2].trim();
        } else if (lp.length == 2) {
            tableSchema = "";
            tableName = lp[0].trim();
            columnName = lp[1].trim();
        } else {
            throw new IllegalArgumentException("Enum overrides: invalid LHS at " + file + ":" + lineNo + " -> " + left);
        }

        String enumPart = right;
        boolean strictUnique = false;

        int semi = enumPart.indexOf(';');
        if (semi >= 0) {
            String tail = enumPart.substring(semi + 1).trim();
            enumPart = enumPart.substring(0, semi).trim();

            for (String kv : tail.split(",")) {
                String t = kv.trim();
                if (t.isEmpty()) continue;
                int eq = t.indexOf('=');
                if (eq < 0) continue;
                String k = t.substring(0, eq).trim();
                String v = t.substring(eq + 1).trim();
                if (k.equalsIgnoreCase("strictUnique")) strictUnique = Boolean.parseBoolean(v);
            }
        }

        String enumRef;
        String enumKey = null;

        int colon = enumPart.indexOf(':');
        if (colon >= 0) {
            enumRef = enumPart.substring(0, colon).trim();
            enumKey = enumPart.substring(colon + 1).trim();
            if (enumKey.isEmpty()) enumKey = null;
        } else {
            enumRef = enumPart.trim();
        }

        return new OverrideSpec(tableSchema, tableName, columnName, null, enumRef, enumKey, strictUnique);
    }

    private static String stripComment(String s) {
        int hash = s.indexOf('#');
        if (hash >= 0) return s.substring(0, hash);
        int sl = s.indexOf("//");
        if (sl >= 0) return s.substring(0, sl);
        return s;
    }

    // ---------------- naming + typing helpers ----------------

    public static boolean isEnumTableName(String tableName) {
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

    public static String stripEnumSuffix(String tableName) {
        if (tableName == null) return "";
        String n = tableName;
        String low = n.toLowerCase(Locale.ROOT);

        if (low.endsWith("_enum")) return n.substring(0, n.length() - 5);
        if (n.endsWith("_ENUM")) return n.substring(0, n.length() - 5);
        if (n.endsWith("Enum")) return n.substring(0, n.length() - 4);

        return n;
    }

    private static String enumPropertyNameForColumn(String col) {
        if (col == null) return "_";
        if ("name".equalsIgnoreCase(col)) return "nameInDB";
        return Naming.sanitizeJavaIdentifier(Naming.toFieldName(col));
    }

    private static String key(String schemaKey, String baseKey) {
        return schemaKey + "::" + baseKey;
    }

    private static String columnKey(String schema, String table, String column) {
        return normalizeSchema(schema) + "::" + normalize(nz(table)) + "::" + normalize(nz(column));
    }

    private static String normalizeSchema(String schema) {
        return normalize(nz(schema));
    }

    private static String normalize(String s) {
        if (s == null) return "";
        String x = s.trim().toLowerCase(Locale.ROOT);
        x = x.replace(' ', '_').replace('-', '_');
        return x;
    }

    private static String nz(String s) {
        return s == null ? "" : s;
    }

    private static String decamelize(String s) {
        if (s == null || s.isEmpty()) return "";
        StringBuilder b = new StringBuilder(s.length() + 8);
        for (int i = 0; i < s.length(); i++) {
            char ch = s.charAt(i);
            if (Character.isUpperCase(ch) && i > 0) b.append('_');
            b.append(Character.toLowerCase(ch));
        }
        return b.toString();
    }

    private static boolean sqlTypeCompatible(int a, int b) {
        if (a == b) return true;
        return sqlTypeFamily(a) == sqlTypeFamily(b);
    }

    private static int sqlTypeFamily(int t) {
        return switch (t) {
            case Types.CHAR, Types.VARCHAR, Types.LONGVARCHAR,
                    Types.NCHAR, Types.NVARCHAR, Types.LONGNVARCHAR -> 1;

            case Types.TINYINT, Types.SMALLINT, Types.INTEGER, Types.BIGINT -> 2;

            case Types.DECIMAL, Types.NUMERIC -> 3;

            case Types.FLOAT, Types.REAL, Types.DOUBLE -> 4;

            case Types.BIT, Types.BOOLEAN -> 5;

            case Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY, Types.BLOB -> 6;

            case Types.DATE, Types.TIME, Types.TIME_WITH_TIMEZONE,
                    Types.TIMESTAMP, Types.TIMESTAMP_WITH_TIMEZONE -> 7;

            default -> 999;
        };
    }

    private static String findSinglePkColumnName(TableModel tm) {
        Set<String> pkUpper = tm.pkColsUpper();
        if (pkUpper == null || pkUpper.size() != 1) return null;
        String pkU = pkUpper.iterator().next();
        for (Col c : tm.cols()) {
            if (c.colName() != null && c.colName().toUpperCase(Locale.ROOT).equals(pkU)) {
                return c.colName();
            }
        }
        return pkU;
    }

    private static Set<String> tryGetUniqueColsUpper(TableModel tm) {
        try {
            Method m = tm.getClass().getMethod("uniqueColsUpper");
            Object v = m.invoke(tm);
            if (v instanceof Set<?> s) {
                Set<String> out = new HashSet<>();
                for (Object o : s) if (o != null) out.add(String.valueOf(o));
                return out;
            }
        } catch (Exception ignored) {
        }
        return Set.of();
    }
}
