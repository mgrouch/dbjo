package org.github.dbjo.codegen.db;

import org.github.dbjo.codegen.Config;
import org.github.dbjo.codegen.types.TypeMappings;
import org.github.dbjo.codegen.util.FilesUtil;
import org.github.dbjo.codegen.util.Naming;
import org.github.dbjo.meta.db.Col;
import org.github.dbjo.meta.db.Nullability;
import org.github.dbjo.meta.db.TableModel;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Types;
import java.util.*;

public final class DbMetaGenerator {
    private final Config cfg;
    private final EnumOverrideIndex enumOverrides; // nullable => no overrides

    public DbMetaGenerator(Config cfg) {
        this(cfg, null);
    }

    public DbMetaGenerator(Config cfg, EnumOverrideIndex enumOverrides) {
        this.cfg = Objects.requireNonNull(cfg, "cfg");
        this.enumOverrides = enumOverrides; // may be null
    }

    public int generateAll(List<TableModel> tables) throws IOException {
        Objects.requireNonNull(tables, "tables");

        // main DbMeta pkg
        Path outDir = cfg.codegenOutJava().resolve(cfg.dbMetaPkg().replace('.', '/'));
        Files.createDirectories(outDir);

        // registry subpackage
        String registryPkg = cfg.dbMetaPkg() + ".registry";
        Path regOutDir = cfg.codegenOutJava().resolve(registryPkg.replace('.', '/'));
        Files.createDirectories(regOutDir);

        // stable order
        List<TableModel> sorted = new ArrayList<>(tables);
        sorted.sort(Comparator
                .comparing((TableModel tm) -> nz(tm.table().schema()).toUpperCase(Locale.ROOT))
                .thenComparing(tm -> nz(tm.table().table()).toUpperCase(Locale.ROOT)));

        int count = 0;
        List<String> metaClassNames = new ArrayList<>();

        for (TableModel tm : sorted) {
            String beanClass = Naming.toClassName(tm.table().table());
            String metaClass = beanClass + "DbMeta";

            String src = render(metaClass, beanClass, tm);

            Path outFile = outDir.resolve(metaClass + ".java");
            FilesUtil.writeString(outFile, src, cfg.overwrite());
            count++;

            metaClassNames.add(metaClass);
        }

        // write registry (single file)
        String regCls = "DbMetas";
        String regSrc = renderRegistry(registryPkg, regCls, cfg.dbMetaPkg(), metaClassNames);
        Path regFile = regOutDir.resolve(regCls + ".java");
        FilesUtil.writeString(regFile, regSrc, cfg.overwrite());
        count++;

        return count;
    }

    // ------------------------------------------------------------------
    // DbMeta per-table class rendering
    // ------------------------------------------------------------------

    private String render(String metaClass, String beanClass, TableModel tm) {
        String schema = tm.table().schema();
        String table  = tm.table().table();
        String fqn = (schema == null || schema.isBlank()) ? table : (schema + "." + table);

        List<Col> cols = tm.cols() == null ? List.of() : tm.cols();

        // PK columns in stable order (as they appear in cols)
        List<Col> pkCols = new ArrayList<>();
        Set<String> pkUpper = tm.pkColsUpper() == null ? Set.of() : tm.pkColsUpper();
        for (Col c : cols) {
            if (c.colName() != null && pkUpper.contains(c.colName().toUpperCase(Locale.ROOT))) pkCols.add(c);
        }

        // Non-autoinc insert columns (existing behavior)
        List<Col> insCols = new ArrayList<>();
        for (Col c : cols) if (!c.autoIncrement()) insCols.add(c);

        // Update columns (non-autoinc, non-PK)
        List<Col> updCols = new ArrayList<>();
        for (Col c : cols) {
            if (c.autoIncrement()) continue;
            if (c.colName() != null && pkUpper.contains(c.colName().toUpperCase(Locale.ROOT))) continue;
            updCols.add(c);
        }

        // Source columns for UPSERT (must include PK for ON(), plus insertable columns)
        List<Col> srcCols = unionByName(pkCols, insCols);

        String insertSql = buildInsertSql(fqn, insCols);
        String updateSql = buildUpdateByIdSql(fqn, updCols, pkCols);
        String selectAllSql = buildSelectAllSql(fqn, cols);

        // Pre-render SQL fragments for merges
        String mergeOn = buildMergeOnClause(pkCols);
        String mergeUpdateSet = buildMergeUpdateSetClause(updCols);
        String mergeInsertCols = joinColNames(insCols);
        String mergeInsertVals = joinPrefixedColNames("s", insCols);

        // single-row upserts
        String upsertMssql = buildMergeUpsertSingle_MssqlSybase(fqn, srcCols, mergeOn, mergeUpdateSet, mergeInsertCols, mergeInsertVals, true);
        String upsertSybase = buildMergeUpsertSingle_MssqlSybase(fqn, srcCols, mergeOn, mergeUpdateSet, mergeInsertCols, mergeInsertVals, false);
        String upsertOracle = buildMergeUpsertSingle_Oracle(fqn, srcCols, mergeOn, mergeUpdateSet, mergeInsertCols, mergeInsertVals);
        String upsertHsql   = buildMergeUpsertSingle_Hsql(fqn, srcCols, mergeOn, mergeUpdateSet, mergeInsertCols, mergeInsertVals);

        // Temp-table column definitions (for batch upsert)
        String tempColDefs = buildTempTableColumnDefs(srcCols);

        // Temp insert SQL (same for all dialects, only table name differs at runtime)
        String tempInsertCols = joinColNames(srcCols);
        String tempInsertQ = repeatQ(srcCols.size());

        // Merge-from-temp SQL (table name substituted at runtime)
        String mergeFromTempMssql = buildMergeFromTemp_MssqlSybase(fqn, "{TEMP}", mergeOn, mergeUpdateSet, mergeInsertCols, mergeInsertVals, true);
        String mergeFromTempSybase = buildMergeFromTemp_MssqlSybase(fqn, "{TEMP}", mergeOn, mergeUpdateSet, mergeInsertCols, mergeInsertVals, false);
        String mergeFromTempOracle = buildMergeFromTemp_Oracle(fqn, "{TEMP}", mergeOn, mergeUpdateSet, mergeInsertCols, mergeInsertVals);
        String mergeFromTempHsql   = buildMergeFromTemp_Hsql(fqn, "{TEMP}", mergeOn, mergeUpdateSet, mergeInsertCols, mergeInsertVals);

        // imports
        Set<String> imports = new TreeSet<>();
        imports.add("java.sql.ResultSet");
        imports.add("java.sql.SQLException");
        imports.add("java.sql.SQLType");
        imports.add("java.sql.JDBCType");
        imports.add("java.util.*");
        imports.add("org.github.dbjo.meta.jdbc.DbMeta");
        imports.add("org.github.dbjo.meta.jdbc.Jdbc");

        if (!cfg.beanPkg().equals(cfg.dbMetaPkg())) imports.add(cfg.beanPkg() + "." + beanClass);

        // add type imports
        for (Col c : cols) TypeMappings.mapSqlTypeToJava(c.sqlType(), c.typeName(), imports);

        // enum imports (for fromRow conversions)
        if (enumOverrides != null) {
            for (Col c : cols) {
                EnumOverrideIndex.Binding b = enumOverrides.find(schema, table, c.colName());
                if (b != null) imports.add(b.enumJavaFqn());
            }
        }

        StringBuilder sb = new StringBuilder(18_000);
        sb.append("package ").append(cfg.dbMetaPkg()).append(";\n\n");
        for (String imp : imports) sb.append("import ").append(imp).append(";\n");
        sb.append("\n");

        sb.append("public final class ").append(metaClass)
                .append(" implements DbMeta<").append(beanClass).append("> {\n\n");

        sb.append("    public static final String SCHEMA = ")
                .append(schema == null ? "null" : "\"" + escape(schema) + "\"").append(";\n");
        sb.append("    public static final String TABLE  = \"").append(escape(table)).append("\";\n");
        sb.append("    public static final String FQN    = \"").append(escape(fqn)).append("\";\n\n");

        sb.append("    public static final String INSERT_SQL = \"").append(escape(insertSql)).append("\";\n");
        sb.append("    public static final String UPDATE_BY_ID_SQL = \"").append(escape(updateSql)).append("\";\n");
        sb.append("    public static final String SELECT_ALL_SQL = \"").append(escape(selectAllSql)).append("\";\n\n");

        sb.append("    // Upsert (dialect-specific)\n");
        sb.append("    private static final String UPSERT_BY_ID_SQL_MSSQL = \"").append(escape(upsertMssql)).append("\";\n");
        sb.append("    private static final String UPSERT_BY_ID_SQL_SYBASE = \"").append(escape(upsertSybase)).append("\";\n");
        sb.append("    private static final String UPSERT_BY_ID_SQL_ORACLE = \"").append(escape(upsertOracle)).append("\";\n");
        sb.append("    private static final String UPSERT_BY_ID_SQL_HSQL   = \"").append(escape(upsertHsql)).append("\";\n\n");

        sb.append("    // Batch upsert via temp table (templates; {TEMP} substituted at runtime)\n");
        sb.append("    private static final String UPSERT_TEMP_COL_DEFS = \"").append(escape(tempColDefs)).append("\";\n");
        sb.append("    private static final String MERGE_FROM_TEMP_MSSQL_TPL = \"").append(escape(mergeFromTempMssql)).append("\";\n");
        sb.append("    private static final String MERGE_FROM_TEMP_SYBASE_TPL = \"").append(escape(mergeFromTempSybase)).append("\";\n");
        sb.append("    private static final String MERGE_FROM_TEMP_ORACLE_TPL = \"").append(escape(mergeFromTempOracle)).append("\";\n");
        sb.append("    private static final String MERGE_FROM_TEMP_HSQL_TPL   = \"").append(escape(mergeFromTempHsql)).append("\";\n\n");

        sb.append("    public static final ").append(metaClass)
                .append(" INSTANCE = new ").append(metaClass).append("();\n\n");
        sb.append("    private ").append(metaClass).append("() {}\n\n");

        // DbMeta interface (existing)
        sb.append("    @Override public String schema() { return SCHEMA; }\n");
        sb.append("    @Override public String table()  { return TABLE; }\n");
        sb.append("    @Override public String fqn()    { return FQN; }\n\n");
        sb.append("    @Override public String insertSql() { return INSERT_SQL; }\n");
        sb.append("    @Override public String updateByIdSql() { return UPDATE_BY_ID_SQL; }\n");
        sb.append("    @Override public String selectAllSql() { return SELECT_ALL_SQL; }\n\n");

        // upsertByIdSql(dialect)
        sb.append("    @Override\n");
        sb.append("    public String upsertByIdSql(DbDialect dialect) {\n");
        sb.append("        if (dialect == null) throw new IllegalArgumentException(\"dialect is null\");\n");
        sb.append("        return switch (dialect) {\n");
        sb.append("            case MSSQL  -> UPSERT_BY_ID_SQL_MSSQL;\n");
        sb.append("            case SYBASE -> UPSERT_BY_ID_SQL_SYBASE;\n");
        sb.append("            case ORACLE -> UPSERT_BY_ID_SQL_ORACLE;\n");
        sb.append("            case HSQL   -> UPSERT_BY_ID_SQL_HSQL;\n");
        sb.append("        };\n");
        sb.append("    }\n\n");

        // fromRow
        sb.append("    @Override\n");
        sb.append("    public ").append(beanClass).append(" fromRow(ResultSet rs) throws SQLException {\n");
        sb.append("        ").append(beanClass).append(" e = new ").append(beanClass).append("();\n");
        sb.append("        int i = 1;\n");
        for (Col c : cols) {
            String prop = Naming.sanitizeJavaIdentifier(Naming.toFieldName(c.colName()));
            String cap  = Naming.capitalize(prop);

            boolean nullable = isNullable(c);

            EnumOverrideIndex.Binding eb = (enumOverrides == null) ? null : enumOverrides.find(schema, table, c.colName());
            if (eb != null) {
                TypeMappings.JavaType jt = TypeMappings.mapSqlTypeToJava(c.sqlType(), c.typeName(), null);
                String rawExpr = rsReadExpr(jt.javaType(), nullable, "rs", "i");

                sb.append("        e.set").append(cap).append("(")
                        .append(eb.enumJavaSimple()).append(".").append(eb.lookupNullableMethod())
                        .append("(").append(rawExpr).append(")")
                        .append(");\n");
            } else {
                TypeMappings.JavaType jt = TypeMappings.mapSqlTypeToJava(c.sqlType(), c.typeName(), null);
                String readExpr = rsReadExpr(jt.javaType(), nullable, "rs", "i");
                sb.append("        e.set").append(cap).append("(").append(readExpr).append(");\n");
            }
            sb.append("        i++;\n");
        }
        sb.append("        return e;\n");
        sb.append("    }\n\n");

        // insert params/types (existing)
        sb.append("    public Object[] getInsertParameters(").append(beanClass).append(" e) {\n");
        sb.append("        return new Object[] {");
        for (int idx = 0; idx < insCols.size(); idx++) {
            Col c = insCols.get(idx);
            if (idx > 0) sb.append(", ");
            sb.append(beanGetterExpr(beanClass, c, schema, table));
        }
        sb.append("};\n");
        sb.append("    }\n\n");

        sb.append("    public SQLType[] getInsertParameterTypes() {\n");
        sb.append("        return new SQLType[] {");
        for (int idx = 0; idx < insCols.size(); idx++) {
            if (idx > 0) sb.append(", ");
            sb.append(jdbcTypeExpr(insCols.get(idx).sqlType(), insCols.get(idx).typeName()));
        }
        sb.append("};\n");
        sb.append("    }\n\n");

        // update params/types (existing)
        sb.append("    public Object[] getUpdateByIdParameters(").append(beanClass).append(" e) {\n");
        sb.append("        return new Object[] {");
        boolean first = true;
        for (Col c : updCols) {
            if (!first) sb.append(", ");
            first = false;
            sb.append(beanGetterExpr(beanClass, c, schema, table));
        }
        for (Col c : pkCols) {
            if (!first) sb.append(", ");
            first = false;
            sb.append(beanGetterExpr(beanClass, c, schema, table));
        }
        sb.append("};\n");
        sb.append("    }\n\n");

        sb.append("    public SQLType[] getUpdateByIdParameterTypes() {\n");
        sb.append("        return new SQLType[] {");
        first = true;
        for (Col c : updCols) {
            if (!first) sb.append(", ");
            first = false;
            sb.append(jdbcTypeExpr(c.sqlType(), c.typeName()));
        }
        for (Col c : pkCols) {
            if (!first) sb.append(", ");
            first = false;
            sb.append(jdbcTypeExpr(c.sqlType(), c.typeName()));
        }
        sb.append("};\n");
        sb.append("    }\n\n");

        // DbMeta methods via above (existing)
        sb.append("    @Override public Object[] insertParams(").append(beanClass).append(" e) { return getInsertParameters(e); }\n");
        sb.append("    @Override public SQLType[] insertParamTypes() { return getInsertParameterTypes(); }\n");
        sb.append("    @Override public Object[] updateByIdParams(").append(beanClass).append(" e) { return getUpdateByIdParameters(e); }\n");
        sb.append("    @Override public SQLType[] updateByIdParamTypes() { return getUpdateByIdParameterTypes(); }\n\n");

        // upsert params/types
        sb.append("    @Override\n");
        sb.append("    public Object[] upsertByIdParams(").append(beanClass).append(" e) {\n");
        sb.append("        return new Object[] {");
        for (int idx = 0; idx < srcCols.size(); idx++) {
            Col c = srcCols.get(idx);
            if (idx > 0) sb.append(", ");
            sb.append(beanGetterExpr(beanClass, c, schema, table));
        }
        sb.append("};\n");
        sb.append("    }\n\n");

        sb.append("    @Override\n");
        sb.append("    public SQLType[] upsertByIdParamTypes() {\n");
        sb.append("        return new SQLType[] {");
        for (int idx = 0; idx < srcCols.size(); idx++) {
            if (idx > 0) sb.append(", ");
            sb.append(jdbcTypeExpr(srcCols.get(idx).sqlType(), srcCols.get(idx).typeName()));
        }
        sb.append("};\n");
        sb.append("    }\n\n");

        // temp helpers
        sb.append("    @Override\n");
        sb.append("    public String createUpsertTempTableSql(DbDialect dialect, String suffix) {\n");
        sb.append("        String tn = upsertTempName(dialect, suffix);\n");
        sb.append("        return switch (dialect) {\n");
        sb.append("            case MSSQL, SYBASE -> \"CREATE TABLE \" + tn + \" (\" + UPSERT_TEMP_COL_DEFS + \")\";\n");
        sb.append("            case ORACLE, HSQL  -> \"CREATE GLOBAL TEMPORARY TABLE \" + tn + \" (\" + UPSERT_TEMP_COL_DEFS + \") ON COMMIT DELETE ROWS\";\n");
        sb.append("        };\n");
        sb.append("    }\n\n");

        sb.append("    @Override\n");
        sb.append("    public String dropUpsertTempTableSql(DbDialect dialect, String suffix) {\n");
        sb.append("        String tn = upsertTempName(dialect, suffix);\n");
        sb.append("        return \"DROP TABLE \" + tn;\n");
        sb.append("    }\n\n");

        sb.append("    @Override\n");
        sb.append("    public String insertUpsertTempSql(DbDialect dialect, String suffix) {\n");
        sb.append("        String tn = upsertTempName(dialect, suffix);\n");
        sb.append("        return \"INSERT INTO \" + tn + \" (").append(escape(tempInsertCols)).append(") VALUES (").append(escape(tempInsertQ)).append(")\";\n");
        sb.append("    }\n\n");

        sb.append("    @Override\n");
        sb.append("    public String mergeUpsertFromTempSql(DbDialect dialect, String suffix) {\n");
        sb.append("        String tn = upsertTempName(dialect, suffix);\n");
        sb.append("        String tpl = switch (dialect) {\n");
        sb.append("            case MSSQL  -> MERGE_FROM_TEMP_MSSQL_TPL;\n");
        sb.append("            case SYBASE -> MERGE_FROM_TEMP_SYBASE_TPL;\n");
        sb.append("            case ORACLE -> MERGE_FROM_TEMP_ORACLE_TPL;\n");
        sb.append("            case HSQL   -> MERGE_FROM_TEMP_HSQL_TPL;\n");
        sb.append("        };\n");
        sb.append("        return tpl.replace(\"{TEMP}\", tn);\n");
        sb.append("    }\n\n");

        sb.append("    public static void bind(java.sql.PreparedStatement ps, Object[] params, SQLType[] types) throws SQLException {\n");
        sb.append("        Jdbc.bind(ps, params, types);\n");
        sb.append("    }\n\n");

        // temp name builder + suffix sanitizer (avoid SQL injection)
        sb.append("    private static String upsertTempName(DbDialect dialect, String suffix) {\n");
        sb.append("        String sfx = safeSuffix(suffix);\n");
        sb.append("        return switch (dialect) {\n");
        sb.append("            case MSSQL, SYBASE -> \"#\" + TABLE + \"_UPSERT_\" + sfx;\n");
        sb.append("            case ORACLE, HSQL  -> TABLE + \"_UPSERT_\" + sfx;\n");
        sb.append("        };\n");
        sb.append("    }\n\n");

        sb.append("    private static String safeSuffix(String suffix) {\n");
        sb.append("        String s = (suffix == null) ? \"\" : suffix.trim();\n");
        sb.append("        if (s.isEmpty()) return \"X\";\n");
        sb.append("        StringBuilder b = new StringBuilder(s.length());\n");
        sb.append("        for (int i = 0; i < s.length(); i++) {\n");
        sb.append("            char ch = s.charAt(i);\n");
        sb.append("            if ((ch >= '0' && ch <= '9') || (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') || ch == '_') b.append(ch);\n");
        sb.append("            else b.append('_');\n");
        sb.append("        }\n");
        sb.append("        String out = b.toString();\n");
        sb.append("        return out.length() > 32 ? out.substring(0, 32) : out;\n");
        sb.append("    }\n");

        sb.append("}\n");
        return sb.toString();
    }

    // ------------------------------------------------------------------
    // Registry rendering (single class under .registry)
    // ------------------------------------------------------------------

    private static String renderRegistry(String pkg, String cls, String metasPkg, List<String> metaClassNames) {
        StringBuilder sb = new StringBuilder(12_000);
        sb.append("package ").append(pkg).append(";\n\n");
        sb.append("import java.util.*;\n");
        sb.append("import org.github.dbjo.meta.jdbc.DbMeta;\n");
        sb.append("import ").append(metasPkg).append(".*;\n\n");

        sb.append("/**\n");
        sb.append(" * Auto-generated registry of all DbMeta instances.\n");
        sb.append(" *\n");
        sb.append(" * Keys:\n");
        sb.append(" *  - fqn: \"SCHEMA.TABLE\" when schema present, else \"TABLE\".\n");
        sb.append(" *  - lookups are case-insensitive.\n");
        sb.append(" */\n");
        sb.append("public final class ").append(cls).append(" {\n\n");

        sb.append("    private ").append(cls).append("() {}\n\n");

        sb.append("    public static final List<DbMeta<?>> ALL;\n");
        sb.append("    public static final Map<String, DbMeta<?>> BY_FQN;\n\n");

        sb.append("    static {\n");
        sb.append("        List<DbMeta<?>> a = new ArrayList<>();\n");
        for (String m : metaClassNames) {
            sb.append("        a.add(").append(m).append(".INSTANCE);\n");
        }
        sb.append("        ALL = Collections.unmodifiableList(a);\n\n");

        sb.append("        Map<String, DbMeta<?>> m = new HashMap<>();\n");
        sb.append("        for (DbMeta<?> dm : a) {\n");
        sb.append("            m.put(norm(dm.fqn()), dm);\n");
        sb.append("        }\n");
        sb.append("        BY_FQN = Collections.unmodifiableMap(m);\n");
        sb.append("    }\n\n");

        sb.append("    public static Optional<DbMeta<?>> find(String schema, String table) {\n");
        sb.append("        if (table == null || table.isBlank()) return Optional.empty();\n");
        sb.append("        String fqn = (schema == null || schema.isBlank()) ? table : (schema + \".\" + table);\n");
        sb.append("        return Optional.ofNullable(BY_FQN.get(norm(fqn)));\n");
        sb.append("    }\n\n");

        sb.append("    public static DbMeta<?> get(String schema, String table) {\n");
        sb.append("        return find(schema, table).orElseThrow(() ->\n");
        sb.append("                new NoSuchElementException(\"Unknown table: \" + ((schema == null || schema.isBlank()) ? \"\" : (schema + \".\")) + table));\n");
        sb.append("    }\n\n");

        sb.append("    public static Optional<DbMeta<?>> findByFqn(String fqn) {\n");
        sb.append("        if (fqn == null || fqn.isBlank()) return Optional.empty();\n");
        sb.append("        return Optional.ofNullable(BY_FQN.get(norm(fqn)));\n");
        sb.append("    }\n\n");

        sb.append("    public static DbMeta<?> getByFqn(String fqn) {\n");
        sb.append("        return findByFqn(fqn).orElseThrow(() -> new NoSuchElementException(\"Unknown fqn: \" + fqn));\n");
        sb.append("    }\n\n");

        sb.append("    private static String norm(String s) {\n");
        sb.append("        return s.trim().toLowerCase(Locale.ROOT);\n");
        sb.append("    }\n");

        sb.append("}\n");
        return sb.toString();
    }

    // ------------------------------------------------------------------
    // helpers (generation-time)
    // ------------------------------------------------------------------

    private static boolean isNullable(Col c) {
        Nullability n = c.nullability();
        return n == null || n == Nullability.NULLABLE || n == Nullability.UNKNOWN;
    }

    private String beanGetterExpr(String beanClass, Col c, String schema, String table) {
        String prop = Naming.sanitizeJavaIdentifier(Naming.toFieldName(c.colName()));
        String cap = Naming.capitalize(prop);

        EnumOverrideIndex.Binding eb = (enumOverrides == null) ? null : enumOverrides.find(schema, table, c.colName());
        if (eb != null) {
            return "e.get" + cap + "() == null ? null : e.get" + cap + "()." + eb.keyGetterMethod() + "()";
        }
        return "e.get" + cap + "()";
    }

    private static List<Col> unionByName(List<Col> a, List<Col> b) {
        Map<String, Col> m = new LinkedHashMap<>();
        for (Col c : a) if (c != null && c.colName() != null) m.putIfAbsent(c.colName().toUpperCase(Locale.ROOT), c);
        for (Col c : b) if (c != null && c.colName() != null) m.putIfAbsent(c.colName().toUpperCase(Locale.ROOT), c);
        return new ArrayList<>(m.values());
    }

    private static String buildInsertSql(String fqn, List<Col> cols) {
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO ").append(fqn).append(" (");
        for (int i = 0; i < cols.size(); i++) {
            if (i > 0) sb.append(", ");
            sb.append(cols.get(i).colName());
        }
        sb.append(") VALUES (");
        for (int i = 0; i < cols.size(); i++) {
            if (i > 0) sb.append(", ");
            sb.append("?");
        }
        sb.append(")");
        return sb.toString();
    }

    private static String buildUpdateByIdSql(String fqn, List<Col> setCols, List<Col> pkCols) {
        StringBuilder sb = new StringBuilder();
        sb.append("UPDATE ").append(fqn).append(" SET ");
        if (setCols.isEmpty()) {
            sb.append(pkCols.isEmpty()
                    ? "/* no columns */ 1=1"
                    : (pkCols.get(0).colName() + "=" + pkCols.get(0).colName()));
        } else {
            for (int i = 0; i < setCols.size(); i++) {
                if (i > 0) sb.append(", ");
                sb.append(setCols.get(i).colName()).append("=?");
            }
        }
        if (!pkCols.isEmpty()) {
            sb.append(" WHERE ");
            for (int i = 0; i < pkCols.size(); i++) {
                if (i > 0) sb.append(" AND ");
                sb.append(pkCols.get(i).colName()).append("=?");
            }
        }
        return sb.toString();
    }

    private static String buildSelectAllSql(String fqn, List<Col> cols) {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ");
        for (int i = 0; i < cols.size(); i++) {
            if (i > 0) sb.append(", ");
            sb.append(cols.get(i).colName());
        }
        sb.append(" FROM ").append(fqn);
        return sb.toString();
    }

    private static String buildMergeOnClause(List<Col> pkCols) {
        if (pkCols == null || pkCols.isEmpty()) return "1=0"; // always insert if no PK
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < pkCols.size(); i++) {
            if (i > 0) sb.append(" AND ");
            String c = pkCols.get(i).colName();
            sb.append("t.").append(c).append("=s.").append(c);
        }
        return sb.toString();
    }

    private static String buildMergeUpdateSetClause(List<Col> updCols) {
        if (updCols == null || updCols.isEmpty()) return "";
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < updCols.size(); i++) {
            if (i > 0) sb.append(", ");
            String c = updCols.get(i).colName();
            sb.append(c).append("=s.").append(c);
        }
        return sb.toString();
    }

    private static String joinColNames(List<Col> cols) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < cols.size(); i++) {
            if (i > 0) sb.append(", ");
            sb.append(cols.get(i).colName());
        }
        return sb.toString();
    }

    private static String joinPrefixedColNames(String prefix, List<Col> cols) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < cols.size(); i++) {
            if (i > 0) sb.append(", ");
            sb.append(prefix).append(".").append(cols.get(i).colName());
        }
        return sb.toString();
    }

    private static String repeatQ(int n) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < n; i++) {
            if (i > 0) sb.append(", ");
            sb.append("?");
        }
        return sb.toString();
    }

    private static String buildMergeUpsertSingle_MssqlSybase(
            String fqn,
            List<Col> srcCols,
            String onClause,
            String updateSet,
            String insertCols,
            String insertVals,
            boolean terminateSemicolon
    ) {
        StringBuilder sb = new StringBuilder();
        sb.append("MERGE INTO ").append(fqn).append(" AS t ");
        sb.append("USING (VALUES (").append(repeatQ(srcCols.size())).append(")) AS s (").append(joinColNames(srcCols)).append(") ");
        sb.append("ON (").append(onClause).append(") ");
        if (updateSet != null && !updateSet.isBlank()) {
            sb.append("WHEN MATCHED THEN UPDATE SET ").append(updateSet).append(" ");
        }
        sb.append("WHEN NOT MATCHED THEN INSERT (").append(insertCols).append(") VALUES (").append(insertVals).append(")");
        if (terminateSemicolon) sb.append(";");
        return sb.toString();
    }

    private static String buildMergeUpsertSingle_Oracle(
            String fqn,
            List<Col> srcCols,
            String onClause,
            String updateSet,
            String insertCols,
            String insertVals
    ) {
        // USING (SELECT ? AS C1, ? AS C2 ... FROM dual)
        StringBuilder using = new StringBuilder();
        using.append("SELECT ");
        for (int i = 0; i < srcCols.size(); i++) {
            if (i > 0) using.append(", ");
            using.append("? AS ").append(srcCols.get(i).colName());
        }
        using.append(" FROM dual");

        StringBuilder sb = new StringBuilder();
        sb.append("MERGE INTO ").append(fqn).append(" t ");
        sb.append("USING (").append(using).append(") s ");
        sb.append("ON (").append(onClause).append(") ");
        if (updateSet != null && !updateSet.isBlank()) {
            sb.append("WHEN MATCHED THEN UPDATE SET ").append(updateSet).append(" ");
        }
        sb.append("WHEN NOT MATCHED THEN INSERT (").append(insertCols).append(") VALUES (").append(insertVals).append(")");
        return sb.toString();
    }

    private static String buildMergeUpsertSingle_Hsql(
            String fqn,
            List<Col> srcCols,
            String onClause,
            String updateSet,
            String insertCols,
            String insertVals
    ) {
        // HSQLDB needs explicit CAST in VALUES to avoid unknown parameter types in USING clause.
        // We cast to the column's SQL type (best-effort from TYPE_NAME/precision/scale).
        StringBuilder vals = new StringBuilder();
        for (int i = 0; i < srcCols.size(); i++) {
            if (i > 0) vals.append(", ");
            Col c = srcCols.get(i);
            vals.append("CAST(? AS ").append(hsqlCastType(c)).append(")");
        }

        StringBuilder sb = new StringBuilder();
        sb.append("MERGE INTO ").append(fqn).append(" AS t ");
        sb.append("USING (VALUES (").append(vals).append(")) AS s (").append(joinColNames(srcCols)).append(") ");
        sb.append("ON (").append(onClause).append(") ");
        if (updateSet != null && !updateSet.isBlank()) {
            sb.append("WHEN MATCHED THEN UPDATE SET ").append(updateSet).append(" ");
        }
        sb.append("WHEN NOT MATCHED THEN INSERT (").append(insertCols).append(") VALUES (").append(insertVals).append(")");
        return sb.toString();
    }

    private static String buildMergeFromTemp_MssqlSybase(
            String fqn,
            String tempNameToken,
            String onClause,
            String updateSet,
            String insertCols,
            String insertVals,
            boolean terminateSemicolon
    ) {
        StringBuilder sb = new StringBuilder();
        sb.append("MERGE INTO ").append(fqn).append(" AS t ");
        sb.append("USING ").append(tempNameToken).append(" AS s ");
        sb.append("ON (").append(onClause).append(") ");
        if (updateSet != null && !updateSet.isBlank()) {
            sb.append("WHEN MATCHED THEN UPDATE SET ").append(updateSet).append(" ");
        }
        sb.append("WHEN NOT MATCHED THEN INSERT (").append(insertCols).append(") VALUES (").append(insertVals).append(")");
        if (terminateSemicolon) sb.append(";");
        return sb.toString();
    }

    private static String buildMergeFromTemp_Oracle(
            String fqn,
            String tempNameToken,
            String onClause,
            String updateSet,
            String insertCols,
            String insertVals
    ) {
        StringBuilder sb = new StringBuilder();
        sb.append("MERGE INTO ").append(fqn).append(" t ");
        sb.append("USING ").append(tempNameToken).append(" s ");
        sb.append("ON (").append(onClause).append(") ");
        if (updateSet != null && !updateSet.isBlank()) {
            sb.append("WHEN MATCHED THEN UPDATE SET ").append(updateSet).append(" ");
        }
        sb.append("WHEN NOT MATCHED THEN INSERT (").append(insertCols).append(") VALUES (").append(insertVals).append(")");
        return sb.toString();
    }

    private static String buildMergeFromTemp_Hsql(
            String fqn,
            String tempNameToken,
            String onClause,
            String updateSet,
            String insertCols,
            String insertVals
    ) {
        StringBuilder sb = new StringBuilder();
        sb.append("MERGE INTO ").append(fqn).append(" AS t ");
        sb.append("USING ").append(tempNameToken).append(" AS s ");
        sb.append("ON (").append(onClause).append(") ");
        if (updateSet != null && !updateSet.isBlank()) {
            sb.append("WHEN MATCHED THEN UPDATE SET ").append(updateSet).append(" ");
        }
        sb.append("WHEN NOT MATCHED THEN INSERT (").append(insertCols).append(") VALUES (").append(insertVals).append(")");
        return sb.toString();
    }

    private static String buildTempTableColumnDefs(List<Col> cols) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < cols.size(); i++) {
            if (i > 0) sb.append(", ");
            Col c = cols.get(i);
            sb.append(c.colName()).append(" ").append(sqlTypeDecl(c));
            sb.append(isNullableForDdl(c) ? " NULL" : " NOT NULL");
        }
        return sb.toString();
    }

    private static boolean isNullableForDdl(Col c) {
        Nullability n = c.nullability();
        return n == null || n != Nullability.NO_NULLS;
    }

    private static String sqlTypeDecl(Col c) {
        String tn = (c.typeName() == null) ? "" : c.typeName().trim();
        String up = tn.toUpperCase(Locale.ROOT);

        // If driver already includes precision/scale, keep it.
        if (tn.contains("(")) return tn;

        int size = c.size();
        int scale = c.scale();

        boolean wantsLen =
                up.contains("CHAR") || up.contains("VARCHAR") || up.contains("VARBINARY") || up.contains("BINARY");
        boolean wantsPrecScale =
                up.contains("DECIMAL") || up.contains("NUMERIC") || up.equals("NUMBER");

        if (wantsPrecScale && size > 0) {
            if (scale > 0) return tn + "(" + size + "," + scale + ")";
            return tn + "(" + size + ")";
        }

        if (wantsLen && size > 0) {
            return tn + "(" + size + ")";
        }

        return tn.isEmpty() ? "VARCHAR(255)" : tn;
    }

    private static String hsqlCastType(Col c) {
        // Cast target must be a valid HSQL type name; we reuse the best-effort decl.
        // HSQL accepts VARCHAR(n), DECIMAL(p,s), TIMESTAMP, etc.
        return sqlTypeDecl(c);
    }

    private static String escape(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private static String jdbcTypeExpr(int sqlType, String typeName) {
        String tn = (typeName == null) ? "" : typeName.trim().toUpperCase(Locale.ROOT);
        if ("UNIQUEIDENTIFIER".equals(tn)) return "JDBCType.OTHER";
        if ("DATETIMEOFFSET".equals(tn)) return "JDBCType.TIMESTAMP_WITH_TIMEZONE";

        return switch (sqlType) {
            case Types.TINYINT -> "JDBCType.TINYINT";
            case Types.SMALLINT -> "JDBCType.SMALLINT";
            case Types.INTEGER -> "JDBCType.INTEGER";
            case Types.BIGINT -> "JDBCType.BIGINT";

            case Types.FLOAT, Types.REAL -> "JDBCType.REAL";
            case Types.DOUBLE -> "JDBCType.DOUBLE";
            case Types.DECIMAL -> "JDBCType.DECIMAL";
            case Types.NUMERIC -> "JDBCType.NUMERIC";

            case Types.BIT, Types.BOOLEAN -> "JDBCType.BOOLEAN";

            case Types.CHAR -> "JDBCType.CHAR";
            case Types.NCHAR -> "JDBCType.NCHAR";

            case Types.VARCHAR, Types.LONGVARCHAR -> "JDBCType.VARCHAR";
            case Types.NVARCHAR, Types.LONGNVARCHAR -> "JDBCType.NVARCHAR";

            case Types.CLOB -> "JDBCType.CLOB";
            case Types.NCLOB -> "JDBCType.NCLOB";
            case Types.SQLXML -> "JDBCType.SQLXML";

            case Types.DATE -> "JDBCType.DATE";
            case Types.TIME -> "JDBCType.TIME";
            case Types.TIME_WITH_TIMEZONE -> "JDBCType.TIME_WITH_TIMEZONE";
            case Types.TIMESTAMP -> "JDBCType.TIMESTAMP";
            case Types.TIMESTAMP_WITH_TIMEZONE -> "JDBCType.TIMESTAMP_WITH_TIMEZONE";

            case Types.BINARY -> "JDBCType.BINARY";
            case Types.VARBINARY, Types.LONGVARBINARY -> "JDBCType.VARBINARY";
            case Types.BLOB -> "JDBCType.BLOB";

            case Types.OTHER -> "JDBCType.OTHER";
            default -> "JDBCType.VARCHAR";
        };
    }

    private static String rsReadExpr(String javaType, boolean nullable, String rs, String idxVar) {
        return switch (javaType) {
            case "Short"   -> nullable ? "Jdbc.rsShort(" + rs + ", " + idxVar + ")" : rs + ".getShort(" + idxVar + ")";
            case "Integer" -> nullable ? "Jdbc.rsInt(" + rs + ", " + idxVar + ")" : rs + ".getInt(" + idxVar + ")";
            case "Long"    -> nullable ? "Jdbc.rsLong(" + rs + ", " + idxVar + ")" : rs + ".getLong(" + idxVar + ")";
            case "Float"   -> nullable ? "Jdbc.rsFloat(" + rs + ", " + idxVar + ")" : rs + ".getFloat(" + idxVar + ")";
            case "Double"  -> nullable ? "Jdbc.rsDouble(" + rs + ", " + idxVar + ")" : rs + ".getDouble(" + idxVar + ")";
            case "Boolean" -> nullable ? "Jdbc.rsBool(" + rs + ", " + idxVar + ")" : rs + ".getBoolean(" + idxVar + ")";

            case "String"  -> rs + ".getString(" + idxVar + ")";
            case "byte[]"  -> rs + ".getBytes(" + idxVar + ")";
            case "BigDecimal" -> rs + ".getBigDecimal(" + idxVar + ")";
            case "Date"    -> rs + ".getDate(" + idxVar + ")";
            case "Time"    -> rs + ".getTime(" + idxVar + ")";
            case "Timestamp" -> rs + ".getTimestamp(" + idxVar + ")";

            case "OffsetDateTime" -> rs + ".getObject(" + idxVar + ", java.time.OffsetDateTime.class)";
            case "OffsetTime"     -> rs + ".getObject(" + idxVar + ", java.time.OffsetTime.class)";
            case "UUID"           -> rs + ".getObject(" + idxVar + ", java.util.UUID.class)";

            default -> rs + ".getObject(" + idxVar + ")";
        };
    }

    private static String nz(String s) {
        return s == null ? "" : s;
    }
}
