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
import java.sql.SQLType;
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
        this.enumOverrides = enumOverrides;
    }

    public int generateAll(List<TableModel> tables) throws IOException {
        Objects.requireNonNull(tables, "tables");

        Path outDir = cfg.codegenOutJava().resolve(cfg.dbMetaPkg().replace('.', '/'));
        Files.createDirectories(outDir);

        String registryPkg = cfg.dbMetaPkg() + ".registry";
        Path regOutDir = cfg.codegenOutJava().resolve(registryPkg.replace('.', '/'));
        Files.createDirectories(regOutDir);

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

        String regCls = "DbMetas";
        String regSrc = renderRegistry(registryPkg, regCls, cfg.dbMetaPkg(), metaClassNames);
        Path regFile = regOutDir.resolve(regCls + ".java");
        FilesUtil.writeString(regFile, regSrc, cfg.overwrite());
        count++;

        return count;
    }

    private String render(String metaClass, String beanClass, TableModel tm) {
        String schema = tm.table().schema();
        String table  = tm.table().table();
        String fqn = (schema == null || schema.isBlank()) ? table : (schema + "." + table);

        List<Col> cols = tm.cols() == null ? List.of() : tm.cols();

        List<Col> pkCols = new ArrayList<>();
        Set<String> pkUpper = tm.pkColsUpper() == null ? Set.of() : tm.pkColsUpper();
        for (Col c : cols) {
            if (c.colName() != null && pkUpper.contains(c.colName().toUpperCase(Locale.ROOT))) pkCols.add(c);
        }

        List<Col> insCols = new ArrayList<>();
        for (Col c : cols) if (!c.autoIncrement()) insCols.add(c);

        List<Col> updCols = new ArrayList<>();
        for (Col c : cols) {
            if (c.autoIncrement()) continue;
            if (c.colName() != null && pkUpper.contains(c.colName().toUpperCase(Locale.ROOT))) continue;
            updCols.add(c);
        }

        List<Col> mergeCols = buildMergeCols(pkCols, insCols, updCols);

        String insertSql = buildInsertSql(fqn, insCols);
        String updateSql = buildUpdateByIdSql(fqn, updCols, pkCols);
        String selectAllSql = buildSelectAllSql(fqn, cols);

        String upsertMssql  = buildUpsertMssqlSybase(fqn, mergeCols, pkCols, updCols, insCols, true);
        String upsertSybase = buildUpsertMssqlSybase(fqn, mergeCols, pkCols, updCols, insCols, false);
        String upsertOracle = buildUpsertOracle(fqn, mergeCols, pkCols, updCols, insCols); // batch this for Oracle
        String upsertHsql   = buildUpsertHsql(fqn, mergeCols, pkCols, updCols, insCols);

        // temp-table defs (used only by MSSQL/SYBASE runtime path)
        String tempColDefs = buildTempColDefs(mergeCols);
        String tempInsertCols = joinColNames(mergeCols);
        int tempParamCount = mergeCols.size();

        String mergeFromTempMssql  = buildMergeFromTempTpl(fqn, pkCols, updCols, insCols, true);
        String mergeFromTempSybase = buildMergeFromTempTpl(fqn, pkCols, updCols, insCols, false);

        // imports (keep clean: no unused)
        Set<String> imports = new TreeSet<>();
        imports.add("java.sql.JDBCType");
        imports.add("java.sql.ResultSet");
        imports.add("java.sql.SQLException");
        imports.add("java.sql.SQLType");
        imports.add("org.github.dbjo.meta.jdbc.DbMetaUpsertSupport");

        if (!cfg.beanPkg().equals(cfg.dbMetaPkg())) imports.add(cfg.beanPkg() + "." + beanClass);

        for (Col c : cols) TypeMappings.mapSqlTypeToJava(c.sqlType(), c.typeName(), imports);

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
                .append(" extends DbMetaUpsertSupport<").append(beanClass).append("> {\n\n");

        sb.append("    public static final String SCHEMA = ")
                .append(schema == null ? "null" : "\"" + escape(schema) + "\"").append(";\n");
        sb.append("    public static final String TABLE  = \"").append(escape(table)).append("\";\n");
        sb.append("    public static final String FQN    = \"").append(escape(fqn)).append("\";\n\n");

        sb.append("    public static final String INSERT_SQL = \"").append(escape(insertSql)).append("\";\n");
        sb.append("    public static final String UPDATE_BY_ID_SQL = \"").append(escape(updateSql)).append("\";\n");
        sb.append("    public static final String SELECT_ALL_SQL = \"").append(escape(selectAllSql)).append("\";\n\n");

        sb.append("    private static final String UPSERT_BY_ID_SQL_MSSQL = \"").append(escape(upsertMssql)).append("\";\n");
        sb.append("    private static final String UPSERT_BY_ID_SQL_SYBASE = \"").append(escape(upsertSybase)).append("\";\n");
        sb.append("    private static final String UPSERT_BY_ID_SQL_ORACLE = \"").append(escape(upsertOracle)).append("\";\n");
        sb.append("    private static final String UPSERT_BY_ID_SQL_HSQL   = \"").append(escape(upsertHsql)).append("\";\n\n");

        sb.append("    private static final String UPSERT_TEMP_COL_DEFS = \"").append(escape(tempColDefs)).append("\";\n");
        sb.append("    private static final String UPSERT_TEMP_INSERT_COLS = \"").append(escape(tempInsertCols)).append("\";\n");
        sb.append("    private static final int UPSERT_TEMP_PARAM_COUNT = ").append(tempParamCount).append(";\n\n");

        // MSSQL/SYBASE only
        sb.append("    private static final String MERGE_FROM_TEMP_MSSQL_TPL = \"").append(escape(mergeFromTempMssql)).append("\";\n");
        sb.append("    private static final String MERGE_FROM_TEMP_SYBASE_TPL = \"").append(escape(mergeFromTempSybase)).append("\";\n\n");

        sb.append("    private static final SQLType[] INSERT_PARAM_TYPES = new SQLType[] {");
        for (int i = 0; i < insCols.size(); i++) {
            if (i > 0) sb.append(", ");
            sb.append(jdbcTypeExpr(insCols.get(i).sqlType(), insCols.get(i).typeName()));
        }
        sb.append("};\n");

        sb.append("    private static final SQLType[] UPDATE_BY_ID_PARAM_TYPES = new SQLType[] {");
        boolean first = true;
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

        sb.append("    private static final SQLType[] UPSERT_BY_ID_PARAM_TYPES = new SQLType[] {");
        for (int i = 0; i < mergeCols.size(); i++) {
            if (i > 0) sb.append(", ");
            sb.append(jdbcTypeExpr(mergeCols.get(i).sqlType(), mergeCols.get(i).typeName()));
        }
        sb.append("};\n\n");

        sb.append("    public static final ").append(metaClass).append(" INSTANCE = new ").append(metaClass).append("();\n\n");
        sb.append("    private ").append(metaClass).append("() {}\n\n");

        sb.append("    @Override public String schema() { return SCHEMA; }\n");
        sb.append("    @Override public String table()  { return TABLE; }\n");
        sb.append("    @Override public String fqn()    { return FQN; }\n\n");

        sb.append("    @Override public String insertSql() { return INSERT_SQL; }\n");
        sb.append("    @Override public String updateByIdSql() { return UPDATE_BY_ID_SQL; }\n");
        sb.append("    @Override public String selectAllSql() { return SELECT_ALL_SQL; }\n\n");

        sb.append("    @Override protected String upsertByIdSqlMssql() { return UPSERT_BY_ID_SQL_MSSQL; }\n");
        sb.append("    @Override protected String upsertByIdSqlSybase() { return UPSERT_BY_ID_SQL_SYBASE; }\n");
        sb.append("    @Override protected String upsertByIdSqlOracle() { return UPSERT_BY_ID_SQL_ORACLE; }\n");
        sb.append("    @Override protected String upsertByIdSqlHsql() { return UPSERT_BY_ID_SQL_HSQL; }\n\n");

        sb.append("    @Override protected String upsertTempColDefs() { return UPSERT_TEMP_COL_DEFS; }\n");
        sb.append("    @Override protected String upsertTempInsertColumns() { return UPSERT_TEMP_INSERT_COLS; }\n");
        sb.append("    @Override protected int upsertTempParamCount() { return UPSERT_TEMP_PARAM_COUNT; }\n\n");

        sb.append("    @Override protected String mergeFromTempTplMssql() { return MERGE_FROM_TEMP_MSSQL_TPL; }\n");
        sb.append("    @Override protected String mergeFromTempTplSybase() { return MERGE_FROM_TEMP_SYBASE_TPL; }\n\n");

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
                        .append("(").append(rawExpr).append("))")
                        .append(";\n");
            } else {
                TypeMappings.JavaType jt = TypeMappings.mapSqlTypeToJava(c.sqlType(), c.typeName(), null);
                String readExpr = rsReadExpr(jt.javaType(), nullable, "rs", "i");
                sb.append("        e.set").append(cap).append("(").append(readExpr).append(");\n");
            }
            sb.append("        i++;\n");
        }

        sb.append("        return e;\n");
        sb.append("    }\n\n");

        sb.append("    @Override\n");
        sb.append("    public Object[] insertParams(").append(beanClass).append(" e) {\n");
        sb.append("        return new Object[] {");
        for (int idx = 0; idx < insCols.size(); idx++) {
            Col c = insCols.get(idx);
            if (idx > 0) sb.append(", ");

            String prop = Naming.sanitizeJavaIdentifier(Naming.toFieldName(c.colName()));
            String cap = Naming.capitalize(prop);

            EnumOverrideIndex.Binding eb = (enumOverrides == null) ? null : enumOverrides.find(schema, table, c.colName());
            if (eb != null) {
                sb.append("e.get").append(cap).append("() == null ? null : e.get")
                        .append(cap).append("().").append(eb.keyGetterMethod()).append("()");
            } else {
                sb.append("e.get").append(cap).append("()");
            }
        }
        sb.append("};\n");
        sb.append("    }\n\n");

        sb.append("    @Override public SQLType[] insertParamTypes() { return INSERT_PARAM_TYPES; }\n\n");

        sb.append("    @Override\n");
        sb.append("    public Object[] updateByIdParams(").append(beanClass).append(" e) {\n");
        sb.append("        return new Object[] {");

        first = true;
        for (Col c : updCols) {
            if (!first) sb.append(", ");
            first = false;

            String prop = Naming.sanitizeJavaIdentifier(Naming.toFieldName(c.colName()));
            String cap = Naming.capitalize(prop);

            EnumOverrideIndex.Binding eb = (enumOverrides == null) ? null : enumOverrides.find(schema, table, c.colName());
            if (eb != null) {
                sb.append("e.get").append(cap).append("() == null ? null : e.get")
                        .append(cap).append("().").append(eb.keyGetterMethod()).append("()");
            } else {
                sb.append("e.get").append(cap).append("()");
            }
        }
        for (Col c : pkCols) {
            if (!first) sb.append(", ");
            first = false;

            String prop = Naming.sanitizeJavaIdentifier(Naming.toFieldName(c.colName()));
            String cap = Naming.capitalize(prop);

            EnumOverrideIndex.Binding eb = (enumOverrides == null) ? null : enumOverrides.find(schema, table, c.colName());
            if (eb != null) {
                sb.append("e.get").append(cap).append("() == null ? null : e.get")
                        .append(cap).append("().").append(eb.keyGetterMethod()).append("()");
            } else {
                sb.append("e.get").append(cap).append("()");
            }
        }

        sb.append("};\n");
        sb.append("    }\n\n");

        sb.append("    @Override public SQLType[] updateByIdParamTypes() { return UPDATE_BY_ID_PARAM_TYPES; }\n\n");

        sb.append("    @Override\n");
        sb.append("    public Object[] upsertByIdParams(").append(beanClass).append(" e) {\n");
        sb.append("        return new Object[] {");
        for (int i = 0; i < mergeCols.size(); i++) {
            if (i > 0) sb.append(", ");
            Col c = mergeCols.get(i);

            String prop = Naming.sanitizeJavaIdentifier(Naming.toFieldName(c.colName()));
            String cap  = Naming.capitalize(prop);

            EnumOverrideIndex.Binding eb = (enumOverrides == null) ? null : enumOverrides.find(schema, table, c.colName());
            if (eb != null) {
                sb.append("e.get").append(cap).append("() == null ? null : e.get")
                        .append(cap).append("().").append(eb.keyGetterMethod()).append("()");
            } else {
                sb.append("e.get").append(cap).append("()");
            }
        }
        sb.append("};\n");
        sb.append("    }\n\n");

        sb.append("    @Override public SQLType[] upsertByIdParamTypes() { return UPSERT_BY_ID_PARAM_TYPES; }\n");

        sb.append("}\n");
        return sb.toString();
    }

    private static String renderRegistry(String pkg, String cls, String metasPkg, List<String> metaClassNames) {
        StringBuilder sb = new StringBuilder(10_000);
        sb.append("package ").append(pkg).append(";\n\n");
        sb.append("import java.util.*;\n");
        sb.append("import org.github.dbjo.meta.jdbc.DbMeta;\n");
        sb.append("import ").append(metasPkg).append(".*;\n\n");

        sb.append("public final class ").append(cls).append(" {\n\n");
        sb.append("    private ").append(cls).append("() {}\n\n");
        sb.append("    public static final List<DbMeta<?>> ALL;\n");
        sb.append("    public static final Map<String, DbMeta<?>> BY_FQN;\n\n");

        sb.append("    static {\n");
        sb.append("        List<DbMeta<?>> a = new ArrayList<>();\n");
        for (String m : metaClassNames) sb.append("        a.add(").append(m).append(".INSTANCE);\n");
        sb.append("        ALL = Collections.unmodifiableList(a);\n\n");

        sb.append("        Map<String, DbMeta<?>> m = new HashMap<>();\n");
        sb.append("        for (DbMeta<?> dm : a) m.put(norm(dm.fqn()), dm);\n");
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

        sb.append("    private static String norm(String s) { return s.trim().toLowerCase(Locale.ROOT); }\n");
        sb.append("}\n");
        return sb.toString();
    }

    // ---------------- generator helpers ----------------

    private static boolean isNullable(Col c) {
        Nullability n = c.nullability();
        return n == null || n == Nullability.NULLABLE || n == Nullability.UNKNOWN;
    }

    private static List<Col> buildMergeCols(List<Col> pkCols, List<Col> insCols, List<Col> updCols) {
        LinkedHashMap<String, Col> m = new LinkedHashMap<>();
        for (Col c : pkCols) m.put(key(c), c);
        for (Col c : updCols) m.putIfAbsent(key(c), c);
        for (Col c : insCols) m.putIfAbsent(key(c), c);
        return new ArrayList<>(m.values());
    }

    private static String key(Col c) {
        return (c == null || c.colName() == null) ? "" : c.colName().toUpperCase(Locale.ROOT);
    }

    private static String joinColNames(List<Col> cols) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < cols.size(); i++) {
            if (i > 0) sb.append(", ");
            sb.append(cols.get(i).colName());
        }
        return sb.toString();
    }

    private static String buildTempColDefs(List<Col> cols) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < cols.size(); i++) {
            Col c = cols.get(i);
            if (i > 0) sb.append(", ");
            sb.append(c.colName()).append(" ").append(typeDecl(c));
            if (c.nullability() == Nullability.NO_NULLS) sb.append(" NOT NULL");
        }
        return sb.toString();
    }

    private static String typeDecl(Col c) {
        String tn = (c.typeName() == null) ? "" : c.typeName().trim();
        String up = tn.toUpperCase(Locale.ROOT);

        int size = c.size();
        int scale = c.scale();

        if (up.contains("CHAR") || up.contains("BINARY")) {
            if (size > 0 && !up.contains("(")) return tn + "(" + size + ")";
        }
        if (up.contains("DECIMAL") || up.contains("NUMERIC")) {
            if (size > 0 && !up.contains("(")) {
                if (scale > 0) return tn + "(" + size + "," + scale + ")";
                return tn + "(" + size + ")";
            }
        }
        return tn.isBlank() ? "VARCHAR(255)" : tn;
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

    private static void appendUpdateSetOrNoop(StringBuilder sb, List<Col> updCols, List<Col> pkCols, String tAlias, String sAlias) {
        if (!updCols.isEmpty()) {
            for (int i = 0; i < updCols.size(); i++) {
                if (i > 0) sb.append(", ");
                sb.append(tAlias).append(".").append(updCols.get(i).colName())
                        .append("=").append(sAlias).append(".").append(updCols.get(i).colName());
            }
            return;
        }
        // No update columns => emit a no-op assignment to keep SQL valid
        if (!pkCols.isEmpty()) {
            String c = pkCols.get(0).colName();
            sb.append(tAlias).append(".").append(c).append("=").append(tAlias).append(".").append(c);
        } else {
            sb.append("1=1"); // should not happen in a real MERGE, but keeps string non-empty
        }
    }

    private static String buildUpsertMssqlSybase(String fqn, List<Col> mergeCols, List<Col> pkCols, List<Col> updCols, List<Col> insCols, boolean mssql) {
        StringBuilder sb = new StringBuilder(2000);
        sb.append("MERGE INTO ").append(fqn).append(" AS t USING (VALUES (");
        for (int i = 0; i < mergeCols.size(); i++) {
            if (i > 0) sb.append(", ");
            sb.append("?");
        }
        sb.append(")) AS s (").append(joinColNames(mergeCols)).append(") ON (");
        for (int i = 0; i < pkCols.size(); i++) {
            if (i > 0) sb.append(" AND ");
            sb.append("t.").append(pkCols.get(i).colName()).append("=s.").append(pkCols.get(i).colName());
        }
        sb.append(") WHEN MATCHED THEN UPDATE SET ");
        appendUpdateSetOrNoop(sb, updCols, pkCols, "t", "s");
        sb.append(" WHEN NOT MATCHED THEN INSERT (");
        sb.append(joinColNames(insCols)).append(") VALUES (");
        for (int i = 0; i < insCols.size(); i++) {
            if (i > 0) sb.append(", ");
            sb.append("s.").append(insCols.get(i).colName());
        }
        sb.append(")");
        if (mssql) sb.append(";");
        return sb.toString();
    }

    private static String buildUpsertOracle(String fqn, List<Col> mergeCols, List<Col> pkCols, List<Col> updCols, List<Col> insCols) {
        StringBuilder sb = new StringBuilder(2400);
        sb.append("MERGE INTO ").append(fqn).append(" t USING (SELECT ");
        for (int i = 0; i < mergeCols.size(); i++) {
            if (i > 0) sb.append(", ");
            sb.append("? AS ").append(mergeCols.get(i).colName());
        }
        sb.append(" FROM dual) s ON (");
        for (int i = 0; i < pkCols.size(); i++) {
            if (i > 0) sb.append(" AND ");
            sb.append("t.").append(pkCols.get(i).colName()).append("=s.").append(pkCols.get(i).colName());
        }
        sb.append(") WHEN MATCHED THEN UPDATE SET ");
        appendUpdateSetOrNoop(sb, updCols, pkCols, "t", "s");
        sb.append(" WHEN NOT MATCHED THEN INSERT (").append(joinColNames(insCols)).append(") VALUES (");
        for (int i = 0; i < insCols.size(); i++) {
            if (i > 0) sb.append(", ");
            sb.append("s.").append(insCols.get(i).colName());
        }
        sb.append(")");
        return sb.toString();
    }

    private static String buildUpsertHsql(String fqn, List<Col> mergeCols, List<Col> pkCols, List<Col> updCols, List<Col> insCols) {
        StringBuilder sb = new StringBuilder(2600);
        sb.append("MERGE INTO ").append(fqn).append(" AS t USING (VALUES (");
        for (int i = 0; i < mergeCols.size(); i++) {
            if (i > 0) sb.append(", ");
            sb.append(hsqlCastExpr(mergeCols.get(i)));
        }
        sb.append(")) AS s (").append(joinColNames(mergeCols)).append(") ON (");
        for (int i = 0; i < pkCols.size(); i++) {
            if (i > 0) sb.append(" AND ");
            sb.append("t.").append(pkCols.get(i).colName()).append("=s.").append(pkCols.get(i).colName());
        }
        sb.append(") WHEN MATCHED THEN UPDATE SET ");
        appendUpdateSetOrNoop(sb, updCols, pkCols, "t", "s");
        sb.append(" WHEN NOT MATCHED THEN INSERT (").append(joinColNames(insCols)).append(") VALUES (");
        for (int i = 0; i < insCols.size(); i++) {
            if (i > 0) sb.append(", ");
            sb.append("s.").append(insCols.get(i).colName());
        }
        sb.append(")");
        return sb.toString();
    }

    private static String hsqlCastExpr(Col c) {
        String tn = (c.typeName() == null) ? "" : c.typeName().trim().toUpperCase(Locale.ROOT);
        if (tn.isBlank()) return "?";
        String castType = tn;

        if ((tn.contains("CHAR") || tn.contains("BINARY")) && c.size() > 0 && !tn.contains("(")) {
            castType = tn + "(" + c.size() + ")";
        } else if ((tn.contains("DECIMAL") || tn.contains("NUMERIC")) && c.size() > 0 && !tn.contains("(")) {
            if (c.scale() > 0) castType = tn + "(" + c.size() + "," + c.scale() + ")";
            else castType = tn + "(" + c.size() + ")";
        }
        return "CAST(? AS " + castType + ")";
    }

    private static String buildMergeFromTempTpl(String fqn, List<Col> pkCols, List<Col> updCols, List<Col> insCols, boolean mssql) {
        StringBuilder sb = new StringBuilder(1800);
        sb.append("MERGE INTO ").append(fqn).append(" AS t USING {TEMP} AS s ON (");
        for (int i = 0; i < pkCols.size(); i++) {
            if (i > 0) sb.append(" AND ");
            sb.append("t.").append(pkCols.get(i).colName()).append("=s.").append(pkCols.get(i).colName());
        }
        sb.append(") WHEN MATCHED THEN UPDATE SET ");
        appendUpdateSetOrNoop(sb, updCols, pkCols, "t", "s");
        sb.append(" WHEN NOT MATCHED THEN INSERT (").append(joinColNames(insCols)).append(") VALUES (");
        for (int i = 0; i < insCols.size(); i++) {
            if (i > 0) sb.append(", ");
            sb.append("s.").append(insCols.get(i).colName());
        }
        sb.append(")");
        if (mssql) sb.append(";");
        return sb.toString();
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
            case "Short"   -> nullable ? "org.github.dbjo.meta.jdbc.Jdbc.rsShort(" + rs + ", " + idxVar + ")" : rs + ".getShort(" + idxVar + ")";
            case "Integer" -> nullable ? "org.github.dbjo.meta.jdbc.Jdbc.rsInt(" + rs + ", " + idxVar + ")" : rs + ".getInt(" + idxVar + ")";
            case "Long"    -> nullable ? "org.github.dbjo.meta.jdbc.Jdbc.rsLong(" + rs + ", " + idxVar + ")" : rs + ".getLong(" + idxVar + ")";
            case "Float"   -> nullable ? "org.github.dbjo.meta.jdbc.Jdbc.rsFloat(" + rs + ", " + idxVar + ")" : rs + ".getFloat(" + idxVar + ")";
            case "Double"  -> nullable ? "org.github.dbjo.meta.jdbc.Jdbc.rsDouble(" + rs + ", " + idxVar + ")" : rs + ".getDouble(" + idxVar + ")";
            case "Boolean" -> nullable ? "org.github.dbjo.meta.jdbc.Jdbc.rsBool(" + rs + ", " + idxVar + ")" : rs + ".getBoolean(" + idxVar + ")";
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

    private static String nz(String s) { return s == null ? "" : s; }
}
