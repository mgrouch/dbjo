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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLType;
import java.sql.Types;
import java.util.*;

public final class DbMetaGenerator {
    private final Config cfg;
    private final EnumOverrideIndex enumOverrides; // nullable => no overrides

    // keep old toolchain calls compiling
    public DbMetaGenerator(Config cfg) {
        this(cfg, null);
    }

    public DbMetaGenerator(Config cfg, EnumOverrideIndex enumOverrides) {
        this.cfg = Objects.requireNonNull(cfg, "cfg");
        this.enumOverrides = enumOverrides; // may be null
    }

    public int generateAll(List<TableModel> tables) throws IOException {
        Path outDir = cfg.codegenOutJava().resolve(cfg.dbMetaPkg().replace('.', '/'));
        Files.createDirectories(outDir);

        int count = 0;
        for (TableModel tm : tables) {
            String beanClass = Naming.toClassName(tm.table().table());
            String metaClass = beanClass + "DbMeta";

            String src = render(metaClass, beanClass, tm);

            Path outFile = outDir.resolve(metaClass + ".java");
            FilesUtil.writeString(outFile, src, cfg.overwrite());
            count++;
        }
        return count;
    }

    private String render(String metaClass, String beanClass, TableModel tm) {
        String schema = tm.table().schema();
        String table  = tm.table().table();
        String fqn = (schema == null || schema.isBlank()) ? table : (schema + "." + table);

        List<Col> cols = tm.cols() == null ? List.of() : tm.cols();

        // PK columns in stable order
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

        String insertSql = buildInsertSql(fqn, insCols);
        String updateSql = buildUpdateByIdSql(fqn, updCols, pkCols);
        String selectAllSql = buildSelectAllSql(fqn, cols);

        // imports
        Set<String> imports = new TreeSet<>();
        imports.add("java.sql.ResultSet");
        imports.add("java.sql.SQLException");
        imports.add("java.sql.SQLType");
        imports.add("java.sql.JDBCType");
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

        StringBuilder sb = new StringBuilder(14_000);
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

        sb.append("    public static final ").append(metaClass)
                .append(" INSTANCE = new ").append(metaClass).append("();\n\n");
        sb.append("    private ").append(metaClass).append("() {}\n\n");

        // DbMeta interface
        sb.append("    @Override public String schema() { return SCHEMA; }\n");
        sb.append("    @Override public String table()  { return TABLE; }\n");
        sb.append("    @Override public String fqn()    { return FQN; }\n\n");
        sb.append("    @Override public String insertSql() { return INSERT_SQL; }\n");
        sb.append("    @Override public String updateByIdSql() { return UPDATE_BY_ID_SQL; }\n");
        sb.append("    @Override public String selectAllSql() { return SELECT_ALL_SQL; }\n\n");

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
                // read raw key then convert to enum
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

        // insert params/types
        sb.append("    public Object[] getInsertParameters(").append(beanClass).append(" e) {\n");
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

        sb.append("    public SQLType[] getInsertParameterTypes() {\n");
        sb.append("        return new SQLType[] {");
        for (int idx = 0; idx < insCols.size(); idx++) {
            if (idx > 0) sb.append(", ");
            sb.append(jdbcTypeExpr(insCols.get(idx).sqlType(), insCols.get(idx).typeName()));
        }
        sb.append("};\n");
        sb.append("    }\n\n");

        // update params/types
        sb.append("    public Object[] getUpdateByIdParameters(").append(beanClass).append(" e) {\n");
        sb.append("        return new Object[] {");

        boolean first = true;
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

        // DbMeta methods via above
        sb.append("    @Override public Object[] insertParams(").append(beanClass).append(" e) { return getInsertParameters(e); }\n");
        sb.append("    @Override public SQLType[] insertParamTypes() { return getInsertParameterTypes(); }\n");
        sb.append("    @Override public Object[] updateByIdParams(").append(beanClass).append(" e) { return getUpdateByIdParameters(e); }\n");
        sb.append("    @Override public SQLType[] updateByIdParamTypes() { return getUpdateByIdParameterTypes(); }\n\n");

        sb.append("    public static void bind(java.sql.PreparedStatement ps, Object[] params, SQLType[] types) throws SQLException {\n");
        sb.append("        Jdbc.bind(ps, params, types);\n");
        sb.append("    }\n");

        sb.append("}\n");
        return sb.toString();
    }

    private static boolean isNullable(Col c) {
        // robust against enum naming differences
        Nullability n = c.nullability();
        if (n == null) return true; // unknown => treat as nullable for safe reads
        String name = n.name().toUpperCase(Locale.ROOT);
        return !(name.contains("NO_NULL") || name.contains("NOT_NULL"));
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

    private static String escape(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private static String jdbcTypeExpr(int sqlType, String typeName) {
        String tn = (typeName == null) ? "" : typeName.trim().toUpperCase(Locale.ROOT);

        // MSSQL UNIQUEIDENTIFIER tends to be OTHER/CHAR/VARCHAR; safest is OTHER
        if ("UNIQUEIDENTIFIER".equals(tn)) return "JDBCType.OTHER";

        // MSSQL DATETIMEOFFSET often appears as TIMESTAMP_WITH_TIMEZONE; if not, still safe as OTHER
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
        // For getObject(..,Class), SQL NULL returns null => fine for nullable case too.
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

            // Cross-db TZ-aware types (Oracle + MSSQL drivers support these)
            case "OffsetDateTime" -> rs + ".getObject(" + idxVar + ", java.time.OffsetDateTime.class)";
            case "OffsetTime"     -> rs + ".getObject(" + idxVar + ", java.time.OffsetTime.class)";
            case "UUID"           -> rs + ".getObject(" + idxVar + ", java.util.UUID.class)";

            default -> rs + ".getObject(" + idxVar + ")";
        };
    }
}
