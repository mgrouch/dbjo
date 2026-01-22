package org.github.dbjo.codegen.db;

import org.github.dbjo.codegen.Config;
import org.github.dbjo.codegen.model.Col;
import org.github.dbjo.codegen.model.TableModel;
import org.github.dbjo.codegen.types.TypeMappings;
import org.github.dbjo.codegen.util.FilesUtil;
import org.github.dbjo.codegen.util.Naming;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DatabaseMetaData;
import java.sql.Types;
import java.util.*;

/**
 * Generates per table:
 *   <Entity>DbMeta implementing org.github.dbjo.meta.jdbc.DbMeta<T>
 *
 * IMPORTANT:
 *  - Uses shared helpers from org.github.dbjo.meta.jdbc.Jdbc (no per-class bind/rs* helpers).
 *  - Keeps SQL strings + parameter arrays/types + row mapper.
 */
public final class DbMetaGenerator {
    private final Config cfg;

    public DbMetaGenerator(Config cfg) {
        this.cfg = Objects.requireNonNull(cfg, "cfg");
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
            System.out.println("Wrote: " + outFile);
            count++;
        }

        return count;
    }

    private String render(String metaClass, String beanClass, TableModel tm) {
        String schema = tm.table().schema();
        String table  = tm.table().table();
        String fqn = (schema == null || schema.isBlank()) ? table : (schema + "." + table);

        // Column sets
        List<Col> cols = tm.cols() == null ? List.of() : tm.cols();

        // PK columns in stable order (as columns appear)
        List<Col> pkCols = new ArrayList<>();
        Set<String> pkUpper = tm.pkColsUpper() == null ? Set.of() : tm.pkColsUpper();
        for (Col c : cols) {
            if (c.colName() != null && pkUpper.contains(c.colName().toUpperCase(Locale.ROOT))) {
                pkCols.add(c);
            }
        }

        // Insert cols: exclude auto-increment
        List<Col> insCols = new ArrayList<>();
        for (Col c : cols) {
            if ("YES".equalsIgnoreCase(c.isAutoIncrement())) continue;
            insCols.add(c);
        }

        // Update cols: exclude PK and auto-increment
        List<Col> updCols = new ArrayList<>();
        for (Col c : cols) {
            if ("YES".equalsIgnoreCase(c.isAutoIncrement())) continue;
            if (c.colName() != null && pkUpper.contains(c.colName().toUpperCase(Locale.ROOT))) continue;
            updCols.add(c);
        }

        // SQL strings
        String insertSql    = buildInsertSql(fqn, insCols);
        String updateSql    = buildUpdateByIdSql(fqn, updCols, pkCols);
        String selectAllSql = buildSelectAllSql(fqn, cols);

        // imports for generated class
        Set<String> imports = new TreeSet<>();
        imports.add("java.sql.ResultSet");
        imports.add("java.sql.SQLException");
        imports.add("java.sql.SQLType");
        imports.add("java.sql.JDBCType");

        // shared runtime api
        imports.add("org.github.dbjo.meta.jdbc.DbMeta");
        imports.add("org.github.dbjo.meta.jdbc.Jdbc");

        // Import entity bean
        if (!cfg.beanPkg().equals(cfg.dbMetaPkg())) {
            imports.add(cfg.beanPkg() + "." + beanClass);
        }

        // If any java type requires extra imports (BigDecimal, Date, Time, Timestamp, UUID, etc.)
        for (Col c : cols) {
            TypeMappings.mapSqlTypeToJava(c.sqlType(), imports);
        }

        StringBuilder sb = new StringBuilder(12_000);
        sb.append("package ").append(cfg.dbMetaPkg()).append(";\n\n");
        for (String imp : imports) sb.append("import ").append(imp).append(";\n");
        sb.append("\n");

        sb.append("/**\n");
        sb.append(" * Auto-generated JDBC meta for ").append(fqn).append("\n");
        sb.append(" *\n");
        sb.append(" * Provides SQL strings + parameter lists/types + row mapper.\n");
        sb.append(" */\n");
        sb.append("public final class ").append(metaClass)
                .append(" implements DbMeta<").append(beanClass).append("> {\n\n");

        sb.append("    public static final String SCHEMA = ")
                .append(schema == null ? "null" : "\"" + escape(schema) + "\"").append(";\n");
        sb.append("    public static final String TABLE  = \"").append(escape(table)).append("\";\n");
        sb.append("    public static final String FQN    = \"").append(escape(fqn)).append("\";\n\n");

        sb.append("    public static final String INSERT_SQL = \"").append(escape(insertSql)).append("\";\n");
        sb.append("    public static final String UPDATE_BY_ID_SQL = \"").append(escape(updateSql)).append("\";\n");
        sb.append("    public static final String SELECT_ALL_SQL = \"").append(escape(selectAllSql)).append("\";\n\n");

        sb.append("    public static final ").append(metaClass).append(" INSTANCE = new ").append(metaClass).append("();\n\n");
        sb.append("    private ").append(metaClass).append("() {}\n\n");

        // DbMeta SQL accessors
        sb.append("    @Override public String insertSql() { return INSERT_SQL; }\n");
        sb.append("    @Override public String updateByIdSql() { return UPDATE_BY_ID_SQL; }\n");
        sb.append("    @Override public String selectAllSql() { return SELECT_ALL_SQL; }\n\n");

        // Row mapper: by column index in same order as SELECT_ALL_SQL list
        sb.append("    @Override\n");
        sb.append("    public ").append(beanClass).append(" fromRow(ResultSet rs) throws SQLException {\n");
        sb.append("        ").append(beanClass).append(" e = new ").append(beanClass).append("();\n");
        sb.append("        int i = 1;\n");

        for (Col c : cols) {
            String prop = Naming.sanitizeJavaIdentifier(Naming.toFieldName(c.colName()));
            String cap  = Naming.capitalize(prop);
            TypeMappings.JavaType jt = TypeMappings.mapSqlTypeToJava(c.sqlType(), null);

            boolean nullable = c.nullable() != DatabaseMetaData.columnNoNulls;
            String readExpr = rsReadExpr(jt.javaType(), nullable, "rs", "i");

            sb.append("        e.set").append(cap).append("(").append(readExpr).append(");\n");
            sb.append("        i++;\n");
        }

        sb.append("        return e;\n");
        sb.append("    }\n\n");

        // INSERT params + types
        sb.append("    @Override\n");
        sb.append("    public Object[] insertParams(").append(beanClass).append(" e) {\n");
        sb.append("        return new Object[] {");
        for (int idx = 0; idx < insCols.size(); idx++) {
            Col c = insCols.get(idx);
            String prop = Naming.sanitizeJavaIdentifier(Naming.toFieldName(c.colName()));
            String cap = Naming.capitalize(prop);
            if (idx > 0) sb.append(", ");
            sb.append("e.get").append(cap).append("()");
        }
        sb.append("};\n");
        sb.append("    }\n\n");

        sb.append("    @Override\n");
        sb.append("    public SQLType[] insertParamTypes() {\n");
        sb.append("        return new SQLType[] {");
        for (int idx = 0; idx < insCols.size(); idx++) {
            Col c = insCols.get(idx);
            if (idx > 0) sb.append(", ");
            sb.append(jdbcTypeExpr(c.sqlType()));
        }
        sb.append("};\n");
        sb.append("    }\n\n");

        // UPDATE params + types
        sb.append("    @Override\n");
        sb.append("    public Object[] updateByIdParams(").append(beanClass).append(" e) {\n");
        sb.append("        return new Object[] {");

        boolean first = true;
        for (Col c : updCols) {
            String prop = Naming.sanitizeJavaIdentifier(Naming.toFieldName(c.colName()));
            String cap = Naming.capitalize(prop);
            if (!first) sb.append(", ");
            sb.append("e.get").append(cap).append("()");
            first = false;
        }
        for (Col c : pkCols) {
            String prop = Naming.sanitizeJavaIdentifier(Naming.toFieldName(c.colName()));
            String cap = Naming.capitalize(prop);
            if (!first) sb.append(", ");
            sb.append("e.get").append(cap).append("()");
            first = false;
        }

        sb.append("};\n");
        sb.append("    }\n\n");

        sb.append("    @Override\n");
        sb.append("    public SQLType[] updateByIdParamTypes() {\n");
        sb.append("        return new SQLType[] {");

        first = true;
        for (Col c : updCols) {
            if (!first) sb.append(", ");
            sb.append(jdbcTypeExpr(c.sqlType()));
            first = false;
        }
        for (Col c : pkCols) {
            if (!first) sb.append(", ");
            sb.append(jdbcTypeExpr(c.sqlType()));
            first = false;
        }

        sb.append("};\n");
        sb.append("    }\n\n");

        sb.append("}\n");
        return sb.toString();
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
            // Avoid invalid SQL; you can decide whether to throw instead.
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

    private static String jdbcTypeExpr(int sqlType) {
        // Map java.sql.Types -> JDBCType constant (best-effort)
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
            case Types.VARCHAR, Types.LONGVARCHAR -> "JDBCType.VARCHAR";
            case Types.NCHAR -> "JDBCType.NCHAR";
            case Types.NVARCHAR, Types.LONGNVARCHAR -> "JDBCType.NVARCHAR";
            case Types.DATE -> "JDBCType.DATE";
            case Types.TIME, Types.TIME_WITH_TIMEZONE -> "JDBCType.TIME";
            case Types.TIMESTAMP, Types.TIMESTAMP_WITH_TIMEZONE -> "JDBCType.TIMESTAMP";
            case Types.BINARY -> "JDBCType.BINARY";
            case Types.VARBINARY, Types.LONGVARBINARY -> "JDBCType.VARBINARY";
            case Types.BLOB -> "JDBCType.BLOB";
            default -> "JDBCType.VARCHAR";
        };
    }

    /**
     * Expression to read a column at index {@code idxVar} from {@code rsVar}.
     * Uses shared helper Jdbc.rsX(...) for nullable primitive-wrapper reads.
     */
    private static String rsReadExpr(String javaType, boolean nullable, String rsVar, String idxVar) {
        return switch (javaType) {
            case "Short"   -> nullable ? "Jdbc.rsShort(" + rsVar + ", " + idxVar + ")" : rsVar + ".getShort(" + idxVar + ")";
            case "Integer" -> nullable ? "Jdbc.rsInt(" + rsVar + ", " + idxVar + ")" : rsVar + ".getInt(" + idxVar + ")";
            case "Long"    -> nullable ? "Jdbc.rsLong(" + rsVar + ", " + idxVar + ")" : rsVar + ".getLong(" + idxVar + ")";
            case "Float"   -> nullable ? "Jdbc.rsFloat(" + rsVar + ", " + idxVar + ")" : rsVar + ".getFloat(" + idxVar + ")";
            case "Double"  -> nullable ? "Jdbc.rsDouble(" + rsVar + ", " + idxVar + ")" : rsVar + ".getDouble(" + idxVar + ")";
            case "Boolean" -> nullable ? "Jdbc.rsBool(" + rsVar + ", " + idxVar + ")" : rsVar + ".getBoolean(" + idxVar + ")";
            case "String"  -> rsVar + ".getString(" + idxVar + ")";
            case "byte[]"  -> rsVar + ".getBytes(" + idxVar + ")";
            case "BigDecimal" -> rsVar + ".getBigDecimal(" + idxVar + ")";
            case "Date"    -> rsVar + ".getDate(" + idxVar + ")";
            case "Time"    -> rsVar + ".getTime(" + idxVar + ")";
            case "Timestamp" -> rsVar + ".getTimestamp(" + idxVar + ")";
            default -> rsVar + ".getObject(" + idxVar + ")";
        };
    }
}
