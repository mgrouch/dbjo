package org.github.dbjo.codegen.rdb;

import org.github.dbjo.codegen.Config;
import org.github.dbjo.meta.db.IndexModel;
import org.github.dbjo.meta.db.TableModel;
import org.github.dbjo.codegen.util.FilesUtil;
import org.github.dbjo.codegen.util.Naming;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Generates a Rocks JDBC catalog with rich metadata for IntelliJ/DataGrip.
 *
 * Key points:
 *  - Implements RocksJdbcCatalog.table(String) + requireTable(String)
 *  - Emits RocksJdbcColumn using the *actual* record ctor:
 *      (int ordinalPos, String colName, int dataType, String typeName, int colSize, int decDigits,
 *       int nullable, String isAutoInc, String columnDef, String getterName)
 *  - Avoids proto imports to prevent entity/proto name clashes; uses fully-qualified proto type in decoder.
 *  - Uses reflection to read column fields so it works even if your Col model differs slightly across modules.
 */
public final class RocksJdbcCatalogGenerator {
    private final Config cfg;

    public RocksJdbcCatalogGenerator(Config cfg) {
        this.cfg = Objects.requireNonNull(cfg, "cfg");
    }

    public int generate(List<TableModel> tables) throws IOException {
        Objects.requireNonNull(tables, "tables");

        String jdbcPkg = toJdbcPkg(cfg.schemaPkg());
        Path outDir = cfg.codegenOutJava().resolve(jdbcPkg.replace('.', '/'));
        Files.createDirectories(outDir);

        String className = "GeneratedRocksJdbcCatalog";
        String src = renderCatalog(jdbcPkg, className, tables);

        Path outFile = outDir.resolve(className + ".java");
        FilesUtil.writeString(outFile, src, cfg.overwrite());
        System.out.println("Wrote: " + outFile);

        return 1;
    }

    private static String toJdbcPkg(String schemaPkg) {
        if (schemaPkg == null || schemaPkg.isBlank()) return "org.github.dbjo.generated.rdb.jdbc";
        if (schemaPkg.endsWith(".schema")) return schemaPkg.substring(0, schemaPkg.length() - ".schema".length()) + ".jdbc";
        return schemaPkg + ".jdbc";
    }

    private String renderCatalog(String outPkg, String className, List<TableModel> tables) {
        StringBuilder sb = new StringBuilder(30_000);

        sb.append("package ").append(outPkg).append(";\n\n");

        // Runtime JDBC catalog types
        sb.append("import org.github.dbjo.rdb.jdbc.catalog.RocksJdbcCatalog;\n");
        sb.append("import org.github.dbjo.rdb.jdbc.catalog.RocksJdbcColumn;\n");
        sb.append("import org.github.dbjo.rdb.jdbc.catalog.RocksJdbcDecoder;\n");
        sb.append("import org.github.dbjo.rdb.jdbc.catalog.RocksJdbcIndex;\n");
        sb.append("import org.github.dbjo.rdb.jdbc.catalog.RocksJdbcTable;\n\n");

        sb.append("import java.sql.SQLException;\n");
        sb.append("import java.util.*;\n\n");

        // Entity + mapper imports only (NO proto imports to avoid collisions)
        Set<String> extraImports = new TreeSet<>();
        for (TableModel tm : tables) {
            String beanClass = Naming.toClassName(tm.table().table());
            String mapperClass = beanClass + cfg.protoMapperSuffix();

            extraImports.add(cfg.beanPkg() + "." + beanClass);
            extraImports.add(cfg.protoMapperPkg() + "." + mapperClass);
        }
        extraImports.add("com.google.protobuf.InvalidProtocolBufferException");

        for (String imp : extraImports) {
            if (imp.startsWith(outPkg + ".")) continue;
            sb.append("import ").append(imp).append(";\n");
        }
        sb.append("\n");

        sb.append("public final class ").append(className).append(" implements RocksJdbcCatalog {\n");
        sb.append("    private static final List<RocksJdbcTable> TABLES = List.of(\n");

        for (int i = 0; i < tables.size(); i++) {
            String beanClass = Naming.toClassName(tables.get(i).table().table());
            sb.append("            table_").append(beanClass).append("()");
            sb.append(i < tables.size() - 1 ? ",\n" : "\n");
        }
        sb.append("    );\n\n");

        sb.append("    private static final Map<String, RocksJdbcTable> BY_NAME = buildByName(TABLES);\n\n");

        sb.append("    @Override\n");
        sb.append("    public List<RocksJdbcTable> tables() {\n");
        sb.append("        return TABLES;\n");
        sb.append("    }\n\n");

        // REQUIRED by your interface (compile error): table(String)
        sb.append("    @Override\n");
        sb.append("    public RocksJdbcTable table(String name) {\n");
        sb.append("        if (name == null) return null;\n");
        sb.append("        String k = name.trim().toLowerCase(Locale.ROOT);\n");
        sb.append("        if (k.isEmpty()) return null;\n");
        sb.append("        return BY_NAME.get(k);\n");
        sb.append("    }\n\n");

        sb.append("    @Override\n");
        sb.append("    public RocksJdbcTable requireTable(String name) throws SQLException {\n");
        sb.append("        RocksJdbcTable t = table(name);\n");
        sb.append("        if (t != null) return t;\n");
        sb.append("        throw new SQLException(\"Unknown table: \" + name);\n");
        sb.append("    }\n\n");

        sb.append("    private static Map<String, RocksJdbcTable> buildByName(List<RocksJdbcTable> tables) {\n");
        sb.append("        Map<String, RocksJdbcTable> m = new HashMap<>();\n");
        sb.append("        for (RocksJdbcTable t : tables) {\n");
        sb.append("            for (String n : t.names()) {\n");
        sb.append("                if (n == null) continue;\n");
        sb.append("                String k = n.trim().toLowerCase(Locale.ROOT);\n");
        sb.append("                if (!k.isEmpty()) m.putIfAbsent(k, t);\n");
        sb.append("            }\n");
        sb.append("        }\n");
        sb.append("        return m;\n");
        sb.append("    }\n\n");

        for (TableModel tm : tables) {
            renderTableMethod(sb, tm);
        }

        sb.append("}\n");
        return sb.toString();
    }

    private void renderTableMethod(StringBuilder sb, TableModel tm) {
        String rawTable = tm.table().table();
        String schema = tm.table().schema();

        String beanClass = Naming.toClassName(rawTable);
        String mapperClass = beanClass + cfg.protoMapperSuffix();

        // SQL-visible name
        String tableName = Naming.toLowerSnake(rawTable);
        // CF name: keep in sync with RocksSchemaGenerator convention
        String cfName = Naming.toLowerSnake(rawTable);
        String schemaName = (schema == null ? "" : schema);

        // Work with cols via reflection to survive different Col models across modules
        List<?> cols = new ArrayList<>(tm.cols());
        cols.sort(Comparator.comparingInt(c -> intVal(c, 0, "pos", "ordinalPosition", "position")));

        // PK columns in ordinal order
        List<String> pkCols = new ArrayList<>();
        Set<String> pkUpper = tm.pkColsUpper() == null ? Set.of() : tm.pkColsUpper();
        for (Object c : cols) {
            String cn = strVal(c, "colName", "columnName", "name");
            if (cn == null) continue;
            if (pkUpper.contains(cn.toUpperCase(Locale.ROOT))) pkCols.add(cn);
        }

        // Indexes (skip pure PK index duplication)
        List<IndexModel> idxs = tm.indexes() == null ? List.of() : tm.indexes();
        List<IndexModel> idxFiltered = new ArrayList<>();
        for (IndexModel ix : idxs) {
            if (ix == null) continue;
            if (isPkIndex(ix, pkUpper)) continue;
            idxFiltered.add(ix);
        }

        sb.append("    private static RocksJdbcTable table_").append(beanClass).append("() {\n");

        // Columns array: IMPORTANT: match RocksJdbcColumn record ctor (10 args)
        sb.append("        RocksJdbcColumn[] cols = new RocksJdbcColumn[] {\n");
        for (int i = 0; i < cols.size(); i++) {
            Object c = cols.get(i);

            int ordinal = intVal(c, i + 1, "pos", "ordinalPosition", "position");
            String colName = strVal(c, "colName", "columnName", "name");
            int dataType = intVal(c, java.sql.Types.VARCHAR, "sqlType", "dataType", "jdbcType");
            String typeName = strVal(c, "typeName", "sqlTypeName", "dbTypeName");
            int colSize = intVal(c, 0, "size", "columnSize", "precision");
            int decDigits = intVal(c, 0, "scale", "decimalDigits");
            int nullable = nullableInt(c);
            String isAutoInc = strValOr(c, "NO", "isAutoIncrement", "autoIncrement");
            String columnDef = strVal(c, "defaultValue", "columnDef", "default");
            String getter = toGetterName(colName);

            sb.append("                new RocksJdbcColumn(\n");
            sb.append("                        ").append(ordinal).append(",\n");
            sb.append("                        ").append(jstr(colName)).append(",\n");
            sb.append("                        ").append(dataType).append(",\n");
            sb.append("                        ").append(jstr(typeName)).append(",\n");
            sb.append("                        ").append(colSize).append(",\n");
            sb.append("                        ").append(decDigits).append(",\n");
            sb.append("                        ").append(nullable).append(",\n");
            sb.append("                        ").append(jstr(isAutoInc)).append(",\n");
            sb.append("                        ").append(jstr(columnDef)).append(",\n");
            sb.append("                        ").append(jstr(getter)).append("\n");
            sb.append("                )");
            sb.append(i < cols.size() - 1 ? ",\n" : "\n");
        }
        sb.append("        };\n\n");

        // pk array
        sb.append("        String[] pkCols = new String[] {");
        for (int i = 0; i < pkCols.size(); i++) {
            if (i > 0) sb.append(", ");
            sb.append(jstr(pkCols.get(i)));
        }
        sb.append("};\n\n");

        // indexes array
        sb.append("        RocksJdbcIndex[] indexes = new RocksJdbcIndex[] {\n");
        if (idxFiltered.isEmpty()) {
            sb.append("        };\n\n");
        } else {
            for (int i = 0; i < idxFiltered.size(); i++) {
                IndexModel ix = idxFiltered.get(i);
                sb.append("                new RocksJdbcIndex(\n");
                sb.append("                        ").append(jstr(ix.indexName())).append(",\n");
                sb.append("                        ").append(ix.unique()).append(",\n");
                sb.append("                        new String[] {");
                List<String> cn = ix.columnNames() == null ? List.of() : ix.columnNames();
                for (int j = 0; j < cn.size(); j++) {
                    if (j > 0) sb.append(", ");
                    sb.append(jstr(cn.get(j)));
                }
                sb.append("}\n");
                sb.append("                )");
                sb.append(i < idxFiltered.size() - 1 ? ",\n" : "\n");
            }
            sb.append("        };\n\n");
        }

        // decoder: use fully-qualified proto type (NO proto import)
        String protoFqn = cfg.protoJavaPkg() + "." + beanClass;

        sb.append("        RocksJdbcDecoder decoder = bytes -> {\n");
        sb.append("            try {\n");
        sb.append("                ").append(protoFqn).append(" p = ").append(protoFqn).append(".parseFrom(bytes);\n");
        sb.append("                return new ").append(mapperClass).append("().fromProto(p);\n");
        sb.append("            } catch (InvalidProtocolBufferException e) {\n");
        sb.append("                throw new SQLException(\"Failed to decode row for table ").append(esc(rawTable)).append("\", e);\n");
        sb.append("            }\n");
        sb.append("        };\n\n");

        // aliases
        sb.append("        String[] names = new String[] {\n");
        sb.append("                ").append(jstr(tableName)).append(",\n");
        sb.append("                ").append(jstr(rawTable)).append(",\n");
        sb.append("                ").append(jstr(beanClass)).append("\n");
        sb.append("        };\n\n");

        // RocksJdbcTable ctor (your runtime signature)
        sb.append("        return new RocksJdbcTable(\n");
        sb.append("                ").append(jstr(tableName)).append(",\n");
        sb.append("                ").append(jstr(cfName)).append(",\n");
        sb.append("                ").append(jstr(schemaName)).append(",\n");
        sb.append("                ").append(beanClass).append(".class,\n");
        sb.append("                cols,\n");
        sb.append("                pkCols,\n");
        sb.append("                indexes,\n");
        sb.append("                decoder,\n");
        sb.append("                names\n");
        sb.append("        );\n");

        sb.append("    }\n\n");
    }

    private static String toGetterName(String colName) {
        if (colName == null || colName.isBlank()) return "get";
        String prop = Naming.sanitizeJavaIdentifier(Naming.toFieldName(colName));
        return "get" + Naming.capitalize(prop);
    }

    private static boolean isPkIndex(IndexModel ix, Set<String> pkColsUpper) {
        if (pkColsUpper == null || pkColsUpper.isEmpty()) return false;
        if (ix.columnNames() == null || ix.columnNames().isEmpty()) return false;

        Set<String> idxCols = ix.columnNames().stream()
                .filter(Objects::nonNull)
                .map(s -> s.toUpperCase(Locale.ROOT))
                .collect(Collectors.toSet());

        return idxCols.equals(pkColsUpper);
    }

    // --- reflection helpers to tolerate different Col models ---

    private static int intVal(Object target, int def, String... methodNames) {
        Object v = invoke0(target, methodNames);
        if (v == null) return def;
        if (v instanceof Integer i) return i;
        if (v instanceof Number n) return n.intValue();
        try { return Integer.parseInt(String.valueOf(v)); } catch (Exception ignore) { return def; }
    }

    private static String strVal(Object target, String... methodNames) {
        Object v = invoke0(target, methodNames);
        if (v == null) return null;
        String s = String.valueOf(v);
        return s.isBlank() ? null : s;
    }

    private static String strValOr(Object target, String def, String... methodNames) {
        String s = strVal(target, methodNames);
        return (s == null) ? def : s;
    }

    // normalize nullable into JDBC int constants:
    // 0 = columnNoNulls, 1 = columnNullable, 2 = columnNullableUnknown
    private static int nullableInt(Object colObj) {
        Object v = invoke0(colObj, "nullable", "isNullable", "getNullable");
        if (v == null) return 2;
        if (v instanceof Integer i) return i;         // already JDBC-style int
        if (v instanceof Boolean b) return b ? 1 : 0; // boolean -> map
        String s = String.valueOf(v).trim();
        if (s.equalsIgnoreCase("YES") || s.equalsIgnoreCase("Y") || s.equals("1") || s.equalsIgnoreCase("true")) return 1;
        if (s.equalsIgnoreCase("NO") || s.equalsIgnoreCase("N") || s.equals("0") || s.equalsIgnoreCase("false")) return 0;
        try { return Integer.parseInt(s); } catch (Exception ignore) { return 2; }
    }

    private static Object invoke0(Object target, String... methodNames) {
        if (target == null) return null;
        for (String mn : methodNames) {
            try {
                var m = target.getClass().getMethod(mn);
                return m.invoke(target);
            } catch (NoSuchMethodException ignored) {
            } catch (Throwable ignored) {
            }
        }
        return null;
    }

    private static String jstr(String s) {
        if (s == null) return "null";
        String out = s
                .replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\r", "\\r")
                .replace("\n", "\\n")
                .replace("\t", "\\t");
        return "\"" + out + "\"";
    }

    private static String esc(String s) {
        if (s == null) return "";
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
