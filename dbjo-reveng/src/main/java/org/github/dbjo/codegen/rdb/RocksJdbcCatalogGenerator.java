package org.github.dbjo.codegen.rdb;

import org.github.dbjo.codegen.Config;
import org.github.dbjo.codegen.util.FilesUtil;
import org.github.dbjo.codegen.util.Naming;
import org.github.dbjo.meta.db.Col;
import org.github.dbjo.meta.db.IndexModel;
import org.github.dbjo.meta.db.TableModel;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Types;
import java.util.*;

/**
 * Generates a RocksJdbcCatalog implementation with metadata + decoders.
 *
 * IMPORTANT:
 *  - No reflection: uses your Col accessors directly.
 *  - RocksJdbcTable ctor order matches your runtime:
 *      (schemaName, tableName, cfName, rowClass, columns, pkColumns, indexes, decoder, names)
 *  - RocksJdbcColumn ctor matches your record:
 *      (pos,name,sqlType,typeName,size,scale,nullable,isAutoIncrement,defaultValue,getterName)
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

        sb.append("import org.github.dbjo.rdb.jdbc.catalog.RocksJdbcCatalog;\n");
        sb.append("import org.github.dbjo.rdb.jdbc.catalog.RocksJdbcColumn;\n");
        sb.append("import org.github.dbjo.rdb.jdbc.catalog.RocksJdbcDecoder;\n");
        sb.append("import org.github.dbjo.rdb.jdbc.catalog.RocksJdbcIndex;\n");
        sb.append("import org.github.dbjo.rdb.jdbc.catalog.RocksJdbcTable;\n\n");

        sb.append("import java.sql.SQLException;\n");
        sb.append("import java.util.*;\n\n");

        // entity + mapper imports only (no proto import)
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

        sb.append("    @Override public List<RocksJdbcTable> tables() { return TABLES; }\n\n");

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

        for (TableModel tm : tables) renderTableMethod(sb, tm);

        sb.append("}\n");
        return sb.toString();
    }

    private void renderTableMethod(StringBuilder sb, TableModel tm) {
        String rawTable = tm.table().table();
        String schema = tm.table().schema();

        String beanClass = Naming.toClassName(rawTable);
        String mapperClass = beanClass + cfg.protoMapperSuffix();

        String tableName = Naming.toLowerSnake(rawTable);
        String cfName = Naming.toLowerSnake(rawTable);
        String schemaName = (schema == null ? "PUBLIC" : schema);

        List<Col> cols = new ArrayList<>(tm.cols());
        cols.sort(Comparator.comparingInt(Col::pos));

        // pk columns by tm.pkColsUpper()
        List<String> pkCols = new ArrayList<>();
        Set<String> pkUpper = (tm.pkColsUpper() == null) ? Set.of() : tm.pkColsUpper();
        for (Col c : cols) {
            if (c.colName() == null) continue;
            if (pkUpper.contains(c.colName().toUpperCase(Locale.ROOT))) pkCols.add(c.colName());
        }

        // indexes (skip pure PK index)
        List<IndexModel> idxs = (tm.indexes() == null) ? List.of() : tm.indexes();
        List<IndexModel> idxFiltered = new ArrayList<>();
        for (IndexModel ix : idxs) {
            if (ix == null) continue;
            if (isPkIndex(ix, pkUpper)) continue;
            idxFiltered.add(ix);
        }

        sb.append("    private static RocksJdbcTable table_").append(beanClass).append("() {\n");

        sb.append("        RocksJdbcColumn[] cols = new RocksJdbcColumn[] {\n");
        for (int i = 0; i < cols.size(); i++) {
            Col c = cols.get(i);

            int ordinal = c.pos();
            String colName = c.colName();
            int dataType = c.sqlType();
            String typeName = (c.typeName() == null || c.typeName().isBlank()) ? "\"" + dataType + "\"": c.typeName();
            int colSize = c.size();
            int decDigits = c.scale();
            boolean nullable = c.nullable();
            String isAutoInc = (c.autoIncrement() ? "YES" : "NO");
            String columnDef = c.defaultValue();
            String getter = toGetterName(colName);


            sb.append("                new RocksJdbcColumn(")
                    .append(ordinal).append(", ")
                    .append(jstr(colName)).append(", ")
                    .append(dataType).append(", ")
                    .append(jstr(typeName)).append(", ")
                    .append(colSize).append(", ")
                    .append(decDigits).append(", ")
                    .append(nullable).append(", ")
                    .append(jstr(isAutoInc)).append(", ")
                    .append(jstr(columnDef)).append(", ")
                    .append(jstr(getter))
                    .append(")");
            sb.append(i < cols.size() - 1 ? ",\n" : "\n");
        }
        sb.append("        };\n\n");

        sb.append("        String[] pkCols = new String[] {");
        for (int i = 0; i < pkCols.size(); i++) {
            if (i > 0) sb.append(", ");
            sb.append(jstr(pkCols.get(i)));
        }
        sb.append("};\n\n");

        sb.append("        RocksJdbcIndex[] indexes = new RocksJdbcIndex[] {\n");
        if (idxFiltered.isEmpty()) {
            sb.append("        };\n\n");
        } else {
            for (int i = 0; i < idxFiltered.size(); i++) {
                IndexModel ix = idxFiltered.get(i);
                sb.append("                new RocksJdbcIndex(")
                        .append(jstr(ix.indexName())).append(", ")
                        .append(ix.unique()).append(", ")
                        .append("new String[] {");
                List<String> cn = (ix.columnNames() == null) ? List.of() : ix.columnNames();
                for (int j = 0; j < cn.size(); j++) {
                    if (j > 0) sb.append(", ");
                    sb.append(jstr(cn.get(j)));
                }
                sb.append("})");
                sb.append(i < idxFiltered.size() - 1 ? ",\n" : "\n");
            }
            sb.append("        };\n\n");
        }

        String protoFqn = cfg.protoJavaPkg() + "." + beanClass;

        sb.append("        RocksJdbcDecoder decoder = bytes -> {\n");
        sb.append("            try {\n");
        sb.append("                ").append(protoFqn).append(" p = ").append(protoFqn).append(".parseFrom(bytes);\n");
        sb.append("                return new ").append(mapperClass).append("().fromProto(p);\n");
        sb.append("            } catch (InvalidProtocolBufferException e) {\n");
        sb.append("                throw new SQLException(\"Failed to decode row for table ").append(esc(rawTable)).append("\", e);\n");
        sb.append("            }\n");
        sb.append("        };\n\n");

        sb.append("        String[] names = new String[] {\n");
        sb.append("                ").append(jstr(tableName)).append(",\n");
        sb.append("                ").append(jstr(rawTable)).append(",\n");
        sb.append("                ").append(jstr(beanClass)).append("\n");
        sb.append("        };\n\n");

        // IMPORTANT: ctor order matches runtime
        sb.append("        return new RocksJdbcTable(\n");
        sb.append("                ").append(jstr(schemaName)).append(",\n");
        sb.append("                ").append(jstr(tableName)).append(",\n");
        sb.append("                ").append(jstr(cfName)).append(",\n");
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

        Set<String> idxCols = new HashSet<>();
        for (String s : ix.columnNames()) {
            if (s != null) idxCols.add(s.toUpperCase(Locale.ROOT));
        }
        return idxCols.equals(pkColsUpper);
    }

    private static String jstr(String s) {
        if (s == null) return "null";
        String out = s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\r", "\\r").replace("\n", "\\n").replace("\t", "\\t");
        return "\"" + out + "\"";
    }

    private static String esc(String s) {
        if (s == null) return "";
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
