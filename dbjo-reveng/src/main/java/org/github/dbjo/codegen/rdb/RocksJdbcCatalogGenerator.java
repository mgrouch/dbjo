package org.github.dbjo.codegen.rdb;

import org.github.dbjo.codegen.Config;
import org.github.dbjo.meta.db.Col;
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
 * Generates a catalog for the Rocks JDBC driver so it can expose table/column metadata.
 *
 * IMPORTANT: does NOT import proto message types to avoid name collisions with entity classes
 * (e.g. entity.Purchase vs proto.Purchase). Proto message types are referenced by fully-qualified name.
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
        StringBuilder sb = new StringBuilder(20_000);

        sb.append("package ").append(outPkg).append(";\n\n");

        // Imports: runtime JDBC catalog types
        sb.append("import org.github.dbjo.rdb.jdbc.catalog.RocksJdbcCatalog;\n");
        sb.append("import org.github.dbjo.rdb.jdbc.catalog.RocksJdbcColumn;\n");
        sb.append("import org.github.dbjo.rdb.jdbc.catalog.RocksJdbcDecoder;\n");
        sb.append("import org.github.dbjo.rdb.jdbc.catalog.RocksJdbcIndex;\n");
        sb.append("import org.github.dbjo.rdb.jdbc.catalog.RocksJdbcTable;\n");
        sb.append("\n");

        sb.append("import java.sql.SQLException;\n");
        sb.append("import java.util.*;\n");
        sb.append("\n");

        // Entity + mapper imports only (NO proto imports)
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
            String method = "table_" + beanClass;
            sb.append("            ").append(method).append("()");
            sb.append(i < tables.size() - 1 ? ",\n" : "\n");
        }
        sb.append("    );\n\n");

        sb.append("    private static final Map<String, RocksJdbcTable> BY_NAME = buildByName(TABLES);\n\n");

        sb.append("    @Override\n");
        sb.append("    public List<RocksJdbcTable> tables() {\n");
        sb.append("        return TABLES;\n");
        sb.append("    }\n\n");

        sb.append("    @Override\n");
        sb.append("    public RocksJdbcTable requireTable(String name) throws SQLException {\n");
        sb.append("        if (name == null) throw new SQLException(\"name is null\");\n");
        sb.append("        String k = name.trim().toLowerCase(Locale.ROOT);\n");
        sb.append("        RocksJdbcTable t = BY_NAME.get(k);\n");
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

        String tableName = Naming.toLowerSnake(rawTable);
        String cfName = Naming.toLowerSnake(rawTable);
        String schemaName = (schema == null ? "" : schema);

        List<Col> cols = new ArrayList<>(tm.cols());
        cols.sort(Comparator.comparingInt(Col::pos));

        List<String> pkCols = new ArrayList<>();
        Set<String> pkUpper = tm.pkColsUpper() == null ? Set.of() : tm.pkColsUpper();
        for (Col c : cols) {
            String up = c.colName() == null ? null : c.colName().toUpperCase(Locale.ROOT);
            if (up != null && pkUpper.contains(up)) pkCols.add(c.colName());
        }

        List<IndexModel> idxs = tm.indexes() == null ? List.of() : tm.indexes();
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

            String colName = c.colName();
            String prop = Naming.sanitizeJavaIdentifier(Naming.toFieldName(colName));
            String getter = "get" + Naming.capitalize(prop);

            // if your Col does not have autoInc/default, keep conservative values
            String ai = "NO";
            String def = null;

            sb.append("                new RocksJdbcColumn(\n");
            sb.append("                        ").append(jstr(colName)).append(",\n");
            sb.append("                        ").append(c.sqlType()).append(",\n");
            sb.append("                        ").append(jstr(c.typeName())).append(",\n");
            sb.append("                        ").append(c.size()).append(",\n");
            sb.append("                        ").append(c.scale()).append(",\n");
            sb.append("                        ").append(c.nullable()).append(",\n");
            sb.append("                        ").append(jstr(ai)).append(",\n");
            sb.append("                        ").append(jstr(def)).append(",\n");
            sb.append("                        ").append(jstr(getter)).append("\n");
            sb.append("                )");
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

        // decoder uses fully-qualified proto type, NO proto import
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

    private static boolean isPkIndex(IndexModel ix, Set<String> pkColsUpper) {
        if (pkColsUpper == null || pkColsUpper.isEmpty()) return false;
        if (ix.columnNames() == null || ix.columnNames().isEmpty()) return false;

        Set<String> idxCols = ix.columnNames().stream()
                .filter(Objects::nonNull)
                .map(s -> s.toUpperCase(Locale.ROOT))
                .collect(Collectors.toSet());

        return idxCols.equals(pkColsUpper);
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
