package org.github.dbjo.codegen.rdb;

import org.github.dbjo.codegen.Config;
import org.github.dbjo.meta.db.Col;
import org.github.dbjo.meta.db.TableModel;
import org.github.dbjo.codegen.util.FilesUtil;
import org.github.dbjo.codegen.util.Naming;
import org.github.dbjo.rdb.jdbc.catalog.RocksJdbcCatalog;
import org.github.dbjo.rdb.jdbc.catalog.RocksJdbcDecoder;
import org.github.dbjo.rdb.jdbc.catalog.RocksJdbcTable;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

/**
 * Generates ONE class (a catalog registry used by the Rocks JDBC driver):
 *
 *   package <rocksJdbcPkg>;
 *   public final class <rocksJdbcCatalogClass> implements RocksJdbcCatalog { ... }
 *
 * Defaults (overridable by system props):
 *   -Ddbjo.rocksJdbcPkg=...              (default: schemaPkg with ".schema" -> ".jdbc", else schemaPkg + ".jdbc")
 *   -Ddbjo.rocksJdbcCatalogClass=...     (default: GeneratedRocksJdbcCatalog)
 *
 * The generated catalog references:
 *   - <beanPkg>.<Entity>
 *   - <protoJavaPkg>.<Entity> (FQN only; not imported)
 *   - <protoMapperPkg>.<Entity><ProtoMapperSuffix>
 *
 * It also computes:
 *   - cfName = Naming.toLowerSnake(tableName)
 *   - getterNames matching EntityGenerator's conventions
 */
public final class RocksJdbcCatalogGenerator {
    private static final String DEFAULT_CLASS = "GeneratedRocksJdbcCatalog";

    private final Config cfg;

    public RocksJdbcCatalogGenerator(Config cfg) {
        this.cfg = Objects.requireNonNull(cfg, "cfg");
    }

    /** @return 1 if written, 0 if no tables */
    public int generate(List<TableModel> tables) throws IOException {
        if (tables == null || tables.isEmpty()) return 0;

        String pkg = sys("dbjo.rocksJdbcPkg", defaultPkg(cfg));
        String cls = sys("dbjo.rocksJdbcCatalogClass", DEFAULT_CLASS);

        Path outDir = cfg.codegenOutJava().resolve(pkg.replace('.', '/'));
        Files.createDirectories(outDir);

        List<Entry> entries = new ArrayList<>(tables.size());
        for (TableModel tm : tables) {
            String tableName = tm.table().table();
            String cfName = Naming.toLowerSnake(tableName);

            String beanSimple = Naming.toClassName(tableName);
            String beanFqn = cfg.beanPkg() + "." + beanSimple;

            String mapperSimple = beanSimple + cfg.protoMapperSuffix();
            String mapperFqn = cfg.protoMapperPkg() + "." + mapperSimple;

            String protoFqn = cfg.protoJavaPkg() + "." + beanSimple;

            List<ColInfo> cols = new ArrayList<>();
            for (Col c : tm.cols()) {
                String colName = c.colName();
                String prop = Naming.sanitizeJavaIdentifier(Naming.toFieldName(colName));
                String getter = "get" + Naming.capitalize(prop);
                cols.add(new ColInfo(colName, getter, c.sqlType()));
            }

            // register multiple names to make SQL friendlier:
            //   - raw DB name
            //   - lower_snake
            //   - class name
            Set<String> names = new LinkedHashSet<>();
            names.add(tableName);
            names.add(cfName);
            names.add(beanSimple);

            entries.add(new Entry(tableName, cfName, beanFqn, beanSimple, mapperFqn, mapperSimple, protoFqn, names, cols));
        }

        entries.sort(Comparator.comparing(e -> e.tableName.toLowerCase(Locale.ROOT)));

        String src = render(pkg, cls, entries);

        Path outFile = outDir.resolve(cls + ".java");
        FilesUtil.writeString(outFile, src, cfg.overwrite());
        System.out.println("Wrote: " + outFile);
        return 1;
    }

    private static String defaultPkg(Config cfg) {
        String schemaPkg = cfg.schemaPkg();
        if (schemaPkg != null && schemaPkg.endsWith(".schema")) {
            return schemaPkg.substring(0, schemaPkg.length() - ".schema".length()) + ".jdbc";
        }
        return (schemaPkg == null || schemaPkg.isBlank()) ? "org.github.dbjo.generated.jdbc" : (schemaPkg + ".jdbc");
    }

    private static String sys(String key, String def) {
        String v = System.getProperty(key);
        if (v == null) return def;
        v = v.trim();
        return v.isEmpty() ? def : v;
    }

    private static String render(String pkg, String cls, List<Entry> entries) {
        // imports (stable)
        Set<String> imports = new TreeSet<>();
        imports.add(RocksJdbcCatalog.class.getCanonicalName());
        imports.add(RocksJdbcDecoder.class.getCanonicalName());
        imports.add(RocksJdbcTable.class.getCanonicalName());
        imports.add("java.sql.SQLException");
        imports.add("java.util.*");

        for (Entry e : entries) {
            imports.add(e.beanFqn);
            imports.add(e.mapperFqn);
        }

        StringBuilder sb = new StringBuilder(16_000);
        sb.append("package ").append(pkg).append(";\n\n");
        for (String imp : imports) sb.append("import ").append(imp).append(";\n");
        sb.append("\n");

        sb.append("public final class ").append(cls).append(" implements RocksJdbcCatalog {\n");
        sb.append("    private final List<RocksJdbcTable> tables;\n");
        sb.append("    private final Map<String, RocksJdbcTable> byName;\n\n");

        sb.append("    public ").append(cls).append("() {\n");
        sb.append("        List<RocksJdbcTable> list = new ArrayList<>(").append(entries.size()).append(");\n");
        sb.append("        Map<String, RocksJdbcTable> map = new HashMap<>(").append(entries.size() * 4).append(");\n\n");

        for (Entry e : entries) {
            String methodName = "mk_" + Naming.toLowerCamel(e.beanSimple);
            sb.append("        RocksJdbcTable ").append(e.beanSimple).append("_t = ").append(methodName).append("();\n");
            sb.append("        list.add(").append(e.beanSimple).append("_t);\n");
            sb.append("        for (String n : ").append(e.beanSimple).append("_t.names()) {\n");
            sb.append("            if (n == null) continue;\n");
            sb.append("            map.putIfAbsent(n.trim().toLowerCase(java.util.Locale.ROOT), ").append(e.beanSimple).append("_t);\n");
            sb.append("        }\n\n");
        }

        sb.append("        this.tables = java.util.Collections.unmodifiableList(list);\n");
        sb.append("        this.byName = java.util.Collections.unmodifiableMap(map);\n");
        sb.append("    }\n\n");

        sb.append("    public static ").append(cls).append(" create() { return new ").append(cls).append("(); }\n\n");

        sb.append("    @Override\n");
        sb.append("    public java.util.List<RocksJdbcTable> tables() {\n");
        sb.append("        return tables;\n");
        sb.append("    }\n\n");

        sb.append("    @Override\n");
        sb.append("    public RocksJdbcTable table(String name) {\n");
        sb.append("        if (name == null) return null;\n");
        sb.append("        String k = name.trim().toLowerCase(java.util.Locale.ROOT);\n");
        sb.append("        if (k.isEmpty()) return null;\n");
        sb.append("        return byName.get(k);\n");
        sb.append("    }\n\n");

        // table factories
        for (Entry e : entries) {
            String methodName = "mk_" + Naming.toLowerCamel(e.beanSimple);

            sb.append("    private static RocksJdbcTable ").append(methodName).append("() {\n");
            sb.append("        final ").append(e.mapperSimple).append(" mapper = new ").append(e.mapperSimple).append("();\n");

            // column arrays
            sb.append("        final String[] colNames = new String[] {");
            for (int i = 0; i < e.cols.size(); i++) {
                if (i > 0) sb.append(", ");
                sb.append("\"").append(esc(e.cols.get(i).colName)).append("\"");
            }
            sb.append("};\n");

            sb.append("        final int[] colTypes = new int[] {");
            for (int i = 0; i < e.cols.size(); i++) {
                if (i > 0) sb.append(", ");
                sb.append(e.cols.get(i).sqlType);
            }
            sb.append("};\n");

            sb.append("        final String[] getters = new String[] {");
            for (int i = 0; i < e.cols.size(); i++) {
                if (i > 0) sb.append(", ");
                sb.append("\"").append(esc(e.cols.get(i).getterName)).append("\"");
            }
            sb.append("};\n");

            // names array
            sb.append("        final String[] names = new String[] {");
            int j = 0;
            for (String n : e.names) {
                if (j++ > 0) sb.append(", ");
                sb.append("\"").append(esc(n)).append("\"");
            }
            sb.append("};\n\n");

            sb.append("        RocksJdbcDecoder decoder = bytes -> {\n");
            sb.append("            try {\n");
            sb.append("                var p = ").append(e.protoFqn).append(".parseFrom(bytes);\n");
            sb.append("                return mapper.fromProto(p);\n");
            sb.append("            } catch (com.google.protobuf.InvalidProtocolBufferException ex) {\n");
            sb.append("                throw new SQLException(\"Failed to decode table ").append(esc(e.tableName)).append("\", ex);\n");
            sb.append("            }\n");
            sb.append("        };\n\n");

            sb.append("        return new RocksJdbcTable(\n");
            sb.append("                \"").append(esc(e.tableName)).append("\",\n");
            sb.append("                \"").append(esc(e.cfName)).append("\",\n");
            sb.append("                ").append(e.beanSimple).append(".class,\n");
            sb.append("                colNames,\n");
            sb.append("                colTypes,\n");
            sb.append("                getters,\n");
            sb.append("                decoder,\n");
            sb.append("                names\n");
            sb.append("        );\n");
            sb.append("    }\n\n");
        }

        sb.append("}\n");
        return sb.toString();
    }

    private static String esc(String s) {
        if (s == null) return "";
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private record ColInfo(String colName, String getterName, int sqlType) {}
    private record Entry(
            String tableName,
            String cfName,
            String beanFqn,
            String beanSimple,
            String mapperFqn,
            String mapperSimple,
            String protoFqn,
            Set<String> names,
            List<ColInfo> cols
    ) {}
}
