package org.github.dbjo.codegen.db;

import org.github.dbjo.codegen.util.FilesUtil;
import org.github.dbjo.codegen.util.Naming;
import org.github.dbjo.meta.db.Col;
import org.github.dbjo.meta.db.IndexModel;
import org.github.dbjo.meta.db.TableModel;
import org.github.dbjo.meta.db.TableRef;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

/**
 * Generates runtime-usable DB schema metadata from already-introspected TableModel list.
 *
 * Output:
 *  - {schemaPkg}.DbSchema
 *  - {schemaPkg}.tables.{Schema}{Table}Table (one per table), each exposing TableModel MODEL
 *
 * Uses meta.db records at runtime (Col/TableRef/TableModel/IndexModel).
 */
public final class DbSchemaGenerator {

    private final Path outJavaDir;
    private final String schemaPkg;
    private final boolean overwrite;

    public DbSchemaGenerator(Path outJavaDir, String schemaPkg, boolean overwrite) {
        this.outJavaDir = Objects.requireNonNull(outJavaDir, "outJavaDir");
        this.schemaPkg = Objects.requireNonNull(schemaPkg, "schemaPkg");
        this.overwrite = overwrite;
        validatePackageName(schemaPkg);
    }

    public int generateAll(List<TableModel> tables) throws IOException {
        Objects.requireNonNull(tables, "tables");

        String tablesPkg = schemaPkg + ".tables";

        Path schemaOutDir = outJavaDir.resolve(schemaPkg.replace('.', '/'));
        Path tablesOutDir = outJavaDir.resolve(tablesPkg.replace('.', '/'));
        Files.createDirectories(schemaOutDir);
        Files.createDirectories(tablesOutDir);

        // deterministic order
        List<TableModel> sorted = new ArrayList<>(tables);
        sorted.sort(Comparator
                .comparing((TableModel tm) -> nz(tm.table() == null ? null : tm.table().schema()).toLowerCase(Locale.ROOT))
                .thenComparing(tm -> nz(tm.table() == null ? null : tm.table().table()).toLowerCase(Locale.ROOT)));

        // Generate per-table classes
        List<String> tableClassFqns = new ArrayList<>();
        for (TableModel tm : sorted) {
            if (tm == null || tm.table() == null) continue;

            String schema = tm.table().schema();
            String table = tm.table().table();
            if (table == null || table.isBlank()) continue;

            String cls = tableMetaClassName(schema, table);
            String fqn = tablesPkg + "." + cls;
            tableClassFqns.add(fqn);

            String src = renderTableClass(tablesPkg, cls, tm);
            Path outFile = tablesOutDir.resolve(cls + ".java");
            FilesUtil.writeString(outFile, src, overwrite);
        }

        // Generate registry
        String schemaCls = "DbSchema";
        String schemaSrc = renderSchemaRegistry(schemaPkg, schemaCls, tableClassFqns);
        Path schemaFile = schemaOutDir.resolve(schemaCls + ".java");
        FilesUtil.writeString(schemaFile, schemaSrc, overwrite);

        return tableClassFqns.size() + 1; // + registry
    }

    // ------------------------------------------------------------
    // Rendering
    // ------------------------------------------------------------

    private String renderTableClass(String pkg, String cls, TableModel tm) {
        TableRef tr = tm.table();
        String schema = tr.schema();
        String table = tr.table();

        List<Col> cols = (tm.cols() == null) ? List.of() : tm.cols();
        Set<String> pk = (tm.pkColsUpper() == null) ? Set.of() : tm.pkColsUpper();
        List<IndexModel> idx = (tm.indexes() == null) ? List.of() : tm.indexes();

        // Ensure stable output for cols (by pos)
        List<Col> colsSorted = new ArrayList<>(cols);
        colsSorted.sort(Comparator.comparingInt(Col::pos));

        // Stable PK order in emitted Set: lexical
        List<String> pkSorted = new ArrayList<>(pk);
        pkSorted.sort(String.CASE_INSENSITIVE_ORDER);

        StringBuilder sb = new StringBuilder(24_000);
        sb.append("package ").append(pkg).append(";\n\n");
        sb.append("import java.util.*;\n");
        sb.append("import org.github.dbjo.meta.db.*;\n\n");

        sb.append("/**\n");
        sb.append(" * Auto-generated table metadata for ").append(nz(schema))
                .append(schema == null || schema.isBlank() ? "" : ".")
                .append(table).append(".\n");
        sb.append(" * Do not edit by hand.\n");
        sb.append(" */\n");
        sb.append("public final class ").append(cls).append(" {\n\n");

        sb.append("    private ").append(cls).append("() {}\n\n");

        // REF
        sb.append("    public static final TableRef REF = new TableRef(")
                .append(strOrNull(schema)).append(", ").append(strOrNull(table)).append(");\n\n");

        // COLS
        sb.append("    public static final List<Col> COLS = List.of(\n");
        for (int i = 0; i < colsSorted.size(); i++) {
            Col c = colsSorted.get(i);
            sb.append("            ").append(renderCol(c));
            sb.append(i == colsSorted.size() - 1 ? "\n" : ",\n");
        }
        sb.append("    );\n\n");

        // PK
        sb.append("    public static final Set<String> PK_COLS_UPPER = Set.of(");
        for (int i = 0; i < pkSorted.size(); i++) {
            if (i > 0) sb.append(", ");
            sb.append("\"").append(escape(pkSorted.get(i))).append("\"");
        }
        sb.append(");\n\n");

        // INDEXES
        sb.append("    public static final List<IndexModel> INDEXES = List.of(\n");
        if (idx.isEmpty()) {
            sb.append("            // none\n");
        } else {
            for (int i = 0; i < idx.size(); i++) {
                sb.append("            ").append(renderIndex(idx.get(i)));
                sb.append(i == idx.size() - 1 ? "\n" : ",\n");
            }
        }
        sb.append("    );\n\n");

        // MODEL
        sb.append("    public static final TableModel MODEL = new TableModel(\n");
        sb.append("            REF,\n");
        sb.append("            COLS,\n");
        sb.append("            PK_COLS_UPPER,\n");
        sb.append("            INDEXES\n");
        sb.append("    );\n");

        sb.append("}\n");
        return sb.toString();
    }

    private String renderSchemaRegistry(String pkg, String cls, List<String> tableClassFqns) {
        String tablesPkg = pkg + ".tables";

        StringBuilder sb = new StringBuilder(18_000);
        sb.append("package ").append(pkg).append(";\n\n");
        sb.append("import java.util.*;\n");
        sb.append("import org.github.dbjo.meta.db.*;\n");
        sb.append("import ").append(tablesPkg).append(".*;\n\n");

        sb.append("/**\n");
        sb.append(" * Auto-generated DB schema registry.\n");
        sb.append(" * Lookup is case-insensitive on schema/table.\n");
        sb.append(" */\n");
        sb.append("public final class ").append(cls).append(" {\n\n");

        sb.append("    public static final ").append(cls).append(" INSTANCE = new ").append(cls).append("();\n\n");

        sb.append("    public final List<TableModel> tables;\n");
        sb.append("    private final Map<String, TableModel> byKey;\n\n");

        sb.append("    private ").append(cls).append("() {\n");
        sb.append("        List<TableModel> t = new ArrayList<>();\n");

        for (String fqn : tableClassFqns) {
            String simple = fqn.substring(fqn.lastIndexOf('.') + 1);
            sb.append("        t.add(").append(simple).append(".MODEL);\n");
        }

        sb.append("        this.tables = Collections.unmodifiableList(t);\n");
        sb.append("        Map<String, TableModel> m = new HashMap<>();\n");
        sb.append("        for (TableModel tm : t) {\n");
        sb.append("            TableRef r = tm.table();\n");
        sb.append("            m.put(key(r.schema(), r.table()), tm);\n");
        sb.append("        }\n");
        sb.append("        this.byKey = Collections.unmodifiableMap(m);\n");
        sb.append("    }\n\n");

        sb.append("    public Optional<TableModel> find(String schema, String table) {\n");
        sb.append("        if (table == null) return Optional.empty();\n");
        sb.append("        return Optional.ofNullable(byKey.get(key(schema, table)));\n");
        sb.append("    }\n\n");

        sb.append("    public TableModel table(String schema, String table) {\n");
        sb.append("        return find(schema, table).orElseThrow(() ->\n");
        sb.append("                new NoSuchElementException(\"Unknown table: \" + nz(schema) + \".\" + table));\n");
        sb.append("    }\n\n");

        sb.append("    private static String key(String schema, String table) {\n");
        sb.append("        return (nz(schema) + \".\" + nz(table)).toLowerCase(java.util.Locale.ROOT);\n");
        sb.append("    }\n\n");

        sb.append("    private static String nz(String s) { return (s == null) ? \"\" : s; }\n");

        sb.append("}\n");
        return sb.toString();
    }

    private static String renderCol(Col c) {
        return "new Col("
                + c.pos() + ", "
                + strOrNull(c.colName()) + ", "
                + c.sqlType() + ", "
                + strOrNull(c.typeName()) + ", "
                + c.size() + ", "
                + c.scale() + ", "
                + c.nullable() + ", "
                + strOrNull(c.isAutoIncrement()) + ", "
                + strOrNull(c.defaultValue())
                + ")";
    }

    private static String renderIndex(IndexModel ix) {
        // IndexModel(String indexName, boolean unique, List<String> columnNames)
        StringBuilder sb = new StringBuilder();
        sb.append("new IndexModel(")
                .append(strOrNull(ix.indexName()))
                .append(", ")
                .append(ix.unique() ? "true" : "false")
                .append(", ")
                .append("List.of(");

        List<String> cols = ix.columnNames();
        if (cols != null) {
            for (int i = 0; i < cols.size(); i++) {
                if (i > 0) sb.append(", ");
                sb.append("\"").append(escape(cols.get(i))).append("\"");
            }
        }
        sb.append("))");
        return sb.toString();
    }

    // ------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------

    private static String tableMetaClassName(String schema, String table) {
        String s = (schema == null || schema.isBlank()) ? "" : safeClassPart(schema);
        String t = safeClassPart(table);
        return s + t + "Table";
    }

    private static String safeClassPart(String raw) {
        String cls = Naming.toClassName(raw);
        if (cls == null || cls.isBlank()) cls = "X";
        if (!Character.isJavaIdentifierStart(cls.charAt(0))) cls = "_" + cls;
        if (Naming.JAVA_KEYWORDS.contains(cls.toLowerCase(Locale.ROOT))) cls = cls + "X";
        return cls;
    }

    private static String strOrNull(String s) {
        return (s == null) ? "null" : "\"" + escape(s) + "\"";
    }

    private static String escape(String s) {
        if (s == null) return "";
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private static String nz(String s) {
        return s == null ? "" : s;
    }

    private static void validatePackageName(String pkg) {
        if (pkg == null || pkg.isBlank()) throw new IllegalArgumentException("schemaPkg is blank");
        String[] parts = pkg.split("\\.");
        for (String p : parts) {
            if (p.isEmpty()) throw new IllegalArgumentException("Invalid package: " + pkg);
            if (!Character.isJavaIdentifierStart(p.charAt(0))) {
                throw new IllegalArgumentException("Invalid package segment '" + p + "' in " + pkg);
            }
            for (int i = 1; i < p.length(); i++) {
                if (!Character.isJavaIdentifierPart(p.charAt(i))) {
                    throw new IllegalArgumentException("Invalid package segment '" + p + "' in " + pkg);
                }
            }
            if (Naming.JAVA_KEYWORDS.contains(p)) {
                throw new IllegalArgumentException("Invalid package: segment is Java keyword: " + p);
            }
        }
    }
}
