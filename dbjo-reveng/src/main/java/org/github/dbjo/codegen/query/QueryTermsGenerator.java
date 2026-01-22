package org.github.dbjo.codegen.query;

import org.github.dbjo.codegen.Config;
import org.github.dbjo.codegen.model.Col;
import org.github.dbjo.codegen.model.TableModel;
import org.github.dbjo.codegen.types.TypeMappings;
import org.github.dbjo.codegen.util.FilesUtil;
import org.github.dbjo.codegen.util.Naming;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

/**
 * Generates per table:
 *   <Entity>Q with PropertyTerm constants referring to <Entity>Meta.<PROP>
 *
 * Example:
 *   public static final PropertyTerm<Client, Long> ID = Terms.prop(ClientMeta.ID);
 *
 * Defaults (override via -D...):
 *   -Ddbjo.queryPkg              (default: cfg.metaPkg())
 *   -Ddbjo.querySuffix           (default: "Q")
 *   -Ddbjo.metaSuffix            (default: "Meta")
 *   -Ddbjo.termsFqn              (default: "org.github.dbjo.meta.query.Terms")
 *   -Ddbjo.propertyTermFqn       (default: "org.github.dbjo.meta.query.PropertyTerm")
 */
public final class QueryTermsGenerator {
    private static final String DEFAULT_TERMS_FQN = "org.github.dbjo.criteria.Terms";
    private static final String DEFAULT_PROPERTY_TERM_FQN = "org.github.dbjo.criteria.PropertyTerm";

    private final Config cfg;

    public QueryTermsGenerator(Config cfg) {
        this.cfg = Objects.requireNonNull(cfg, "cfg");
    }

    public int generateAll(List<TableModel> tables) throws IOException {
        String queryPkg = effectiveQueryPkg();
        Path outDir = cfg.codegenOutJava().resolve(queryPkg.replace('.', '/'));
        Files.createDirectories(outDir);

        int n = 0;
        for (TableModel tm : tables) {
            String beanClass = Naming.toClassName(tm.table().table());
            String qClass = beanClass + effectiveQuerySuffix();
            String metaClass = beanClass + effectiveMetaSuffix(); // e.g. ClientMeta

            String src = renderQ(queryPkg, tm, beanClass, qClass, metaClass);

            Path outFile = outDir.resolve(qClass + ".java");
            FilesUtil.writeString(outFile, src, cfg.overwrite());
            System.out.println("Wrote: " + outFile);
            n++;
        }
        return n;
    }

    private String renderQ(String queryPkg, TableModel tm, String beanClass, String qClass, String metaClass) {
        Set<String> imports = new TreeSet<>();

        String termsFqn = System.getProperty("dbjo.termsFqn", DEFAULT_TERMS_FQN).trim();
        String propTermFqn = System.getProperty("dbjo.propertyTermFqn", DEFAULT_PROPERTY_TERM_FQN).trim();

        imports.add(termsFqn);
        imports.add(propTermFqn);

        // Import bean/meta if not in same package as generated Q
        if (!cfg.beanPkg().equals(queryPkg)) {
            imports.add(cfg.beanPkg() + "." + beanClass);
        }
        if (!cfg.metaPkg().equals(queryPkg)) {
            imports.add(cfg.metaPkg() + "." + metaClass);
        }

        // Add Java type imports needed in generics (Timestamp/BigDecimal/etc.)
        for (Col c : tm.cols()) {
            var jt = TypeMappings.mapSqlTypeToJava(c.sqlType(), null);
            addTypeImport(imports, jt.javaType());
        }

        StringBuilder sb = new StringBuilder(4000);
        sb.append("package ").append(queryPkg).append(";\n\n");
        for (String imp : imports) sb.append("import ").append(imp).append(";\n");
        sb.append("\n");

        sb.append("public final class ").append(qClass).append(" {\n");
        sb.append("  private ").append(qClass).append("() {}\n\n");

        for (Col c : tm.cols()) {
            String prop = Naming.sanitizeJavaIdentifier(Naming.toFieldName(c.colName()));

            // Use UPPER_SNAKE for BOTH Q constants and Meta constants
            String constName = Naming.toUpperSnake(prop); // createdAt -> CREATED_AT

            var jt = TypeMappings.mapSqlTypeToJava(c.sqlType(), null);
            String javaType = jt.javaType();

            sb.append("  public static final PropertyTerm<")
                    .append(beanClass).append(", ").append(javaType).append("> ")
                    .append(constName)
                    .append(" = Terms.prop(")
                    .append(metaClass).append(".").append(constName)
                    .append(");\n");
        }

        sb.append("}\n");
        return sb.toString();
    }

    private static void addTypeImport(Set<String> imports, String javaType) {
        // Only types that are not in java.lang and not primitives/arrays need imports.
        if (javaType == null || javaType.isBlank()) return;

        // Arrays and primitives: no import
        if (javaType.endsWith("[]")) return;
        switch (javaType) {
            case "boolean", "byte", "short", "int", "long", "float", "double", "char" -> { return; }
        }

        // java.lang: no import
        switch (javaType) {
            case "String", "Boolean", "Byte", "Short", "Integer", "Long", "Float", "Double", "Character" -> { return; }
        }

        // Common JDBC / numeric types you already hit
        switch (javaType) {
            case "BigDecimal" -> imports.add("java.math.BigDecimal");
            case "UUID" -> imports.add("java.util.UUID");
            case "Date" -> imports.add("java.sql.Date");
            case "Time" -> imports.add("java.sql.Time");
            case "Timestamp" -> imports.add("java.sql.Timestamp");
            default -> {
                // If TypeMappings returns fully-qualified types sometimes, allow those too:
                if (javaType.contains(".")) {
                    imports.add(javaType);
                }
            }
        }
    }

    private String effectiveQueryPkg() {
        String p = System.getProperty("dbjo.queryPkg");
        if (p != null && !p.isBlank()) return p.trim();
        // sensible default: keep Q alongside Meta so you usually avoid extra imports
        return cfg.metaPkg();
    }

    private static String effectiveQuerySuffix() {
        String s = System.getProperty("dbjo.querySuffix");
        return (s != null && !s.isBlank()) ? s.trim() : "Q";
    }

    private static String effectiveMetaSuffix() {
        String s = System.getProperty("dbjo.metaSuffix");
        return (s != null && !s.isBlank()) ? s.trim() : "Meta";
    }
}
