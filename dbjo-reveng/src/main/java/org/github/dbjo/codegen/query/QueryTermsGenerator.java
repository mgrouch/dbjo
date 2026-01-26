package org.github.dbjo.codegen.query;

import org.github.dbjo.codegen.Config;
import org.github.dbjo.meta.db.Col;
import org.github.dbjo.meta.db.TableModel;
import org.github.dbjo.codegen.types.TypeMappings;
import org.github.dbjo.codegen.util.FilesUtil;
import org.github.dbjo.codegen.util.Naming;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

public final class QueryTermsGenerator {

    private static final String DEFAULT_TERMS_FQN = "org.github.dbjo.criteria.Terms";
    private static final String DEFAULT_PROPERTY_TERM_FQN = "org.github.dbjo.criteria.PropertyTerm";
    private static final String DEFAULT_QUERY_SUFFIX = "Q";
    private static final String DEFAULT_META_SUFFIX = "Meta";

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
            String metaClass = beanClass + effectiveMetaSuffix();

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

        imports.add(effectiveTermsFqn());
        imports.add(effectivePropertyTermFqn());

        if (!cfg.beanPkg().equals(queryPkg)) {
            imports.add(cfg.beanPkg() + "." + beanClass);
        }
        if (!cfg.metaPkg().equals(queryPkg)) {
            imports.add(cfg.metaPkg() + "." + metaClass);
        }

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

            // IMPORTANT: must match Meta generator naming
            // If your Meta generator produces PRICECENTS/CREATEDAT, use Naming.toUpperConst(prop) instead.
            String constName = Naming.toUpperSnake(prop);

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

    private String effectiveQueryPkg() {
        // If Config has queryPkg, use it; otherwise derive from metaPkg.
        String qp = safeGet(cfg, "queryPkg");
        if (qp != null && !qp.isBlank()) return qp.trim();

        // Derive: replace last segment "meta" -> "query", else append ".query"
        String mp = cfg.metaPkg();
        if (mp == null || mp.isBlank()) return "query";

        int lastDot = mp.lastIndexOf('.');
        String lastSeg = lastDot >= 0 ? mp.substring(lastDot + 1) : mp;

        if ("meta".equals(lastSeg)) {
            return (lastDot >= 0 ? mp.substring(0, lastDot + 1) : "") + "query";
        }
        return mp + ".query";
    }

    private String effectiveQuerySuffix() {
        String s = safeGet(cfg, "querySuffix");
        return (s != null && !s.isBlank()) ? s.trim() : DEFAULT_QUERY_SUFFIX;
    }

    private String effectiveMetaSuffix() {
        String s = safeGet(cfg, "metaSuffix");
        return (s != null && !s.isBlank()) ? s.trim() : DEFAULT_META_SUFFIX;
    }

    private String effectiveTermsFqn() {
        String s = safeGet(cfg, "termsFqn");
        return (s != null && !s.isBlank()) ? s.trim() : DEFAULT_TERMS_FQN;
    }

    private String effectivePropertyTermFqn() {
        String s = safeGet(cfg, "propertyTermFqn");
        return (s != null && !s.isBlank()) ? s.trim() : DEFAULT_PROPERTY_TERM_FQN;
    }

    /**
     * Backwards compatible: if your Config record doesn't yet have these accessors,
     * this will just return null and we’ll use defaults/derived values.
     */
    private static String safeGet(Config cfg, String accessor) {
        try {
            var m = cfg.getClass().getMethod(accessor);
            Object v = m.invoke(cfg);
            return (v instanceof String s) ? s : null;
        } catch (Exception ignored) {
            return null;
        }
    }

    private static void addTypeImport(Set<String> imports, String javaType) {
        if (javaType == null || javaType.isBlank()) return;
        if (javaType.endsWith("[]")) return;

        switch (javaType) {
            case "boolean","byte","short","int","long","float","double",
                    "char", "String", "Boolean", "Byte", "Short", "Integer",
                    "Long", "Float", "Double", "Character" -> { return; }
        }

        switch (javaType) {
            case "BigDecimal" -> imports.add("java.math.BigDecimal");
            case "UUID" -> imports.add("java.util.UUID");
            case "Date" -> imports.add("java.sql.Date");
            case "Time" -> imports.add("java.sql.Time");
            case "Timestamp" -> imports.add("java.sql.Timestamp");
            default -> {
                if (javaType.contains(".")) imports.add(javaType);
            }
        }
    }
}
