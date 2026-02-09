package org.github.dbjo.codegen.entity;

import org.github.dbjo.codegen.Config;
import org.github.dbjo.codegen.types.TypeMappings;
import org.github.dbjo.codegen.util.FilesUtil;
import org.github.dbjo.codegen.util.Naming;
import org.github.dbjo.meta.db.Col;
import org.github.dbjo.meta.db.TableModel;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

/**
 * Generates one POJO validator class per table.
 */
public final class PojoValidatorGenerator {
    private final Config cfg;

    public PojoValidatorGenerator(Config cfg) {
        this.cfg = cfg;
    }

    public int generateAll(List<TableModel> tables) throws IOException {
        Path outDir = cfg.codegenOutJava().resolve(cfg.validatorPkg().replace('.', '/'));
        Files.createDirectories(outDir);

        int count = 0;
        for (TableModel tm : tables) {
            String beanClass = Naming.toClassName(tm.table().table());
            String validatorClass = beanClass + cfg.validatorSuffix();

            String src = renderValidator(cfg.validatorPkg(), cfg.beanPkg(), beanClass, validatorClass, tm);
            Path outFile = outDir.resolve(validatorClass + ".java");
            FilesUtil.writeString(outFile, src, cfg.overwrite());
            System.out.println("Wrote: " + outFile);
            count++;
        }
        return count;
    }

    static String renderValidator(String validatorPkg, String beanPkg, String beanClass, String validatorClass, TableModel tm) {
        List<String> checks = new ArrayList<>();

        for (Col c : tm.cols()) {
            String field = Naming.sanitizeJavaIdentifier(Naming.toFieldName(c.colName()));
            String cap = Naming.capitalize(field);
            String getter = "pojo.get" + cap + "()";
            String dbCol = c.colName();

            if (!c.nullable()) {
                checks.add("        if (" + getter + " == null) errors.add(\"" + dbCol + " must not be null\");");
            }

            TypeMappings.JavaType jt = TypeMappings.mapSqlTypeToJava(c.sqlType(), c.typeName(), null);
            String javaType = jt.javaType();

            if ((c.sqlType() == Types.CHAR || c.sqlType() == Types.VARCHAR || c.sqlType() == Types.NCHAR || c.sqlType() == Types.NVARCHAR
                    || c.sqlType() == Types.LONGVARCHAR || c.sqlType() == Types.LONGNVARCHAR)
                    && c.size() > 0) {
                checks.add("        if (" + getter + " != null && " + getter + ".length() > " + c.size() + ") errors.add(\"" + dbCol + " length must be <= " + c.size() + "\");");
            }

            if ("byte[]".equals(javaType) && c.size() > 0) {
                checks.add("        if (" + getter + " != null && " + getter + ".length > " + c.size() + ") errors.add(\"" + dbCol + " byte length must be <= " + c.size() + "\");");
            }

            if (c.scale() >= 0 && (c.sqlType() == Types.DECIMAL || c.sqlType() == Types.NUMERIC)) {
                checks.add("        if (" + getter + " != null && " + getter + ".scale() > " + c.scale() + ") errors.add(\"" + dbCol + " scale must be <= " + c.scale() + "\");");
                if (c.size() > 0) {
                    checks.add("        if (" + getter + " != null && " + getter + ".precision() > " + c.size() + ") errors.add(\"" + dbCol + " precision must be <= " + c.size() + "\");");
                }
            }
        }

        StringBuilder sb = new StringBuilder(5000);
        sb.append("package ").append(validatorPkg).append(";\n\n");
        sb.append("import java.util.ArrayList;\n");
        sb.append("import java.util.List;\n");
        if (!validatorPkg.equals(beanPkg)) {
            sb.append("import ").append(beanPkg).append('.').append(beanClass).append(";\n");
        }
        sb.append("\n");

        sb.append("/**\n");
        sb.append(" * Auto-generated validator for ").append(tm.table().schema()).append('.').append(tm.table().table()).append(".\n");
        sb.append(" */\n");
        sb.append("public final class ").append(validatorClass).append(" {\n\n");
        sb.append("    private ").append(validatorClass).append("() {}\n\n");

        sb.append("    public static List<String> validate(").append(beanClass).append(" pojo) {\n");
        sb.append("        List<String> errors = new ArrayList<>();\n");
        sb.append("        if (pojo == null) {\n");
        sb.append("            errors.add(\"pojo must not be null\");\n");
        sb.append("            return errors;\n");
        sb.append("        }\n");
        if (!checks.isEmpty()) {
            for (String check : checks) {
                sb.append(check).append("\n");
            }
        }
        sb.append("        return errors;\n");
        sb.append("    }\n\n");

        sb.append("    public static void validateOrThrow(").append(beanClass).append(" pojo) {\n");
        sb.append("        List<String> errors = validate(pojo);\n");
        sb.append("        if (!errors.isEmpty()) {\n");
        sb.append("            throw new IllegalArgumentException(String.join(\"; \", errors));\n");
        sb.append("        }\n");
        sb.append("    }\n");

        sb.append("}\n");
        return sb.toString();
    }
}
