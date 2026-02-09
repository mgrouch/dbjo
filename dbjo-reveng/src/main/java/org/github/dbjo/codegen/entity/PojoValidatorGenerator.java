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
import java.util.Locale;

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

            String src = renderValidator(cfg.validatorPkg(), cfg.beanPkg(), cfg.dbSchemaPkg(), beanClass, validatorClass, tm);
            Path outFile = outDir.resolve(validatorClass + ".java");
            FilesUtil.writeString(outFile, src, cfg.overwrite());
            System.out.println("Wrote: " + outFile);
            count++;
        }
        return count;
    }

    static String renderValidator(String validatorPkg, String beanPkg, String dbSchemaPkg, String beanClass, String validatorClass, TableModel tm) {
        List<String> checks = new ArrayList<>();
        String tableClass = tableMetaClassName(tm.table().schema(), tm.table().table());

        for (Col c : tm.cols()) {
            String field = Naming.sanitizeJavaIdentifier(Naming.toFieldName(c.colName()));
            String cap = Naming.capitalize(field);
            String getter = "pojo.get" + cap + (isVersionField(c, field) ? "Boxed" : "") + "()";
            String dbCol = c.colName();

            checks.add("        ValidationSupport.validateNullableAndLength(errors, \"" + dbCol + "\", COLS_BY_NAME.get(\"" + dbCol.toUpperCase(Locale.ROOT) + "\"), " + getter + ");");

            TypeMappings.JavaType jt = TypeMappings.mapSqlTypeToJava(c.sqlType(), c.typeName(), null);
            String javaType = jt.javaType();

            if (c.scale() >= 0 && (c.sqlType() == Types.DECIMAL || c.sqlType() == Types.NUMERIC)) {
                checks.add("        if (" + getter + " != null && " + getter + ".scale() > " + c.scale() + ") errors.add(\"" + dbCol + " scale must be <= " + c.scale() + "\");");
                if (c.size() > 0) {
                    checks.add("        if (" + getter + " != null && " + getter + ".precision() > " + c.size() + ") errors.add(\"" + dbCol + " precision must be <= " + c.size() + "\");");
                }
            }

            if (isYyyyMmDdIntDate(c, dbCol, javaType)) {
                checks.add("        if (" + getter + " != null && !ValidationSupport.isValidYyyyMmDd(" + getter + ")) errors.add(\"" + dbCol + " must be a valid date in yyyyMMdd format\");");
            }
        }

        StringBuilder sb = new StringBuilder(5000);
        sb.append("package ").append(validatorPkg).append(";\n\n");
        sb.append("import java.util.ArrayList;\n");
        sb.append("import java.util.List;\n");
        sb.append("import java.util.Map;\n");
        sb.append("import org.github.dbjo.meta.db.Col;\n");
        sb.append("import org.github.dbjo.meta.validation.ValidationSupport;\n");
        sb.append("import ").append(dbSchemaPkg).append(".tables.").append(tableClass).append(";\n");
        if (!validatorPkg.equals(beanPkg)) {
            sb.append("import ").append(beanPkg).append('.').append(beanClass).append(";\n");
        }
        sb.append("\n");

        sb.append("/**\n");
        sb.append(" * Auto-generated validator for ").append(tm.table().schema()).append('.').append(tm.table().table()).append(".\n");
        sb.append(" */\n");
        sb.append("public final class ").append(validatorClass).append(" {\n\n");
        sb.append("    private ").append(validatorClass).append("() {}\n\n");
        sb.append("    private static final Map<String, Col> COLS_BY_NAME = ValidationSupport.colsByName(").append(tableClass).append(".MODEL);\n\n");

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
        sb.append("        ValidationSupport.throwIfAny(validate(pojo));\n");
        sb.append("    }\n");

        sb.append("}\n");
        return sb.toString();
    }

    private static boolean isYyyyMmDdIntDate(Col c, String dbCol, String javaType) {
        if (!("Integer".equals(javaType) || "Long".equals(javaType) || "Short".equals(javaType) || "Byte".equals(javaType))) {
            return false;
        }
        if (!(c.sqlType() == Types.INTEGER || c.sqlType() == Types.BIGINT || c.sqlType() == Types.SMALLINT || c.sqlType() == Types.TINYINT
                || c.sqlType() == Types.NUMERIC || c.sqlType() == Types.DECIMAL)) {
            return false;
        }
        if (c.scale() > 0) {
            return false;
        }
        return dbCol.toLowerCase(Locale.ROOT).endsWith("_date");
    }

    private static boolean isVersionField(Col c, String fieldName) {
        return "version".equals(fieldName) && c.sqlType() == Types.INTEGER;
    }

    private static String tableMetaClassName(String schema, String table) {
        String s = (schema == null || schema.isBlank()) ? "" : safeClassPart(schema);
        String t = safeClassPart(table);
        return s + t + "Table";
    }

    private static String safeClassPart(String raw) {
        String cls = Naming.toClassName(raw);
        if (cls.isBlank()) cls = "X";
        if (!Character.isJavaIdentifierStart(cls.charAt(0))) cls = "_" + cls;

        StringBuilder b = new StringBuilder(cls.length());
        for (int i = 0; i < cls.length(); i++) {
            char ch = cls.charAt(i);
            b.append(Character.isJavaIdentifierPart(ch) ? ch : '_');
        }
        cls = b.toString();

        if (Naming.JAVA_KEYWORDS.contains(cls.toLowerCase(Locale.ROOT))) cls = cls + "X";
        return cls;
    }
}
