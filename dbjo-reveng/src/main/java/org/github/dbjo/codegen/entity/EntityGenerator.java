package org.github.dbjo.codegen.entity;

import org.github.dbjo.codegen.Config;
import org.github.dbjo.codegen.model.Col;
import org.github.dbjo.codegen.model.TableModel;
import org.github.dbjo.codegen.util.FilesUtil;
import org.github.dbjo.codegen.util.Naming;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Types;
import java.util.*;

public final class EntityGenerator {

    private final Config cfg;

    public EntityGenerator(Config cfg) {
        this.cfg = cfg;
    }

    public int generateAll(List<TableModel> tables) throws IOException {
        Path beanDir = cfg.codegenOutJava().resolve(cfg.beanPkg().replace('.', '/'));
        Path metaDir = cfg.codegenOutJava().resolve(cfg.metaPkg().replace('.', '/'));
        Files.createDirectories(beanDir);
        Files.createDirectories(metaDir);

        int count = 0;
        for (TableModel tm : tables) {
            String beanClass = Naming.toClassName(tm.table().table());

            String beanSrc = renderBean(cfg.beanPkg(), beanClass, tm);
            Path beanFile = beanDir.resolve(beanClass + ".java");
            FilesUtil.writeString(beanFile, beanSrc, cfg.overwrite());

            String metaClass = beanClass + "Meta";
            String metaSrc = renderMeta(cfg.metaPkg(), cfg.baseMetaPkg(), cfg.beanPkg(), beanClass, metaClass, tm);
            Path metaFile = metaDir.resolve(metaClass + ".java");
            FilesUtil.writeString(metaFile, metaSrc, cfg.overwrite());

            System.out.println("Wrote: " + beanFile);
            System.out.println("Wrote: " + metaFile);
            count++;
        }

        return count;
    }

    private static String renderBean(String pkg, String className, TableModel tm) {
        Set<String> imports = new TreeSet<>();
        imports.add("java.io.Serializable");
        imports.add("java.util.Objects");

        List<Field> fields = new ArrayList<>();
        for (Col c : tm.cols()) {
            JavaType jt = mapSqlTypeToJava(c.sqlType(), imports);
            String fieldName = Naming.sanitizeJavaIdentifier(Naming.toFieldName(c.colName()));
            boolean isPk = tm.pkColsUpper().contains(c.colName().toUpperCase(Locale.ROOT));
            fields.add(new Field(fieldName, jt.javaType, jt.classLiteral, c, isPk));
        }

        StringBuilder sb = new StringBuilder(10_000);
        sb.append("package ").append(pkg).append(";\n\n");
        for (String imp : imports) sb.append("import ").append(imp).append(";\n");
        sb.append("\n");

        sb.append("/**\n");
        sb.append(" * Auto-generated bean for ").append(tm.table().schema()).append(".").append(tm.table().table()).append("\n");
        sb.append(" */\n");
        sb.append("public class ").append(className).append(" implements Serializable {\n\n");
        sb.append("  private static final long serialVersionUID = 1L;\n\n");

        for (Field f : fields) {
            sb.append("  /** DB: ").append(f.col.typeName());
            if (f.isPk) sb.append(" (PK)");
            if ("YES".equalsIgnoreCase(f.col.isAutoIncrement())) sb.append(" (AI)");
            sb.append(" */\n");
            sb.append("  private ").append(f.javaType).append(" ").append(f.name).append(";\n\n");
        }

        sb.append("  public ").append(className).append("() {}\n\n");

        sb.append("  public ").append(className).append("(");
        for (int i = 0; i < fields.size(); i++) {
            if (i > 0) sb.append(", ");
            sb.append(fields.get(i).javaType).append(" ").append(fields.get(i).name);
        }
        sb.append(") {\n");
        for (Field f : fields) sb.append("    this.").append(f.name).append(" = ").append(f.name).append(";\n");
        sb.append("  }\n\n");

        for (Field f : fields) {
            String cap = Naming.capitalize(f.name);
            sb.append("  public ").append(f.javaType).append(" get").append(cap).append("() {\n")
                    .append("    return ").append(f.name).append(";\n")
                    .append("  }\n\n");
            sb.append("  public void set").append(cap).append("(").append(f.javaType).append(" ").append(f.name).append(") {\n")
                    .append("    this.").append(f.name).append(" = ").append(f.name).append(";\n")
                    .append("  }\n\n");
        }

        sb.append("  @Override\n");
        sb.append("  public boolean equals(Object o) {\n");
        sb.append("    if (this == o) return true;\n");
        sb.append("    if (o == null || getClass() != o.getClass()) return false;\n");
        sb.append("    ").append(className).append(" that = (").append(className).append(") o;\n");
        if (fields.isEmpty()) {
            sb.append("    return true;\n");
        } else {
            sb.append("    return ");
            for (int i = 0; i < fields.size(); i++) {
                Field f = fields.get(i);
                if (i > 0) sb.append("\n        && ");
                sb.append("Objects.equals(").append(f.name).append(", that.").append(f.name).append(")");
            }
            sb.append(";\n");
        }
        sb.append("  }\n\n");

        sb.append("  @Override\n");
        sb.append("  public int hashCode() {\n");
        sb.append("    return Objects.hash(");
        for (int i = 0; i < fields.size(); i++) {
            if (i > 0) sb.append(", ");
            sb.append(fields.get(i).name);
        }
        sb.append(");\n");
        sb.append("  }\n\n");

        sb.append("  @Override\n");
        sb.append("  public String toString() {\n");
        sb.append("    return \"").append(className).append("{\" +\n");
        for (int i = 0; i < fields.size(); i++) {
            Field f = fields.get(i);
            sb.append("        \"").append(i == 0 ? "" : ", ").append(f.name).append("=\" + ").append(f.name).append(" +\n");
        }
        sb.append("        \"}\";\n");
        sb.append("  }\n");

        sb.append("}\n");
        return sb.toString();
    }

    private static String renderMeta(
            String metaPkg,
            String baseMetaPkg,
            String beanPkg,
            String beanClass,
            String metaClass,
            TableModel tm
    ) {
        Set<String> imports = new TreeSet<>();
        imports.add("java.io.Serializable");
        imports.add("java.util.List");
        imports.add(beanPkg + "." + beanClass);

        // configurable base meta package
        imports.add(baseMetaPkg + ".EntityMeta");
        imports.add(baseMetaPkg + ".PropertyMeta");

        List<MetaProp> props = new ArrayList<>();
        for (Col c : tm.cols()) {
            String propName = Naming.sanitizeJavaIdentifier(Naming.toFieldName(c.colName()));
            JavaType jt = mapSqlTypeToJava(c.sqlType(), null);
            boolean isPk = tm.pkColsUpper().contains(c.colName().toUpperCase(Locale.ROOT));

            String constName = Naming.toUpperSnake(propName);
            props.add(new MetaProp(constName, propName, jt.javaType, jt.classLiteral, c, isPk));
        }

        StringBuilder sb = new StringBuilder(12_000);
        sb.append("package ").append(metaPkg).append(";\n\n");
        for (String imp : imports) sb.append("import ").append(imp).append(";\n");
        sb.append("\n");

        sb.append("/**\n");
        sb.append(" * Auto-generated entity meta for ").append(tm.table().schema()).append(".").append(tm.table().table()).append("\n");
        sb.append(" */\n");
        sb.append("public final class ").append(metaClass).append(" {\n\n");
        sb.append("  private ").append(metaClass).append("() {}\n\n");

        for (MetaProp p : props) {
            sb.append("  /** DB: ").append(p.col.typeName());
            if (p.isPk) sb.append(" (PK)");
            if ("YES".equalsIgnoreCase(p.col.isAutoIncrement())) sb.append(" (AI)");
            sb.append(" */\n");
            sb.append("  public static final PropertyMeta<").append(beanClass).append(", ").append(p.javaType).append("> ")
                    .append(p.constName).append(" = new PropertyMeta<>(")
                    .append("\"").append(p.propName).append("\", ")
                    .append(p.classLiteral).append(", ")
                    .append(beanClass).append("::get").append(Naming.capitalize(p.propName)).append(", ")
                    .append(beanClass).append("::set").append(Naming.capitalize(p.propName))
                    .append(");\n\n");
        }

        sb.append("  public static final List<String> _ALL_PROP_NAMES = List.of(\n");
        for (int i = 0; i < props.size(); i++) {
            sb.append("      \"").append(props.get(i).propName).append("\"");
            sb.append(i < props.size() - 1 ? ",\n" : "\n");
        }
        sb.append("  );\n\n");

        sb.append("  public static final List<Class<?>> _ALL_PROP_TYPES = List.of(\n");
        for (int i = 0; i < props.size(); i++) {
            sb.append("      ").append(props.get(i).classLiteral);
            sb.append(i < props.size() - 1 ? ",\n" : "\n");
        }
        sb.append("  );\n\n");

        sb.append("  @SuppressWarnings({\"rawtypes\", \"unchecked\"})\n");
        sb.append("  public static final List<PropertyMeta<").append(beanClass).append(", Serializable>> _ALL_PROPERTY_METAS =\n");
        sb.append("      (List) List.of(\n");
        for (int i = 0; i < props.size(); i++) {
            sb.append("          ").append(props.get(i).constName);
            sb.append(i < props.size() - 1 ? ",\n" : "\n");
        }
        sb.append("      );\n\n");

        sb.append("  public static final EntityMeta<").append(beanClass).append("> _META = new EntityMeta<>(\n");
        sb.append("      _ALL_PROPERTY_METAS,\n");
        sb.append("      _ALL_PROP_NAMES,\n");
        sb.append("      _ALL_PROP_TYPES\n");
        sb.append("  );\n");

        sb.append("}\n");
        return sb.toString();
    }

    private static JavaType mapSqlTypeToJava(int sqlType, Set<String> imports) {
        return switch (sqlType) {
            case Types.TINYINT, Types.SMALLINT -> new JavaType("Short", "Short.class");
            case Types.INTEGER -> new JavaType("Integer", "Integer.class");
            case Types.BIGINT -> new JavaType("Long", "Long.class");
            case Types.FLOAT, Types.REAL -> new JavaType("Float", "Float.class");
            case Types.DOUBLE -> new JavaType("Double", "Double.class");
            case Types.DECIMAL, Types.NUMERIC -> {
                if (imports != null) imports.add("java.math.BigDecimal");
                yield new JavaType("BigDecimal", "BigDecimal.class");
            }
            case Types.BIT, Types.BOOLEAN -> new JavaType("Boolean", "Boolean.class");

            case Types.CHAR, Types.VARCHAR, Types.LONGVARCHAR,
                    Types.NCHAR, Types.NVARCHAR, Types.LONGNVARCHAR -> new JavaType("String", "String.class");

            case Types.DATE -> {
                if (imports != null) imports.add("java.sql.Date");
                yield new JavaType("Date", "Date.class");
            }
            case Types.TIME, Types.TIME_WITH_TIMEZONE -> {
                if (imports != null) imports.add("java.sql.Time");
                yield new JavaType("Time", "Time.class");
            }
            case Types.TIMESTAMP, Types.TIMESTAMP_WITH_TIMEZONE -> {
                if (imports != null) imports.add("java.sql.Timestamp");
                yield new JavaType("Timestamp", "Timestamp.class");
            }

            case Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY, Types.BLOB -> new JavaType("byte[]", "byte[].class");

            default -> new JavaType("String", "String.class");
        };
    }

    private record JavaType(String javaType, String classLiteral) {}
    private record Field(String name, String javaType, String classLiteral, Col col, boolean isPk) {}
    private record MetaProp(String constName, String propName, String javaType, String classLiteral, Col col, boolean isPk) {}
}
