package org.github.dbjo.codegen.rdb;

import org.github.dbjo.codegen.Config;
import org.github.dbjo.meta.db.Col;
import org.github.dbjo.meta.db.TableModel;
import org.github.dbjo.codegen.types.TypeMappings;
import org.github.dbjo.codegen.util.EnumIndex;
import org.github.dbjo.codegen.util.FilesUtil;
import org.github.dbjo.codegen.util.Naming;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Types;
import java.util.*;

public final class ProtoMapperGenerator {
    private final Config cfg;
    private final EnumIndex enumIndex; // may be null

    public ProtoMapperGenerator(Config cfg) {
        this(cfg, null);
    }

    public ProtoMapperGenerator(Config cfg, EnumIndex enumIndex) {
        this.cfg = Objects.requireNonNull(cfg, "cfg");
        this.enumIndex = enumIndex;
    }

    public int generateAll(List<TableModel> tables) throws IOException {
        Path outDir = cfg.codegenOutJava().resolve(cfg.protoMapperPkg().replace('.', '/'));
        Files.createDirectories(outDir);

        int count = 0;
        for (TableModel tm : tables) {
            String beanClass = Naming.toClassName(tm.table().table());
            String mapperClass = beanClass + cfg.protoMapperSuffix();
            String protoFqn = cfg.protoJavaPkg() + "." + beanClass;

            String src = renderMapper(cfg.protoMapperPkg(), cfg.beanPkg(), mapperClass, beanClass, protoFqn, tm);

            Path outFile = outDir.resolve(mapperClass + ".java");
            FilesUtil.writeString(outFile, src, cfg.overwrite());
            System.out.println("Wrote: " + outFile);
            count++;
        }

        return count;
    }

    private String renderMapper(
            String mapperPkg,
            String beanPkg,
            String mapperClass,
            String beanClass,
            String protoFqn,
            TableModel tm
    ) {
        boolean importBean = beanPkg != null && !beanPkg.equals(mapperPkg);

        String schema = nz(tm.table().schema());
        String table  = nz(tm.table().table());

        Set<String> imports = new TreeSet<>();
        imports.add("org.github.dbjo.rdb.ProtobufPojoCodec");
        if (importBean) imports.add(beanPkg + "." + beanClass);

        boolean needBigDecimal = false;
        boolean needByteString = false;
        boolean needSqlDate = false;
        boolean needSqlTime = false;
        boolean needSqlTimestamp = false;
        boolean needProtoTimestamp = false;

        record FieldInfo(
                String prop,
                String cap,
                TypeMappings.JavaType jt,
                TypeMappings.ProtoType pt,
                boolean nullable,
                boolean hasPresence,
                boolean isVersion
        ) {}
        List<FieldInfo> fields = new ArrayList<>();

        for (Col c : tm.cols()) {
            String prop = Naming.sanitizeJavaIdentifier(Naming.toFieldName(c.colName()));
            String cap = Naming.capitalize(prop);

            boolean nullable = c.nullable();

            EnumIndex.Binding eb = (enumIndex == null) ? null : enumIndex.find(schema, table, c.colName(), Types.OTHER, true);
            var jt = (eb == null)
                    ? TypeMappings.mapSqlTypeToJava(c.sqlType(), c.typeName(), null)
                    : TypeMappings.mapSqlTypeToJava(eb.enumKeySqlType(), null);
            var pt = (eb == null)
                    ? TypeMappings.mapSqlTypeToProto(c.sqlType(), c.typeName())
                    : TypeMappings.mapSqlTypeToProto(eb.enumKeySqlType(), null);

            boolean protoOptional = cfg.protoExperimentalOptional() && nullable && pt.allowOptional(); // aligned with ProtoGenerator
            boolean hasPresence = pt.isMessage() || protoOptional;

            boolean isVersion = isVersionField(c, prop);

            fields.add(new FieldInfo(prop, cap, jt, pt, nullable, hasPresence, isVersion));

            if ("BigDecimal".equals(jt.javaType())) needBigDecimal = true;
            if ("byte[]".equals(jt.javaType())) needByteString = true;
            if ("Date".equals(jt.javaType())) needSqlDate = true;
            if ("Time".equals(jt.javaType())) needSqlTime = true;
            if ("Timestamp".equals(jt.javaType())) {
                needSqlTimestamp = true;
                needProtoTimestamp = true;
            }

            if ("google.protobuf.Timestamp".equals(pt.protoType())) needProtoTimestamp = true;
        }

        if (needBigDecimal) imports.add("java.math.BigDecimal");
        if (needSqlDate) imports.add("java.sql.Date");
        if (needSqlTime) imports.add("java.sql.Time");
        if (needSqlTimestamp) imports.add("java.sql.Timestamp");
        if (needByteString) imports.add("com.google.protobuf.ByteString");
        // IMPORTANT: do NOT import com.google.protobuf.Timestamp (name-clash with java.sql.Timestamp)

        StringBuilder sb = new StringBuilder(9000);
        sb.append("package ").append(mapperPkg).append(";\n\n");
        for (String imp : imports) sb.append("import ").append(imp).append(";\n");
        sb.append("\n");

        sb.append("public final class ").append(mapperClass).append("\n")
                .append("        implements ProtobufPojoCodec.ProtoMapper<")
                .append(beanClass).append(", ").append(protoFqn).append("> {\n\n");

        // toProto
        sb.append("    @Override\n");
        sb.append("    public ").append(protoFqn).append(" toProto(").append(beanClass).append(" pojo) {\n");
        sb.append("        var b = ").append(protoFqn).append(".newBuilder();\n\n");

        for (FieldInfo f : fields) {
            String getter = f.isVersion ? "pojo.get" + f.cap + "Boxed()" : "pojo.get" + f.cap + "()";
            String setCall = "b.set" + f.cap + "(" + toProtoExpr(f.jt.javaType(), getter) + ");";
            sb.append("        if (").append(getter).append(" != null) ").append(setCall).append("\n");
        }

        sb.append("\n        return b.build();\n");
        sb.append("    }\n\n");

        // fromProto
        sb.append("    @Override\n");
        sb.append("    public ").append(beanClass).append(" fromProto(").append(protoFqn).append(" p) {\n");
        sb.append("        ").append(beanClass).append(" u = new ").append(beanClass).append("();\n");

        for (FieldInfo f : fields) {
            String setter = f.isVersion ? "u.set" + f.cap + "Boxed" : "u.set" + f.cap;
            String protoGet = "p.get" + f.cap + "()";

            String rhs = fromProtoExpr(f.jt.javaType(), protoGet);
            if (f.nullable && f.hasPresence) {
                sb.append("        ").append(setter).append("(p.has").append(f.cap).append("() ? ").append(rhs).append(" : null);\n");
            } else if (f.nullable && "string".equals(f.pt.protoType()) && nullSentinelByEmptyString(f.jt.javaType())) {
                // proto3 scalar strings have no presence unless optional; treat empty string as null for nullable SQL values
                sb.append("        ").append(setter).append("(").append(protoGet).append(".isEmpty() ? null : ").append(rhs).append(");\n");
            } else {
                sb.append("        ").append(setter).append("(").append(rhs).append(");\n");
            }
        }

        sb.append("        return u;\n");
        sb.append("    }\n\n");

        // Timestamp helpers if needed
        if (needProtoTimestamp || needSqlTimestamp) {
            sb.append("    private static java.sql.Timestamp fromProtoTimestamp(com.google.protobuf.Timestamp t) {\n");
            sb.append("        long millis = t.getSeconds() * 1000L + (t.getNanos() / 1_000_000L);\n");
            sb.append("        java.sql.Timestamp ts = new java.sql.Timestamp(millis);\n");
            sb.append("        ts.setNanos(t.getNanos());\n");
            sb.append("        return ts;\n");
            sb.append("    }\n\n");

            sb.append("    private static com.google.protobuf.Timestamp toProtoTimestamp(java.sql.Timestamp ts) {\n");
            sb.append("        long seconds = ts.getTime() / 1000L;\n");
            sb.append("        int nanos = ts.getNanos();\n");
            sb.append("        return com.google.protobuf.Timestamp.newBuilder().setSeconds(seconds).setNanos(nanos).build();\n");
            sb.append("    }\n\n");
        }

        sb.append("}\n");
        return sb.toString();
    }

    private static boolean isVersionField(Col col, String fieldName) {
        return "version".equals(fieldName) && col.sqlType() == Types.INTEGER;
    }

    private static String toProtoExpr(String javaType, String getterExpr) {
        return switch (javaType) {
            case "byte[]" -> "ByteString.copyFrom(" + getterExpr + ")";
            case "BigDecimal" -> getterExpr + ".toPlainString()";
            case "Date", "Time" -> getterExpr + ".toString()";
            case "Timestamp" -> "toProtoTimestamp(" + getterExpr + ")";
            default -> getterExpr;
        };
    }

    private static String fromProtoExpr(String javaType, String protoGetExpr) {
        return switch (javaType) {
            case "Short" -> "(short) " + protoGetExpr;
            case "byte[]" -> protoGetExpr + ".toByteArray()";
            case "BigDecimal" -> "new BigDecimal(" + protoGetExpr + ")";
            case "Date" -> "Date.valueOf(" + protoGetExpr + ")";
            case "Time" -> "Time.valueOf(" + protoGetExpr + ")";
            case "Timestamp" -> "fromProtoTimestamp(" + protoGetExpr + ")";
            default -> protoGetExpr;
        };
    }

    /**
     * When proto field has no presence (not optional, not message), we can sometimes infer "unset" from default.
     * Returns an expression like "!p.getX().isEmpty()" or "p.getX() != 0", else null if not safely representable.
     */
    private static String protoHasValueExpr(String protoType, String protoGetExpr) {
        if (protoType == null) return null;
        return switch (protoType) {
            case "string" -> "!" + protoGetExpr + ".isEmpty()";
            case "bytes" -> protoGetExpr + ".size() != 0";
            case "int32", "int64", "uint32", "uint64", "sint32", "sint64",
                    "fixed32", "fixed64", "sfixed32", "sfixed64" -> protoGetExpr + " != 0";
            default -> null; // bool/float/double can't represent null reliably without optional
        };
    }

    private static boolean nullSentinelByEmptyString(String javaType) {
        return switch (javaType) {
            case "BigDecimal", "Date", "Time" -> true;
            default -> false;
        };
    }

    private static String nz(String s) { return s == null ? "" : s; }
}
