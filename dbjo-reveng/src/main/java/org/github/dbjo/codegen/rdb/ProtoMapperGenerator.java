package org.github.dbjo.codegen.rdb;

import org.github.dbjo.codegen.Config;
import org.github.dbjo.codegen.model.Col;
import org.github.dbjo.codegen.model.TableModel;
import org.github.dbjo.codegen.types.TypeMappings;
import org.github.dbjo.codegen.util.FilesUtil;
import org.github.dbjo.codegen.util.Naming;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DatabaseMetaData;
import java.util.*;

public final class ProtoMapperGenerator {
    private final Config cfg;

    public ProtoMapperGenerator(Config cfg) {
        this.cfg = cfg;
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

        // imports needed by conversions
        Set<String> imports = new TreeSet<>();
        imports.add("org.github.dbjo.rdb.ProtobufPojoCodec");
        if (importBean) imports.add(beanPkg + "." + beanClass);

        boolean needBigDecimal = false;
        boolean needByteString = false;
        boolean needSqlDate = false;
        boolean needSqlTime = false;
        boolean needSqlTimestamp = false;
        boolean needProtoTimestamp = false;

        record FieldInfo(String prop, String cap, TypeMappings.JavaType jt, TypeMappings.ProtoType pt, boolean nullable, boolean hasPresence) {}
        List<FieldInfo> fields = new ArrayList<>();

        for (Col c : tm.cols()) {
            String prop = Naming.sanitizeJavaIdentifier(Naming.toFieldName(c.colName()));
            String cap = Naming.capitalize(prop);

            boolean nullable = c.nullable() != DatabaseMetaData.columnNoNulls;

            var jt = TypeMappings.mapSqlTypeToJava(c.sqlType(), null);
            var pt = TypeMappings.mapSqlTypeToProto(c.sqlType());

            boolean protoOptional = nullable && pt.allowOptional(); // we only mark optional for scalars/bytes/string
            boolean hasPresence = pt.isMessage() || protoOptional;  // message fields have hasX() too

            fields.add(new FieldInfo(prop, cap, jt, pt, nullable, hasPresence));

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
        if (needProtoTimestamp) imports.add("com.google.protobuf.Timestamp");

        StringBuilder sb = new StringBuilder(6000);
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
            String getter = "pojo.get" + f.cap + "()";
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
            String setter = "u.set" + f.cap;
            String protoGet = "p.get" + f.cap + "()";
            String rhs = fromProtoExpr(f.jt.javaType(), f.pt.protoType(), protoGet);

            if (f.nullable && f.hasPresence) {
                sb.append("        ").append(setter).append("(p.has").append(f.cap).append("() ? ").append(rhs).append(" : null);\n");
            } else {
                sb.append("        ").append(setter).append("(").append(rhs).append(");\n");
            }
        }

        sb.append("        return u;\n");
        sb.append("    }\n\n");

        // Timestamp helpers if needed
        if (needProtoTimestamp || needSqlTimestamp) {
            sb.append("    private static Timestamp fromProtoTimestamp(Timestamp t) {\n");
            sb.append("        long millis = t.getSeconds() * 1000L + (t.getNanos() / 1_000_000L);\n");
            sb.append("        Timestamp ts = new Timestamp(millis);\n");
            sb.append("        ts.setNanos(t.getNanos());\n");
            sb.append("        return ts;\n");
            sb.append("    }\n\n");
            sb.append("    private static Timestamp toProtoTimestamp(Timestamp ts) {\n");
            sb.append("        long seconds = ts.getTime() / 1000L;\n");
            sb.append("        int nanos = ts.getNanos();\n");
            sb.append("        return Timestamp.newBuilder().setSeconds(seconds).setNanos(nanos).build();\n");
            sb.append("    }\n\n");
        }

        sb.append("}\n");
        return sb.toString();
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

    private static String fromProtoExpr(String javaType, String protoType, String protoGetExpr) {
        // protoType is useful for Timestamp (message)
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
}
