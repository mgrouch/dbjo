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
                boolean isEnum,
                EnumIndex.Binding enumBinding
        ) {}
        List<FieldInfo> fields = new ArrayList<>();

        for (Col c : tm.cols()) {
            String prop = Naming.sanitizeJavaIdentifier(Naming.toFieldName(c.colName()));
            String cap = Naming.capitalize(prop);

            boolean nullable = c.nullable();

            var jt = TypeMappings.mapSqlTypeToJava(c.sqlType(), null);
            var pt = TypeMappings.mapSqlTypeToProto(c.sqlType());

            boolean protoOptional = cfg.protoExperimentalOptional() && nullable && pt.allowOptional(); // aligned with ProtoGenerator
            boolean hasPresence = pt.isMessage() || protoOptional;

            EnumIndex.Binding eb = (enumIndex == null) ? null : enumIndex.find(schema, table, c.colName());
            boolean isEnum = eb != null;

            fields.add(new FieldInfo(prop, cap, jt, pt, nullable, hasPresence, isEnum, eb));

            // imports needed by helper conversions for NON-enum java types
            if (!isEnum) {
                if ("BigDecimal".equals(jt.javaType())) needBigDecimal = true;
                if ("byte[]".equals(jt.javaType())) needByteString = true;
                if ("Date".equals(jt.javaType())) needSqlDate = true;
                if ("Time".equals(jt.javaType())) needSqlTime = true;
                if ("Timestamp".equals(jt.javaType())) {
                    needSqlTimestamp = true;
                    needProtoTimestamp = true;
                }
            }

            if ("google.protobuf.Timestamp".equals(pt.protoType())) needProtoTimestamp = true;

            if (isEnum) {
                imports.add(eb.enumJavaFqn());
            }
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
            String getter = "pojo.get" + f.cap + "()";
            if (f.isEnum) {
                // enum -> key
                String keyExpr = getter + "." + f.enumBinding.keyGetterMethod() + "()";
                sb.append("        if (").append(getter).append(" != null) b.set").append(f.cap).append("(").append(keyExpr).append(");\n");
            } else {
                String setCall = "b.set" + f.cap + "(" + toProtoExpr(f.jt.javaType(), getter) + ");";
                sb.append("        if (").append(getter).append(" != null) ").append(setCall).append("\n");
            }
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

            if (f.isEnum) {
                String lookupNullable = f.enumBinding.enumJavaSimple() + "." + f.enumBinding.lookupNullableMethod();
                String lookupNonNull = f.enumBinding.enumJavaSimple() + "." + nonNullLookupMethod(f.enumBinding.lookupNullableMethod());

                if (f.nullable) {
                    if (f.hasPresence) {
                        sb.append("        ").append(setter).append("(p.has").append(f.cap).append("() ? ")
                                .append(lookupNullable).append("(").append(protoGet).append(") : null);\n");
                    } else {
                        String hasValueExpr = protoHasValueExpr(f.pt.protoType(), protoGet);
                        if (hasValueExpr != null) {
                            sb.append("        ").append(setter).append("(").append(hasValueExpr)
                                    .append(" ? ").append(lookupNullable).append("(").append(protoGet).append(") : null);\n");
                        } else {
                            // cannot represent null reliably -> best effort
                            sb.append("        ").append(setter).append("(").append(lookupNullable).append("(").append(protoGet).append("));\n");
                        }
                    }
                } else {
                    sb.append("        ").append(setter).append("(").append(lookupNonNull).append("(").append(protoGet).append("));\n");
                }

                continue;
            }

            // non-enum: existing mapping
            String rhs = fromProtoExpr(f.jt.javaType(), protoGet);
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

    private static String nonNullLookupMethod(String nullableMethod) {
        if (nullableMethod == null) return "of";
        if (nullableMethod.endsWith("Nullable")) {
            return nullableMethod.substring(0, nullableMethod.length() - "Nullable".length());
        }
        // fallback: assume already non-null lookup
        return nullableMethod;
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

    private static String nz(String s) { return s == null ? "" : s; }
}
