package org.github.dbjo.codegen.proto;

import org.github.dbjo.codegen.Config;
import org.github.dbjo.meta.db.Col;
import org.github.dbjo.meta.db.TableModel;
import org.github.dbjo.codegen.util.EnumIndex;
import org.github.dbjo.codegen.util.FilesUtil;
import org.github.dbjo.codegen.util.Naming;

import java.io.IOException;
import java.sql.DatabaseMetaData;
import java.sql.Types;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

public final class ProtoGenerator {
    private final Config cfg;
    private final EnumIndex enumIndex; // may be null

    public ProtoGenerator(Config cfg) {
        this(cfg, null);
    }

    public ProtoGenerator(Config cfg, EnumIndex enumIndex) {
        this.cfg = Objects.requireNonNull(cfg, "cfg");
        this.enumIndex = enumIndex; // optional
    }

    public List<Path> generateAll(List<TableModel> tables) throws IOException {
        Files.createDirectories(cfg.protoOutProto());
        Files.createDirectories(cfg.protoOutJava());

        if (!cfg.protoPerTable()) {
            throw new UnsupportedOperationException("protoPerTable=false not implemented yet");
        }

        List<Path> out = new ArrayList<>();
        for (TableModel tm : tables) {
            String msgName = Naming.toClassName(tm.table().table());

            String schema = nz(tm.table().schema());
            String schemaPart = schema.isBlank() ? "default" : schema;

            String fileName =
                    Naming.toLowerSnake(schemaPart) + "_" + Naming.toLowerSnake(tm.table().table()) + ".proto";
            Path protoFile = cfg.protoOutProto().resolve(fileName);

            String src = renderProtoFile(tm, msgName);
            FilesUtil.writeString(protoFile, src, cfg.overwrite());
            out.add(protoFile);

            System.out.println("Wrote: " + protoFile);
        }

        return out;
    }

    private String renderProtoFile(TableModel tm, String messageName) {
        boolean needTimestamp = false;

        String schema = nz(tm.table().schema());
        String table = nz(tm.table().table());

        List<ProtoField> fields = new ArrayList<>();
        for (Col c : tm.cols()) {
            boolean nullable = c.nullable() != DatabaseMetaData.columnNoNulls;
            boolean isPk = tm.pkColsUpper().contains(c.colName().toUpperCase(Locale.ROOT));

            ProtoType pt = mapSqlTypeToProto(c.sqlType());
            if (pt.needsTimestamp) needTimestamp = true;

            String fieldName = Naming.toLowerSnake(
                    Naming.sanitizeProtoIdentifier(Naming.toFieldName(c.colName()))
            );
            int fieldNumber = Math.max(1, c.pos());

            // IMPORTANT: only emit proto3 optional if cfg says so.
            boolean protoOptionalEnabled = cfg.protoExperimentalOptional();
            boolean isOptional = protoOptionalEnabled && nullable && pt.allowOptional;

            EnumIndex.Binding eb = (enumIndex == null) ? null : enumIndex.find(schema, table, c.colName());

            fields.add(new ProtoField(fieldName, pt.protoType, isOptional, fieldNumber, c, isPk, eb));
        }

        String schemaPart = schema.isBlank() ? "default" : schema;
        String protoPkg = cfg.protoPkgBase() + "." + Naming.toLowerSnake(schemaPart);

        StringBuilder sb = new StringBuilder(12_000);
        sb.append("syntax = \"proto3\";\n\n");
        sb.append("package ").append(protoPkg).append(";\n\n");

        sb.append("option java_package = \"").append(cfg.protoJavaPkg()).append("\";\n");
        sb.append("option java_multiple_files = true;\n");
        sb.append("option java_outer_classname = \"").append(messageName).append(cfg.protoOuterSuffix()).append("\";\n\n");

        if (needTimestamp) sb.append("import \"google/protobuf/timestamp.proto\";\n\n");

        sb.append("// DB: ").append(schemaPart).append(".").append(table).append("\n");
        sb.append("message ").append(messageName).append(" {\n");

        for (ProtoField f : fields) {
            sb.append("  // DB: ").append(f.col.typeName());
            if (f.isPk) sb.append(" (PK)");
            if ("YES".equalsIgnoreCase(f.col.isAutoIncrement())) sb.append(" (AI)");
            if (f.enumBinding != null) {
                sb.append("\n  // Enum: ").append(f.enumBinding.enumJavaSimple())
                        .append(" (lookup=").append(f.enumBinding.lookupNullableMethod())
                        .append(", keyGetter=").append(f.enumBinding.keyGetterMethod()).append(")");
            }
            sb.append("\n");

            sb.append("  ");
            if (f.optional) sb.append("optional ");
            sb.append(f.protoType).append(" ").append(f.name)
                    .append(" = ").append(f.number).append(";\n\n");
        }

        sb.append("}\n");
        return sb.toString();
    }

    private static ProtoType mapSqlTypeToProto(int sqlType) {
        return switch (sqlType) {
            case Types.TINYINT, Types.SMALLINT, Types.INTEGER -> ProtoType.scalar("int32");
            case Types.BIGINT -> ProtoType.scalar("int64");

            case Types.FLOAT, Types.REAL -> ProtoType.scalar("float");
            case Types.DOUBLE -> ProtoType.scalar("double");

            case Types.DECIMAL, Types.NUMERIC, Types.CHAR, Types.VARCHAR, Types.LONGVARCHAR, Types.NCHAR, Types.NVARCHAR, Types.LONGNVARCHAR -> ProtoType.scalar("string"); // portable

            case Types.BIT, Types.BOOLEAN -> ProtoType.scalar("bool");

            case Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY, Types.BLOB -> ProtoType.scalar("bytes");

            case Types.TIMESTAMP, Types.TIMESTAMP_WITH_TIMEZONE ->
                    ProtoType.message("google.protobuf.Timestamp", true);

            case Types.DATE, Types.TIME, Types.TIME_WITH_TIMEZONE ->
                    ProtoType.scalar("string"); // portable

            default -> ProtoType.scalar("string");
        };
    }

    private record ProtoType(String protoType, boolean allowOptional, boolean needsTimestamp) {
        static ProtoType scalar(String t) { return new ProtoType(t, true, false); }
        static ProtoType message(String t, boolean ts) { return new ProtoType(t, false, ts); }
    }

    private record ProtoField(
            String name,
            String protoType,
            boolean optional,
            int number,
            Col col,
            boolean isPk,
            EnumIndex.Binding enumBinding
    ) {}

    private static String nz(String s) { return s == null ? "" : s; }
}
