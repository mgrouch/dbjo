package org.github.dbjo.codegen.proto;

import org.github.dbjo.codegen.Config;
import org.github.dbjo.codegen.model.Col;
import org.github.dbjo.codegen.model.TableModel;
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

    public ProtoGenerator(Config cfg) {
        this.cfg = cfg;
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
            String fileName = Naming.toLowerSnake(tm.table().schema()) + "_" + Naming.toLowerSnake(tm.table().table()) + ".proto";
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

        List<ProtoField> fields = new ArrayList<>();
        for (Col c : tm.cols()) {
            boolean nullable = c.nullable() != DatabaseMetaData.columnNoNulls;
            boolean isPk = tm.pkColsUpper().contains(c.colName().toUpperCase(Locale.ROOT));

            ProtoType pt = mapSqlTypeToProto(c.sqlType());
            if (pt.needsTimestamp) needTimestamp = true;

            String fieldName = Naming.toLowerSnake(Naming.sanitizeProtoIdentifier(Naming.toFieldName(c.colName())));
            int fieldNumber = Math.max(1, c.pos());

            boolean isOptional = nullable && pt.allowOptional; // no optional for message types (Timestamp)

            fields.add(new ProtoField(fieldName, pt.protoType, isOptional, fieldNumber, c, isPk));
        }

        String protoPkg = cfg.protoPkgBase() + "." + Naming.toLowerSnake(tm.table().schema());

        StringBuilder sb = new StringBuilder(12_000);
        sb.append("syntax = \"proto3\";\n\n");
        sb.append("package ").append(protoPkg).append(";\n\n");

        sb.append("option java_package = \"").append(cfg.protoJavaPkg()).append("\";\n");
        sb.append("option java_multiple_files = true;\n");
        sb.append("option java_outer_classname = \"").append(messageName).append(cfg.protoOuterSuffix()).append("\";\n\n");

        if (needTimestamp) sb.append("import \"google/protobuf/timestamp.proto\";\n\n");

        sb.append("// DB: ").append(tm.table().schema()).append(".").append(tm.table().table()).append("\n");
        sb.append("message ").append(messageName).append(" {\n");

        for (ProtoField f : fields) {
            sb.append("  // DB: ").append(f.col.typeName());
            if (f.isPk) sb.append(" (PK)");
            if ("YES".equalsIgnoreCase(f.col.isAutoIncrement())) sb.append(" (AI)");
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

            case Types.DECIMAL, Types.NUMERIC -> ProtoType.scalar("string"); // portable

            case Types.BIT, Types.BOOLEAN -> ProtoType.scalar("bool");

            case Types.CHAR, Types.VARCHAR, Types.LONGVARCHAR,
                    Types.NCHAR, Types.NVARCHAR, Types.LONGNVARCHAR -> ProtoType.scalar("string");

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

    private record ProtoField(String name, String protoType, boolean optional, int number, Col col, boolean isPk) {}
}
