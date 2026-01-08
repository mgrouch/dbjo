package org.github.dbjo.codegen.types;

import java.sql.Types;
import java.util.Set;

public final class TypeMappings {
    private TypeMappings() {}

    public record JavaType(String javaType, String classLiteral) {}

    public record ProtoType(String protoType, boolean allowOptional, boolean needsTimestamp, boolean isMessage) {
        public static ProtoType scalar(String t) { return new ProtoType(t, true, false, false); }
        public static ProtoType message(String t, boolean ts) { return new ProtoType(t, false, ts, true); }
    }

    // Bean/Meta-friendly Java mapping (wrapper types, Serializable-friendly)
    public static JavaType mapSqlTypeToJava(int sqlType, Set<String> imports) {
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

            case Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY, Types.BLOB ->
                    new JavaType("byte[]", "byte[].class");

            default -> new JavaType("String", "String.class");
        };
    }

    // Proto base type mapping (nullable handled by "optional" elsewhere)
    public static ProtoType mapSqlTypeToProto(int sqlType) {
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
                    ProtoType.scalar("string"); // portable ISO string

            default -> ProtoType.scalar("string");
        };
    }
}
