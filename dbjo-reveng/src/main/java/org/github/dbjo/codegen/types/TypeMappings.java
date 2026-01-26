package org.github.dbjo.codegen.types;

import java.sql.Types;
import java.util.Locale;
import java.util.Set;

public final class TypeMappings {
    private TypeMappings() {}

    public record JavaType(String javaType, String classLiteral) {}

    public record ProtoType(String protoType, boolean allowOptional, boolean needsTimestamp, boolean isMessage) {
        public static ProtoType scalar(String t) { return new ProtoType(t, true, false, false); }
        public static ProtoType message(String t, boolean ts) { return new ProtoType(t, false, ts, true); }
    }

    // Java mapping

    /** Back-compat entry point (no typeName hints). */
    public static JavaType mapSqlTypeToJava(int sqlType, Set<String> imports) {
        return mapSqlTypeToJava(sqlType, null, imports);
    }

    /**
     * Java mapping with TYPE_NAME hints (needed for MSSQL/Sybase/Oracle oddities
     * that often appear as Types.OTHER / non-standard).
     */
    public static JavaType mapSqlTypeToJava(int sqlType, String typeName, Set<String> imports) {
        String tn = norm(typeName);

        // ---- Vendor/type-name hints first (most important for MSSQL/Oracle/Sybase) ----

        // MSSQL: UNIQUEIDENTIFIER -> UUID (often Types.OTHER or CHAR/VARCHAR depending on driver)
        if (isOneOf(tn, "UNIQUEIDENTIFIER", "UUID")) {
            if (imports != null) imports.add("java.util.UUID");
            return new JavaType("UUID", "UUID.class");
        }

        // MSSQL: DATETIMEOFFSET, Oracle: TIMESTAMP WITH TIME ZONE
        if (tn.contains("DATETIMEOFFSET") || (tn.contains("TIMESTAMP") && tn.contains("TIME ZONE"))) {
            if (imports != null) imports.add("java.time.OffsetDateTime");
            return new JavaType("OffsetDateTime", "OffsetDateTime.class");
        }

        // TIME WITH TIME ZONE (less common, but supported in some DBs)
        if (tn.contains("TIME") && tn.contains("TIME ZONE") && !tn.contains("TIMESTAMP")) {
            if (imports != null) imports.add("java.time.OffsetTime");
            return new JavaType("OffsetTime", "OffsetTime.class");
        }

        // Oracle: XMLTYPE / JDBC SQLXML
        if (tn.contains("XMLTYPE") || sqlType == Types.SQLXML) {
            return new JavaType("String", "String.class");
        }

        // Sybase/SQL Server: TEXT/NTEXT/IMAGE historically
        if (isOneOf(tn, "TEXT", "NTEXT")) {
            return new JavaType("String", "String.class");
        }
        if (isOneOf(tn, "IMAGE")) {
            return new JavaType("byte[]", "byte[].class");
        }

        // ---- Standard JDBC Types ----
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

            case Types.DATE -> {
                if (imports != null) imports.add("java.sql.Date");
                yield new JavaType("Date", "Date.class");
            }

            case Types.TIME -> {
                if (imports != null) imports.add("java.sql.Time");
                yield new JavaType("Time", "Time.class");
            }

            // Prefer java.time for TZ-aware variants (portable across Oracle + MSSQL)
            case Types.TIME_WITH_TIMEZONE -> {
                if (imports != null) imports.add("java.time.OffsetTime");
                yield new JavaType("OffsetTime", "OffsetTime.class");
            }

            case Types.TIMESTAMP -> {
                if (imports != null) imports.add("java.sql.Timestamp");
                yield new JavaType("Timestamp", "Timestamp.class");
            }

            case Types.TIMESTAMP_WITH_TIMEZONE -> {
                if (imports != null) imports.add("java.time.OffsetDateTime");
                yield new JavaType("OffsetDateTime", "OffsetDateTime.class");
            }

            case Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY,
                    Types.BLOB -> new JavaType("byte[]", "byte[].class");

            // ROWID/OTHER: keep portable
            case Types.ROWID, Types.OTHER, Types.JAVA_OBJECT,
                    Types.DISTINCT, Types.STRUCT,
                    Types.ARRAY -> new JavaType("String", "String.class");

            default -> new JavaType("String", "String.class");
        };
    }

    // --------------------------------------------------------------------
    // Proto mapping
    // --------------------------------------------------------------------

    /** Back-compat entry point (no typeName hints). */
    public static ProtoType mapSqlTypeToProto(int sqlType) {
        return mapSqlTypeToProto(sqlType, null);
    }

    /** Proto mapping with TYPE_NAME hints. */
    public static ProtoType mapSqlTypeToProto(int sqlType, String typeName) {
        String tn = norm(typeName);

        // UUID / uniqueidentifier -> string (portable)
        if (isOneOf(tn, "UNIQUEIDENTIFIER", "UUID")) return ProtoType.scalar("string");

        // datetimeoffset / timestamp with time zone -> Timestamp
        if (tn.contains("DATETIMEOFFSET") || (tn.contains("TIMESTAMP") && tn.contains("TIME ZONE"))) {
            return ProtoType.message("google.protobuf.Timestamp", true);
        }

        // SQLXML / XMLTYPE -> string
        if (tn.contains("XMLTYPE") || sqlType == Types.SQLXML) return ProtoType.scalar("string");

        // Standard JDBC
        return switch (sqlType) {
            case Types.TINYINT, Types.SMALLINT, Types.INTEGER -> ProtoType.scalar("int32");
            case Types.BIGINT -> ProtoType.scalar("int64");

            case Types.FLOAT, Types.REAL -> ProtoType.scalar("float");
            case Types.DOUBLE -> ProtoType.scalar("double");

            // portable

            case Types.BIT, Types.BOOLEAN -> ProtoType.scalar("bool");

            case Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY,
                    Types.BLOB -> ProtoType.scalar("bytes");

            case Types.TIMESTAMP, Types.TIMESTAMP_WITH_TIMEZONE ->
                    ProtoType.message("google.protobuf.Timestamp", true);

            default -> ProtoType.scalar("string");
        };
    }

    // --------------------------------------------------------------------
    // helpers
    // --------------------------------------------------------------------

    private static String norm(String s) {
        if (s == null) return "";
        String x = s.trim().toUpperCase(Locale.ROOT);
        // normalize whitespace
        x = x.replace('\t', ' ');
        while (x.contains("  ")) x = x.replace("  ", " ");
        return x;
    }

    private static boolean isOneOf(String s, String... opts) {
        if (s == null) return false;
        for (String o : opts) if (s.equals(o)) return true;
        return false;
    }
}
