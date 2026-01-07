package org.github.dbjo;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermission;
import java.sql.*;
import java.util.*;

/**
 * Generates:
 *  - .proto per table in: target/dbjo-generated/proto
 *  - Java classes from protoc in: target/dbjo-generated/proto-java
 *
 * Requirements:
 *  - protoc available on PATH (or set PROTOC env var / system property below)
 *  - protobuf-java runtime dependency in your app build
 *
 * Notes:
 *  - Uses google.protobuf wrappers for NULLable DB columns.
 */
public final class DbjoProtoCodegen {

    private static final String DEFAULT_URL  = "jdbc:hsqldb:hsql://localhost:9001/dbjo";
    private static final String DEFAULT_USER = "SA";
    private static final String DEFAULT_PASS = "";

    private static final Path OUT_BASE   = Paths.get("target", "dbjo-generated");
    private static final Path OUT_PROTO  = OUT_BASE.resolve("proto");
    private static final Path OUT_JAVA   = OUT_BASE.resolve("proto-java");

    // Where generated Java should live
    private static final String JAVA_PKG = "org.github.dbjo.generated.proto";

    // If you want a proto package too (purely informational when java_package is set)
    private static final String PROTO_PKG_BASE = "dbjo"; // e.g. dbjo.<schema>

    public static void main(String[] args) throws Exception {
        String url  = args.length > 0 ? args[0] : DEFAULT_URL;
        String user = args.length > 1 ? args[1] : DEFAULT_USER;
        String pass = args.length > 2 ? args[2] : DEFAULT_PASS;

        Class.forName("org.hsqldb.jdbc.JDBCDriver");

        try (Connection conn = DriverManager.getConnection(url, user, pass)) {
            DatabaseMetaData md = conn.getMetaData();
            System.out.println("Connected:");
            System.out.println("  url     = " + url);
            System.out.println("  db      = " + md.getDatabaseProductName() + " " + md.getDatabaseProductVersion());
            System.out.println();

            List<Path> protoFiles = generateAllProtos(conn);
            System.out.println("Wrote " + protoFiles.size() + " proto file(s) into: " + OUT_PROTO.toAbsolutePath());

            if (!protoFiles.isEmpty()) {
                runProtoc(protoFiles);
                System.out.println("Generated Java into: " + OUT_JAVA.toAbsolutePath());
            }
        }
    }

    private static List<Path> generateAllProtos(Connection conn) throws SQLException, IOException {
        DatabaseMetaData meta = conn.getMetaData();

        List<TableRef> tables = listUserTables(meta);
        if (tables.isEmpty()) {
            System.out.println("No user tables found (non-system schemas).");
            return List.of();
        }

        Files.createDirectories(OUT_PROTO);
        Files.createDirectories(OUT_JAVA);

        List<Path> out = new ArrayList<>();

        for (TableRef t : tables) {
            List<Col> cols = listColumns(meta, t.schema, t.table);
            if (cols.isEmpty()) continue;

            Set<String> pkColsUpper = getPrimaryKeyColumns(meta, t.schema, t.table);

            String msgName = toClassName(t.table);
            String fileName = toProtoFileName(t.schema, t.table);
            Path protoFile = OUT_PROTO.resolve(fileName);

            String protoSrc = renderProtoFile(t, msgName, cols, pkColsUpper);

            write(protoFile, protoSrc);
            out.add(protoFile);

            System.out.println("Wrote: " + protoFile);
        }

        return out;
    }

    // -------------------- protoc invocation --------------------

    private static void runProtoc(List<Path> protoFiles) throws IOException, InterruptedException {
        Path protoc = resolveProtocPath();
        ensureExecutable(protoc);

        List<String> cmd = new ArrayList<>();
        cmd.add(protoc.toAbsolutePath().toString());

        // Your generated .proto dir
        cmd.add("-I" + OUT_PROTO.toAbsolutePath());

        // Optional: include dir for google/protobuf/*.proto (wrappers, timestamp, etc.)
        Path includeDir = resolveProtocIncludeDir();
        if (Files.isDirectory(includeDir)) {
            cmd.add("-I" + includeDir.toAbsolutePath());
        }

        // Java output
        cmd.add("--java_out=" + OUT_JAVA.toAbsolutePath());

        // Compile all protos in one run
        for (Path p : protoFiles) cmd.add(p.toAbsolutePath().toString());

        System.out.println("\nRunning: " + String.join(" ", cmd) + "\n");

        Process proc = new ProcessBuilder(cmd)
                .inheritIO()
                .start();

        int code = proc.waitFor();
        if (code != 0) throw new RuntimeException("protoc failed with exit code " + code);
    }

    private static Path resolveProtocPath() {
        String p = System.getProperty("protoc");
        if (p != null && !p.isBlank()) return Paths.get(p);
        boolean win = System.getProperty("os.name").toLowerCase(Locale.ROOT).contains("win");
        return Paths.get("target", "tools", "protoc", win ? "protoc.exe" : "protoc");
    }

    private static Path resolveProtocIncludeDir() {
        String p = System.getProperty("protoc.include");
        if (p != null && !p.isBlank()) return Paths.get(p);
        return Paths.get("target", "tools", "protoc", "include");
    }

    private static void ensureExecutable(Path protoc) throws IOException {
        if (!Files.exists(protoc)) {
            throw new IOException("protoc not found: " + protoc.toAbsolutePath());
        }
        if (!Files.isExecutable(protoc)) {
            // Works on most OSes; on Windows it's irrelevant
            boolean ok = protoc.toFile().setExecutable(true);
            // If still not executable on a POSIX FS, try chmod-style perms
            if (!Files.isExecutable(protoc)) {
                try {
                    Set<PosixFilePermission> perms = EnumSet.of(
                            PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE, PosixFilePermission.OWNER_EXECUTE,
                            PosixFilePermission.GROUP_READ, PosixFilePermission.GROUP_EXECUTE,
                            PosixFilePermission.OTHERS_READ, PosixFilePermission.OTHERS_EXECUTE
                    );
                    Files.setPosixFilePermissions(protoc, perms);
                } catch (UnsupportedOperationException ignored) {
                    // non-POSIX FS
                }
            }
            if (!Files.isExecutable(protoc) && !ok) {
                throw new IOException("protoc exists but is not executable: " + protoc.toAbsolutePath());
            }
        }
    }

    // -------------------- .proto rendering --------------------

    private static String renderProtoFile(
            TableRef table,
            String messageName,
            List<Col> cols,
            Set<String> pkColsUpper
    ) {
        // Decide imports based on types used
        boolean needWrappers = false;
        boolean needTimestamp = false;

        List<ProtoField> fields = new ArrayList<>();
        for (Col c : cols) {
            boolean nullable = c.nullable != DatabaseMetaData.columnNoNulls; // includes unknown => treat as nullable
            boolean isPk = pkColsUpper.contains(c.colName.toUpperCase(Locale.ROOT));

            ProtoType pt = mapSqlTypeToProto(c.sqlType, nullable);

            if (pt.needsWrappers) needWrappers = true;
            if (pt.needsTimestamp) needTimestamp = true;

            // Use proto lower_snake field name (conventional)
            String fieldName = toLowerSnake(sanitizeProtoIdentifier(toFieldName(c.colName)));

            // Field number: deterministic based on ordinal position
            int fieldNumber = Math.max(1, c.pos);

            fields.add(new ProtoField(fieldName, pt.protoType, fieldNumber, c, isPk));
        }

        String protoPkg = PROTO_PKG_BASE + "." + toLowerSnake(table.schema);

        StringBuilder sb = new StringBuilder(12_000);
        sb.append("syntax = \"proto3\";\n\n");
        sb.append("package ").append(protoPkg).append(";\n\n");

        // Java options
        sb.append("option java_package = \"").append(JAVA_PKG).append("\";\n");
        sb.append("option java_multiple_files = true;\n");
        sb.append("option java_outer_classname = \"").append(messageName).append("Proto\";\n\n");

        if (needWrappers) sb.append("import \"google/protobuf/wrappers.proto\";\n");
        if (needTimestamp) sb.append("import \"google/protobuf/timestamp.proto\";\n");
        if (needWrappers || needTimestamp) sb.append("\n");

        sb.append("// DB: ").append(table.schema).append(".").append(table.table).append("\n");
        sb.append("message ").append(messageName).append(" {\n");

        for (ProtoField f : fields) {
            sb.append("  // DB: ").append(f.col.typeName);
            if (f.isPk) sb.append(" (PK)");
            if ("YES".equalsIgnoreCase(f.col.isAutoIncrement)) sb.append(" (AI)");
            sb.append("\n");

            sb.append("  ").append(f.protoType).append(" ").append(f.name)
                    .append(" = ").append(f.number).append(";\n\n");
        }

        sb.append("}\n");
        return sb.toString();
    }

    // -------------------- Type mapping --------------------

    private static ProtoType mapSqlTypeToProto(int sqlType, boolean nullable) {
        // If nullable -> wrapper type (preserve null vs default).
        // If not nullable -> primitive (or string/bytes) where possible.
        return switch (sqlType) {
            case Types.TINYINT, Types.SMALLINT, Types.INTEGER -> nullable
                    ? ProtoType.w("google.protobuf.Int32Value", true, false)
                    : ProtoType.p("int32");

            case Types.BIGINT -> nullable
                    ? ProtoType.w("google.protobuf.Int64Value", true, false)
                    : ProtoType.p("int64");

            case Types.FLOAT, Types.REAL -> nullable
                    ? ProtoType.w("google.protobuf.FloatValue", true, false)
                    : ProtoType.p("float");

            case Types.DOUBLE -> nullable
                    ? ProtoType.w("google.protobuf.DoubleValue", true, false)
                    : ProtoType.p("double");

            case Types.DECIMAL, Types.NUMERIC ->
                    // No built-in decimal; safest interoperable representation:
                    // - string (e.g., "123.45") OR
                    // - bytes (scaled int) if you want strictness.
                    // Keep it string, preserve null via wrapper.
                    nullable
                        ? ProtoType.w("google.protobuf.StringValue", true, false)
                        : ProtoType.p("string");

            case Types.BIT, Types.BOOLEAN -> nullable
                    ? ProtoType.w("google.protobuf.BoolValue", true, false)
                    : ProtoType.p("bool");

            case Types.CHAR, Types.VARCHAR, Types.LONGVARCHAR,
                 Types.NCHAR, Types.NVARCHAR, Types.LONGNVARCHAR -> nullable
                    ? ProtoType.w("google.protobuf.StringValue", true, false)
                    : ProtoType.p("string");

            case Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY, Types.BLOB -> nullable
                    ? ProtoType.w("google.protobuf.BytesValue", true, false)
                    : ProtoType.p("bytes");

            case Types.TIMESTAMP, Types.TIMESTAMP_WITH_TIMEZONE ->
                    // Timestamp supports presence in proto3 (message type => nullable by nature),
                    // so we don't need wrappers for it.
                    ProtoType.ts("google.protobuf.Timestamp");

            case Types.DATE, Types.TIME, Types.TIME_WITH_TIMEZONE ->
                    // Keep portable: ISO string (e.g. "2026-01-06", "13:45:10.123")
                    // If you want stricter types later, we can move to google.type.Date/TimeOfDay.
                    nullable
                        ? ProtoType.w("google.protobuf.StringValue", true, false)
                        : ProtoType.p("string");

            default ->
                    // fallback: store as string
                    nullable
                        ? ProtoType.w("google.protobuf.StringValue", true, false)
                        : ProtoType.p("string");
        };
    }

    private record ProtoType(String protoType, boolean needsWrappers, boolean needsTimestamp) {
        static ProtoType p(String t) { return new ProtoType(t, false, false); }
        static ProtoType w(String t, boolean wrappers, boolean ts) { return new ProtoType(t, wrappers, ts); }
        static ProtoType ts(String t) { return new ProtoType(t, false, true); }
    }

    // -------------------- DB Introspection (same as your original) --------------------

    private static List<TableRef> listUserTables(DatabaseMetaData meta) throws SQLException {
        List<TableRef> out = new ArrayList<>();
        try (ResultSet rs = meta.getTables(null, null, "%", new String[]{"TABLE"})) {
            while (rs.next()) {
                String schema = rs.getString("TABLE_SCHEM");
                String table  = rs.getString("TABLE_NAME");
                if (schema == null || table == null) continue;
                if (isSystemSchema(schema)) continue;
                out.add(new TableRef(schema, table));
            }
        }
        out.sort(Comparator
                .comparing((TableRef t) -> t.schema.toUpperCase(Locale.ROOT))
                .thenComparing(t -> t.table.toUpperCase(Locale.ROOT)));
        return out;
    }

    private static List<Col> listColumns(DatabaseMetaData meta, String schema, String table) throws SQLException {
        List<Col> cols = new ArrayList<>();
        try (ResultSet crs = meta.getColumns(null, schema, table, "%")) {
            while (crs.next()) {
                cols.add(new Col(
                        crs.getInt("ORDINAL_POSITION"),
                        crs.getString("COLUMN_NAME"),
                        crs.getInt("DATA_TYPE"),    // java.sql.Types
                        crs.getString("TYPE_NAME"), // DB type name
                        crs.getInt("COLUMN_SIZE"),
                        crs.getInt("DECIMAL_DIGITS"),
                        crs.getInt("NULLABLE"),
                        safeGet(crs, "IS_AUTOINCREMENT"),
                        safeGet(crs, "COLUMN_DEF")
                ));
            }
        }
        cols.sort(Comparator.comparingInt(c -> c.pos));
        return cols;
    }

    private static Set<String> getPrimaryKeyColumns(DatabaseMetaData meta, String schema, String table) throws SQLException {
        Set<String> pk = new HashSet<>();
        try (ResultSet rs = meta.getPrimaryKeys(null, schema, table)) {
            while (rs.next()) {
                String col = rs.getString("COLUMN_NAME");
                if (col != null) pk.add(col.toUpperCase(Locale.ROOT));
            }
        }
        return pk;
    }

    private static boolean isSystemSchema(String schema) {
        String s = schema.toUpperCase(Locale.ROOT);
        return s.equals("INFORMATION_SCHEMA") || s.startsWith("SYSTEM") || s.startsWith("SYS");
    }

    private static String safeGet(ResultSet rs, String col) {
        try { return rs.getString(col); }
        catch (SQLException ignored) { return null; }
    }

    // -------------------- Naming helpers --------------------

    private static String toProtoFileName(String schema, String table) {
        return toLowerSnake(schema) + "_" + toLowerSnake(table) + ".proto";
    }

    private static String toClassName(String tableName) {
        String camel = toCamelCase(tableName, true);
        if (!camel.isEmpty() && Character.isDigit(camel.charAt(0))) camel = "_" + camel;
        return camel.isEmpty() ? "Table" : camel;
    }

    private static String toFieldName(String columnName) {
        String camel = toCamelCase(columnName, false);
        if (!camel.isEmpty() && Character.isDigit(camel.charAt(0))) camel = "_" + camel;
        return camel.isEmpty() ? "field" : camel;
    }

    private static String toCamelCase(String s, boolean capFirst) {
        String[] parts = s == null ? new String[0] : s.split("[^A-Za-z0-9]+");
        StringBuilder out = new StringBuilder();
        for (String p : parts) {
            if (p.isEmpty()) continue;
            String lower = p.toLowerCase(Locale.ROOT);
            out.append(Character.toUpperCase(lower.charAt(0))).append(lower.substring(1));
        }
        if (!capFirst && out.length() > 0) out.setCharAt(0, Character.toLowerCase(out.charAt(0)));
        return out.toString();
    }

    private static String toLowerSnake(String s) {
        if (s == null || s.isEmpty()) return s;
        String camel = toCamelCase(s, true);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < camel.length(); i++) {
            char ch = camel.charAt(i);
            if (Character.isUpperCase(ch) && i > 0) sb.append('_');
            sb.append(Character.toLowerCase(ch));
        }
        return sb.toString().replaceAll("[^a-z0-9_]+", "_");
    }

    private static String sanitizeProtoIdentifier(String name) {
        if (name == null || name.isBlank()) return "field";
        // proto identifiers: [A-Za-z_][A-Za-z0-9_]*
        String n = name.replaceAll("[^A-Za-z0-9_]", "_");
        if (Character.isDigit(n.charAt(0))) n = "_" + n;
        // avoid a few obvious keywords (not exhaustive)
        return switch (n) {
            case "package", "syntax", "import", "message", "enum", "service", "rpc",
                 "option", "returns", "reserved" -> n + "_";
            default -> n;
        };
    }

    private static void write(Path file, String content) throws IOException {
        Files.createDirectories(file.getParent());
        Files.writeString(file, content, StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
    }

    // -------------------- Small records --------------------

    private record TableRef(String schema, String table) {}

    private record Col(
            int pos,
            String colName,
            int sqlType,
            String typeName,
            int size,
            int scale,
            int nullable,
            String isAutoIncrement,
            String defaultValue
    ) {}

    private record ProtoField(String name, String protoType, int number, Col col, boolean isPk) {}
}
