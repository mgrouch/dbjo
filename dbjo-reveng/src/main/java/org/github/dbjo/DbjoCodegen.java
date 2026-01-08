package org.github.dbjo;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermission;
import java.sql.*;
import java.util.*;
import java.util.regex.Pattern;

/**
 * One runner for:
 *  - Proto generation per table + protoc Java output
 *  - Bean + Meta generation per table
 *
 * Config via:
 *  - CLI: --key=value  (or --key value)
 *  - System props: db.url, db.user, db.pass, db.driver, protoc, protoc.include
 *
 * Default run mode: all
 */
public final class DbjoCodegen {

    // -------------------- defaults --------------------

    private static final String DEFAULT_URL    = "jdbc:hsqldb:hsql://localhost:9001/dbjo";
    private static final String DEFAULT_USER   = "SA";
    private static final String DEFAULT_PASS   = "";
    private static final String DEFAULT_DRIVER = "org.hsqldb.jdbc.JDBCDriver";

    private static final Path   DEFAULT_OUT_BASE = Paths.get("target", "dbjo-generated");

    // entity defaults
    private static final String DEFAULT_BEAN_PKG = "org.github.dbjo.generated";
    private static final String DEFAULT_META_PKG = "org.github.dbjo.meta.entity";
    private static final String DEFAULT_BASE_META_PKG = "org.github.dbjo.meta.entity"; // where EntityMeta/PropertyMeta live

    // proto defaults
    private static final String DEFAULT_PROTO_JAVA_PKG  = "org.github.dbjo.generated.proto";
    private static final String DEFAULT_PROTO_PKG_BASE  = "dbjo";
    private static final String DEFAULT_PROTO_OUTER_SUFFIX = "Proto";
    private static final boolean DEFAULT_PROTO_PER_TABLE = true;

    private DbjoCodegen() {}

    // -------------------- main --------------------

    public static void main(String[] args) throws Exception {
        ArgMap am = ArgMap.parse(args);

        Config cfg = Config.from(am);

        System.out.println("DBJO codegen");
        System.out.println("  run        = " + cfg.runMode);
        System.out.println("  url        = " + cfg.url);
        System.out.println("  user       = " + cfg.user);
        System.out.println("  outBase    = " + cfg.outBase.toAbsolutePath());
        System.out.println("  overwrite  = " + cfg.overwrite);
        if (cfg.schemaInclude != null) System.out.println("  schemaInc  = " + cfg.schemaInclude);
        if (cfg.schemaExclude != null) System.out.println("  schemaExc  = " + cfg.schemaExclude);
        if (cfg.tableInclude  != null) System.out.println("  tableInc   = " + cfg.tableInclude);
        if (cfg.tableExclude  != null) System.out.println("  tableExc   = " + cfg.tableExclude);
        System.out.println();

        Class.forName(cfg.driver);

        try (Connection conn = DriverManager.getConnection(cfg.url, cfg.user, cfg.pass)) {
            DatabaseMetaData md = conn.getMetaData();
            System.out.println("Connected:");
            System.out.println("  db = " + md.getDatabaseProductName() + " " + md.getDatabaseProductVersion());
            System.out.println();

            DbIntrospector di = new DbIntrospector(cfg);
            List<TableModel> tables = di.loadTables(conn.getMetaData());

            if (tables.isEmpty()) {
                System.out.println("No user tables found after filtering.");
                return;
            }

            System.out.println("Tables: " + tables.size());
            System.out.println();

            if (cfg.runMode.runProto()) {
                ProtoGenerator pg = new ProtoGenerator(cfg);
                List<Path> protos = pg.generateAll(tables);

                System.out.println("Wrote " + protos.size() + " proto file(s) into: " + cfg.protoOutProto.toAbsolutePath());

                if (!protos.isEmpty() && cfg.protoRunProtoc) {
                    ProtocInvoker pi = new ProtocInvoker(cfg);
                    pi.runProtoc(protos);
                    System.out.println("Generated Java into: " + cfg.protoOutJava.toAbsolutePath());
                }
                System.out.println();
            }

            if (cfg.runMode.runEntity()) {
                EntityGenerator eg = new EntityGenerator(cfg);
                int n = eg.generateAll(tables);
                System.out.println("Generated entity/meta for " + n + " table(s) into: " + cfg.outBase.toAbsolutePath());
                System.out.println();
            }
        }
    }

    // -------------------- config --------------------

    enum RunMode {
        ALL, PROTO, ENTITY;

        boolean runProto()  { return this == ALL || this == PROTO; }
        boolean runEntity() { return this == ALL || this == ENTITY; }

        static RunMode parse(String s) {
            if (s == null) return ALL;
            return switch (s.trim().toLowerCase(Locale.ROOT)) {
                case "all", "both" -> ALL;
                case "proto" -> PROTO;
                case "entity", "entities" -> ENTITY;
                default -> throw new IllegalArgumentException("Unknown --run=" + s + " (use all|proto|entity)");
            };
        }
    }

    static final class Config {
        final String driver;
        final String url;
        final String user;
        final String pass;

        final Path outBase;
        final boolean overwrite;

        // filters
        final Pattern schemaInclude;
        final Pattern schemaExclude;
        final Pattern tableInclude;
        final Pattern tableExclude;

        final RunMode runMode;

        // proto
        final Path protoOutProto;
        final Path protoOutJava;
        final String protoJavaPkg;
        final String protoPkgBase;
        final String protoOuterSuffix;
        final boolean protoPerTable;
        final boolean protoRunProtoc;
        final boolean protoAddExperimentalOptionalFlag; // for older protoc only (off by default)

        // protoc
        final Path protocPath;
        final Path protocInclude;

        // entity
        final String beanPkg;
        final String metaPkg;
        final String baseMetaPkg;

        private Config(
                String driver, String url, String user, String pass,
                Path outBase, boolean overwrite,
                Pattern schemaInclude, Pattern schemaExclude,
                Pattern tableInclude, Pattern tableExclude,
                RunMode runMode,
                Path protoOutProto, Path protoOutJava,
                String protoJavaPkg, String protoPkgBase, String protoOuterSuffix,
                boolean protoPerTable, boolean protoRunProtoc, boolean protoAddExperimentalOptionalFlag,
                Path protocPath, Path protocInclude,
                String beanPkg, String metaPkg, String baseMetaPkg
        ) {
            this.driver = driver;
            this.url = url;
            this.user = user;
            this.pass = pass;
            this.outBase = outBase;
            this.overwrite = overwrite;
            this.schemaInclude = schemaInclude;
            this.schemaExclude = schemaExclude;
            this.tableInclude = tableInclude;
            this.tableExclude = tableExclude;
            this.runMode = runMode;

            this.protoOutProto = protoOutProto;
            this.protoOutJava = protoOutJava;
            this.protoJavaPkg = protoJavaPkg;
            this.protoPkgBase = protoPkgBase;
            this.protoOuterSuffix = protoOuterSuffix;
            this.protoPerTable = protoPerTable;
            this.protoRunProtoc = protoRunProtoc;
            this.protoAddExperimentalOptionalFlag = protoAddExperimentalOptionalFlag;

            this.protocPath = protocPath;
            this.protocInclude = protocInclude;

            this.beanPkg = beanPkg;
            this.metaPkg = metaPkg;
            this.baseMetaPkg = baseMetaPkg;
        }

        static Config from(ArgMap am) {
            String driver = am.get("driver", System.getProperty("db.driver", DEFAULT_DRIVER));
            String url    = am.get("url",    System.getProperty("db.url",    DEFAULT_URL));
            String user   = am.get("user",   System.getProperty("db.user",   DEFAULT_USER));
            String pass   = am.get("pass",   System.getProperty("db.pass",   DEFAULT_PASS));

            Path outBase = Paths.get(am.get("outBase", DEFAULT_OUT_BASE.toString()));
            boolean overwrite = am.getBool("overwrite", false);

            Pattern schemaInc = am.getRegex("schemaInclude", null);
            Pattern schemaExc = am.getRegex("schemaExclude", null);
            Pattern tableInc  = am.getRegex("tableInclude", null);
            Pattern tableExc  = am.getRegex("tableExclude", null);

            RunMode run = RunMode.parse(am.get("run", "all"));

            // proto outputs under outBase by default
            Path protoOutProto = Paths.get(am.get("protoOutProto", outBase.resolve("proto").toString()));
            Path protoOutJava  = Paths.get(am.get("protoOutJava",  outBase.resolve("proto-java").toString()));

            String protoJavaPkg = am.get("protoJavaPkg", DEFAULT_PROTO_JAVA_PKG);
            String protoPkgBase = am.get("protoPkgBase", DEFAULT_PROTO_PKG_BASE);
            String protoOuterSuffix = am.get("protoOuterSuffix", DEFAULT_PROTO_OUTER_SUFFIX);
            boolean protoPerTable = am.getBool("protoPerTable", DEFAULT_PROTO_PER_TABLE);
            boolean protoRunProtoc = am.getBool("protoRunProtoc", true);
            boolean protoAddExperimentalOptionalFlag = am.getBool("protoExperimentalOptional", false);

            // protoc path + include
            Path protocPath = resolveProtocPath(am);
            Path protocInclude = resolveProtocIncludeDir(am);

            String beanPkg = am.get("beanPkg", DEFAULT_BEAN_PKG);
            String metaPkg = am.get("metaPkg", DEFAULT_META_PKG);
            String baseMetaPkg = am.get("baseMetaPkg", DEFAULT_BASE_META_PKG);

            return new Config(
                    driver, url, user, pass,
                    outBase, overwrite,
                    schemaInc, schemaExc, tableInc, tableExc,
                    run,
                    protoOutProto, protoOutJava,
                    protoJavaPkg, protoPkgBase, protoOuterSuffix,
                    protoPerTable, protoRunProtoc, protoAddExperimentalOptionalFlag,
                    protocPath, protocInclude,
                    beanPkg, metaPkg, baseMetaPkg
            );
        }

        private static Path resolveProtocPath(ArgMap am) {
            String p = am.get("protoc", System.getProperty("protoc"));
            if (p != null && !p.isBlank()) return Paths.get(p);

            boolean win = System.getProperty("os.name").toLowerCase(Locale.ROOT).contains("win");
            return Paths.get("target", "tools", "protoc", win ? "protoc.exe" : "protoc");
        }

        private static Path resolveProtocIncludeDir(ArgMap am) {
            String p = am.get("protocInclude", System.getProperty("protoc.include"));
            if (p != null && !p.isBlank()) return Paths.get(p);
            return Paths.get("target", "tools", "protoc", "include");
        }
    }

    // -------------------- db introspection (shared) --------------------

    static final class DbIntrospector {
        private final Config cfg;

        DbIntrospector(Config cfg) { this.cfg = cfg; }

        List<TableModel> loadTables(DatabaseMetaData meta) throws SQLException {
            List<TableRef> tables = listUserTables(meta);
            tables = applyFilters(tables);

            List<TableModel> out = new ArrayList<>(tables.size());
            for (TableRef t : tables) {
                List<Col> cols = listColumns(meta, t.schema, t.table);
                if (cols.isEmpty()) continue;
                Set<String> pk = getPrimaryKeyColumns(meta, t.schema, t.table);
                out.add(new TableModel(t, cols, pk));
            }

            out.sort(Comparator
                    .comparing((TableModel tm) -> tm.table.schema.toUpperCase(Locale.ROOT))
                    .thenComparing(tm -> tm.table.table.toUpperCase(Locale.ROOT)));
            return out;
        }

        private List<TableRef> applyFilters(List<TableRef> in) {
            if (cfg.schemaInclude == null && cfg.schemaExclude == null && cfg.tableInclude == null && cfg.tableExclude == null) {
                return in;
            }
            List<TableRef> out = new ArrayList<>(in.size());
            for (TableRef t : in) {
                String s = t.schema == null ? "" : t.schema;
                String n = t.table == null ? "" : t.table;

                if (cfg.schemaInclude != null && !cfg.schemaInclude.matcher(s).find()) continue;
                if (cfg.schemaExclude != null && cfg.schemaExclude.matcher(s).find()) continue;
                if (cfg.tableInclude  != null && !cfg.tableInclude.matcher(n).find()) continue;
                if (cfg.tableExclude  != null && cfg.tableExclude.matcher(n).find()) continue;

                out.add(t);
            }
            return out;
        }

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
                            crs.getInt("DATA_TYPE"),
                            crs.getString("TYPE_NAME"),
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
    }

    // -------------------- proto generator --------------------

    static final class ProtoGenerator {
        private final Config cfg;

        ProtoGenerator(Config cfg) { this.cfg = cfg; }

        List<Path> generateAll(List<TableModel> tables) throws IOException {
            Files.createDirectories(cfg.protoOutProto);
            Files.createDirectories(cfg.protoOutJava);

            if (!cfg.protoPerTable) {
                // keep this simple for now: per-table is default and recommended
                throw new UnsupportedOperationException("protoPerTable=false not implemented yet");
            }

            List<Path> out = new ArrayList<>();
            for (TableModel tm : tables) {
                String msgName = Naming.toClassName(tm.table.table);
                String fileName = Naming.toLowerSnake(tm.table.schema) + "_" + Naming.toLowerSnake(tm.table.table) + ".proto";
                Path protoFile = cfg.protoOutProto.resolve(fileName);

                String src = renderProtoFile(tm, msgName);
                FilesUtil.writeString(protoFile, src, cfg.overwrite);
                out.add(protoFile);

                System.out.println("Wrote: " + protoFile);
            }
            return out;
        }

        private String renderProtoFile(TableModel tm, String messageName) {
            boolean needTimestamp = false;

            List<ProtoField> fields = new ArrayList<>();
            for (Col c : tm.cols) {
                boolean nullable = c.nullable != DatabaseMetaData.columnNoNulls; // includes unknown => treat as nullable
                boolean isPk = tm.pkColsUpper.contains(c.colName.toUpperCase(Locale.ROOT));

                ProtoType pt = mapSqlTypeToProto(c.sqlType);
                if (pt.needsTimestamp) needTimestamp = true;

                String fieldName = Naming.toLowerSnake(Naming.sanitizeProtoIdentifier(Naming.toFieldName(c.colName)));
                int fieldNumber = Math.max(1, c.pos);

                // optional only for scalars/string/bytes/enums (not message types like Timestamp)
                boolean isOptional = nullable && pt.allowOptional;

                fields.add(new ProtoField(fieldName, pt.protoType, isOptional, fieldNumber, c, isPk));
            }

            String protoPkg = cfg.protoPkgBase + "." + Naming.toLowerSnake(tm.table.schema);

            StringBuilder sb = new StringBuilder(12_000);
            sb.append("syntax = \"proto3\";\n\n");
            sb.append("package ").append(protoPkg).append(";\n\n");

            sb.append("option java_package = \"").append(cfg.protoJavaPkg).append("\";\n");
            sb.append("option java_multiple_files = true;\n");
            sb.append("option java_outer_classname = \"").append(messageName).append(cfg.protoOuterSuffix).append("\";\n\n");

            if (needTimestamp) sb.append("import \"google/protobuf/timestamp.proto\";\n\n");

            sb.append("// DB: ").append(tm.table.schema).append(".").append(tm.table.table).append("\n");
            sb.append("message ").append(messageName).append(" {\n");

            for (ProtoField f : fields) {
                sb.append("  // DB: ").append(f.col.typeName);
                if (f.isPk) sb.append(" (PK)");
                if ("YES".equalsIgnoreCase(f.col.isAutoIncrement)) sb.append(" (AI)");
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
                        ProtoType.scalar("string"); // portable ISO string

                default -> ProtoType.scalar("string");
            };
        }

        private record ProtoType(String protoType, boolean allowOptional, boolean needsTimestamp) {
            static ProtoType scalar(String t) { return new ProtoType(t, true, false); }
            static ProtoType message(String t, boolean ts) { return new ProtoType(t, false, ts); }
        }

        private record ProtoField(String name, String protoType, boolean optional, int number, Col col, boolean isPk) {}
    }

    // -------------------- protoc invoker --------------------

    static final class ProtocInvoker {
        private final Config cfg;

        ProtocInvoker(Config cfg) { this.cfg = cfg; }

        void runProtoc(List<Path> protoFiles) throws IOException, InterruptedException {
            Path protoc = cfg.protocPath;
            ensureExecutable(protoc);

            List<String> cmd = new ArrayList<>();
            cmd.add(protoc.toAbsolutePath().toString());

            if (cfg.protoAddExperimentalOptionalFlag) {
                cmd.add("--experimental_allow_proto3_optional");
            }

            cmd.add("-I" + cfg.protoOutProto.toAbsolutePath());

            if (Files.isDirectory(cfg.protocInclude)) {
                cmd.add("-I" + cfg.protocInclude.toAbsolutePath());
            }

            cmd.add("--java_out=" + cfg.protoOutJava.toAbsolutePath());

            for (Path p : protoFiles) cmd.add(p.toAbsolutePath().toString());

            System.out.println("\nRunning: " + String.join(" ", cmd) + "\n");

            Process proc = new ProcessBuilder(cmd)
                    .inheritIO()
                    .start();

            int code = proc.waitFor();
            if (code != 0) throw new RuntimeException("protoc failed with exit code " + code);
        }

        private static void ensureExecutable(Path protoc) throws IOException {
            if (!Files.exists(protoc)) {
                throw new IOException("protoc not found: " + protoc.toAbsolutePath());
            }
            if (!Files.isExecutable(protoc)) {
                boolean ok = protoc.toFile().setExecutable(true);
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
    }

    // -------------------- entity generator --------------------

    static final class EntityGenerator {
        private final Config cfg;

        EntityGenerator(Config cfg) { this.cfg = cfg; }

        int generateAll(List<TableModel> tables) throws IOException {
            Path beanDir = cfg.outBase.resolve(cfg.beanPkg.replace('.', '/'));
            Path metaDir = cfg.outBase.resolve(cfg.metaPkg.replace('.', '/'));
            Files.createDirectories(beanDir);
            Files.createDirectories(metaDir);

            int count = 0;
            for (TableModel tm : tables) {
                String beanClass = Naming.toClassName(tm.table.table);

                String beanSrc = renderBean(cfg.beanPkg, beanClass, tm);
                Path beanFile = beanDir.resolve(beanClass + ".java");
                FilesUtil.writeString(beanFile, beanSrc, cfg.overwrite);

                String metaClass = beanClass + "Meta";
                String metaSrc = renderMeta(cfg.metaPkg, cfg.baseMetaPkg, cfg.beanPkg, beanClass, metaClass, tm);
                Path metaFile = metaDir.resolve(metaClass + ".java");
                FilesUtil.writeString(metaFile, metaSrc, cfg.overwrite);

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
            for (Col c : tm.cols) {
                JavaType jt = mapSqlTypeToJava(c.sqlType, imports);
                String fieldName = Naming.sanitizeJavaIdentifier(Naming.toFieldName(c.colName));
                boolean isPk = tm.pkColsUpper.contains(c.colName.toUpperCase(Locale.ROOT));
                fields.add(new Field(fieldName, jt.javaType, jt.classLiteral, c, isPk));
            }

            StringBuilder sb = new StringBuilder(10_000);
            sb.append("package ").append(pkg).append(";\n\n");
            for (String imp : imports) sb.append("import ").append(imp).append(";\n");
            sb.append("\n");

            sb.append("/**\n");
            sb.append(" * Auto-generated bean for ").append(tm.table.schema).append(".").append(tm.table.table).append("\n");
            sb.append(" */\n");
            sb.append("public class ").append(className).append(" implements Serializable {\n\n");
            sb.append("  private static final long serialVersionUID = 1L;\n\n");

            for (Field f : fields) {
                sb.append("  /** DB: ").append(f.col.typeName);
                if (f.isPk) sb.append(" (PK)");
                if ("YES".equalsIgnoreCase(f.col.isAutoIncrement)) sb.append(" (AI)");
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

            // Import base meta types explicitly so metaPkg is fully configurable
            imports.add(baseMetaPkg + ".EntityMeta");
            imports.add(baseMetaPkg + ".PropertyMeta");

            List<MetaProp> props = new ArrayList<>();
            for (Col c : tm.cols) {
                String propName = Naming.sanitizeJavaIdentifier(Naming.toFieldName(c.colName));
                JavaType jt = mapSqlTypeToJava(c.sqlType, null); // type + class literal only
                boolean isPk = tm.pkColsUpper.contains(c.colName.toUpperCase(Locale.ROOT));

                String constName = Naming.toUpperSnake(propName);
                props.add(new MetaProp(constName, propName, jt.javaType, jt.classLiteral, c, isPk));
            }

            StringBuilder sb = new StringBuilder(12_000);
            sb.append("package ").append(metaPkg).append(";\n\n");
            for (String imp : imports) sb.append("import ").append(imp).append(";\n");
            sb.append("\n");

            sb.append("/**\n");
            sb.append(" * Auto-generated entity meta for ").append(tm.table.schema).append(".").append(tm.table.table).append("\n");
            sb.append(" */\n");
            sb.append("public final class ").append(metaClass).append(" {\n\n");
            sb.append("  private ").append(metaClass).append("() {}\n\n");

            for (MetaProp p : props) {
                sb.append("  /** DB: ").append(p.col.typeName);
                if (p.isPk) sb.append(" (PK)");
                if ("YES".equalsIgnoreCase(p.col.isAutoIncrement)) sb.append(" (AI)");
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

            sb.append("  public static final EntityMeta<").append(beanClass).append("> META = new EntityMeta<>(\n");
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

                case Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY, Types.BLOB ->
                        new JavaType("byte[]", "byte[].class");

                default -> new JavaType("String", "String.class");
            };
        }

        private record JavaType(String javaType, String classLiteral) {}
        private record Field(String name, String javaType, String classLiteral, Col col, boolean isPk) {}
        private record MetaProp(String constName, String propName, String javaType, String classLiteral, Col col, boolean isPk) {}
    }

    // -------------------- shared model records --------------------

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

    private record TableModel(TableRef table, List<Col> cols, Set<String> pkColsUpper) {}

    // -------------------- shared naming helpers --------------------

    static final class Naming {
        private Naming() {}

        static String toClassName(String tableName) {
            String camel = toCamelCase(tableName, true);
            if (!camel.isEmpty() && Character.isDigit(camel.charAt(0))) camel = "_" + camel;
            return camel.isEmpty() ? "Table" : camel;
        }

        static String toFieldName(String columnName) {
            String camel = toCamelCase(columnName, false);
            if (!camel.isEmpty() && Character.isDigit(camel.charAt(0))) camel = "_" + camel;
            return camel.isEmpty() ? "field" : camel;
        }

        static String toCamelCase(String s, boolean capFirst) {
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

        static String toLowerSnake(String s) {
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

        static String toUpperSnake(String camel) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < camel.length(); i++) {
                char ch = camel.charAt(i);
                if (Character.isUpperCase(ch) && i > 0) sb.append('_');
                sb.append(Character.toUpperCase(ch));
            }
            return sanitizeJavaIdentifier(sb.toString());
        }

        static String capitalize(String s) {
            if (s == null || s.isEmpty()) return s;
            return Character.toUpperCase(s.charAt(0)) + s.substring(1);
        }

        static String sanitizeJavaIdentifier(String name) {
            if (name == null || name.isEmpty()) return name;
            return switch (name) {
                case "class", "public", "private", "protected", "static", "final", "void",
                        "int", "long", "float", "double", "boolean", "byte", "short", "char",
                        "return", "package", "import", "new", "null", "true", "false",
                        "this", "super", "interface", "enum", "extends", "implements",
                        "switch", "case", "default", "break", "continue", "for", "while", "do",
                        "if", "else", "try", "catch", "finally", "throw", "throws", "instanceof" -> name + "_";
                default -> name;
            };
        }

        static String sanitizeProtoIdentifier(String name) {
            if (name == null || name.isBlank()) return "field";
            String n = name.replaceAll("[^A-Za-z0-9_]", "_");
            if (Character.isDigit(n.charAt(0))) n = "_" + n;
            return switch (n) {
                case "package", "syntax", "import", "message", "enum", "service", "rpc",
                        "option", "returns", "reserved" -> n + "_";
                default -> n;
            };
        }
    }

    // -------------------- shared file utils --------------------

    static final class FilesUtil {
        private FilesUtil() {}

        static void writeString(Path file, String content, boolean overwrite) throws IOException {
            Files.createDirectories(file.getParent());
            if (!overwrite && Files.exists(file)) {
                // keep it simple: skip
                System.out.println("Skip (exists): " + file);
                return;
            }
            Files.writeString(file, content, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
        }
    }

    // -------------------- minimal CLI parsing --------------------

    static final class ArgMap {
        private final Map<String, String> map;

        private ArgMap(Map<String, String> map) {
            this.map = map;
        }

        static ArgMap parse(String[] args) {
            Map<String, String> m = new LinkedHashMap<>();
            for (int i = 0; i < args.length; i++) {
                String a = args[i];
                if (!a.startsWith("--")) continue;
                String key;
                String val;

                int eq = a.indexOf('=');
                if (eq >= 0) {
                    key = a.substring(2, eq).trim();
                    val = a.substring(eq + 1).trim();
                } else {
                    key = a.substring(2).trim();
                    if (i + 1 < args.length && !args[i + 1].startsWith("--")) {
                        val = args[++i];
                    } else {
                        val = "true";
                    }
                }
                if (!key.isEmpty()) m.put(key, val);
            }
            return new ArgMap(m);
        }

        String get(String key, String def) {
            String v = map.get(key);
            return (v == null || v.isBlank()) ? def : v;
        }

        boolean getBool(String key, boolean def) {
            String v = map.get(key);
            if (v == null) return def;
            return switch (v.trim().toLowerCase(Locale.ROOT)) {
                case "1", "true", "yes", "y", "on" -> true;
                case "0", "false", "no", "n", "off" -> false;
                default -> def;
            };
        }

        Pattern getRegex(String key, Pattern def) {
            String v = map.get(key);
            if (v == null || v.isBlank()) return def;
            return Pattern.compile(v);
        }
    }
}
