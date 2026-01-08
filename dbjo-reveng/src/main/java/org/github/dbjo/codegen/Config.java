package org.github.dbjo.codegen;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Locale;
import java.util.regex.Pattern;

public record Config(
        // DB
        String driver,
        String url,
        String user,
        String pass,

        // output
        Path outBase,
        boolean overwrite,

        // filters
        Pattern schemaInclude,
        Pattern schemaExclude,
        Pattern tableInclude,
        Pattern tableExclude,

        // run mode
        RunMode runMode,

        // proto
        Path protoOutProto,
        Path protoOutJava,
        String protoJavaPkg,
        String protoPkgBase,
        String protoOuterSuffix,
        boolean protoPerTable,
        boolean protoRunProtoc,
        boolean protoExperimentalOptional,

        // protoc paths
        Path protocPath,
        Path protocInclude,

        // entity/meta
        String beanPkg,
        String metaPkg,
        String baseMetaPkg,
        Path codegenOutJava,

        // RocksDB DAO generator
        String daoPkg,
        String schemaPkg,
        String daoClassSuffix,
        String schemaClassSuffix,
        String cfConstSuffix,
        String daoBaseClass,   // e.g. IndexedRocksDao

        // Protobuf mapper generator
        String protoMapperPkg,
        String protoMapperSuffix
) {
    // ---------------- defaults ----------------
    public static final String DEFAULT_URL    = "jdbc:hsqldb:hsql://localhost:9001/dbjo";
    public static final String DEFAULT_USER   = "SA";
    public static final String DEFAULT_PASS   = "";
    public static final String DEFAULT_DRIVER = "org.hsqldb.jdbc.JDBCDriver";

    public static final Path   DEFAULT_OUT_BASE = Paths.get("target", "generated-sources");

    public static final String DEFAULT_BEAN_PKG = "org.github.dbjo.generated.model.entity";
    public static final String DEFAULT_META_PKG = "org.github.dbjo.generated.model.meta";
    public static final String DEFAULT_BASE_META_PKG = "org.github.dbjo.meta.entity";

    public static final String DEFAULT_PROTO_JAVA_PKG  = "org.github.dbjo.generated.proto";
    public static final String DEFAULT_PROTO_PKG_BASE  = "dbjo";
    public static final String DEFAULT_PROTO_OUTER_SUFFIX = "Proto";
    public static final boolean DEFAULT_PROTO_PER_TABLE = true;

    public static final String DEFAULT_DAO_PKG = "org.github.dbjo.generated.rdb.dao";
    public static final String DEFAULT_SCHEMA_PKG = DEFAULT_DAO_PKG;
    public static final String DEFAULT_DAO_CLASS_SUFFIX = "Dao";
    public static final String DEFAULT_SCHEMA_CLASS_SUFFIX = "Schema";
    public static final String DEFAULT_CF_CONST_SUFFIX = "_CF";
    public static final String DEFAULT_DAO_BASE_CLASS = "IndexedRocksDao";

    public static final String DEFAULT_PROTO_MAPPER_PKG = "org.github.dbjo.generated.rdb.mapper";
    public static final String DEFAULT_PROTO_MAPPER_SUFFIX = "ProtoMapper";

    public enum RunMode {
        ALL, PROTO, ENTITY, DAO, MAPPER, RDB;

        public boolean runProto()   { return this == ALL || this == PROTO; }
        public boolean runEntity()  { return this == ALL || this == ENTITY; }
        public boolean runDao()     { return this == ALL || this == DAO || this == RDB; }
        public boolean runMapper()  { return this == ALL || this == MAPPER || this == RDB; }

        public static RunMode parse(String s) {
            if (s == null) return ALL;
            return switch (s.trim().toLowerCase(Locale.ROOT)) {
                case "all", "both" -> ALL;
                case "proto" -> PROTO;
                case "entity", "entities" -> ENTITY;
                case "dao", "daos" -> DAO;
                case "mapper", "mappers" -> MAPPER;
                case "rdb", "rocks", "rocksdb" -> RDB;
                default -> throw new IllegalArgumentException("Unknown --run=" + s + " (use all|proto|entity|dao|mapper|rdb)");
            };
        }
    }

    public static Config from(ArgMap am) {
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

        RunMode runMode = RunMode.parse(am.get("run", "all"));

        Path protoOutProto = Paths.get(am.get("protoOutProto", outBase.resolve("proto").toString()));
        Path protoOutJava  = Paths.get(am.get("protoOutJava",  outBase.resolve("proto-java").toString()));

        String protoJavaPkg = am.get("protoJavaPkg", DEFAULT_PROTO_JAVA_PKG);
        String protoPkgBase = am.get("protoPkgBase", DEFAULT_PROTO_PKG_BASE);
        String protoOuterSuffix = am.get("protoOuterSuffix", DEFAULT_PROTO_OUTER_SUFFIX);
        boolean protoPerTable = am.getBool("protoPerTable", DEFAULT_PROTO_PER_TABLE);
        boolean protoRunProtoc = am.getBool("protoRunProtoc", true);
        boolean protoExperimentalOptional = am.getBool("protoExperimentalOptional", false);

        Path protocPath = resolveProtocPath(am);
        Path protocInclude = resolveProtocIncludeDir(am);

        Path codegenOutJava  = Paths.get(am.get("codegenOutJava",  outBase.resolve("codegen-java").toString()));

        String beanPkg = am.get("beanPkg", DEFAULT_BEAN_PKG);
        String metaPkg = am.get("metaPkg", DEFAULT_META_PKG);
        String baseMetaPkg = am.get("baseMetaPkg", DEFAULT_BASE_META_PKG);

        String daoPkg = am.get("daoPkg", DEFAULT_DAO_PKG);
        String schemaPkg = am.get("schemaPkg", DEFAULT_SCHEMA_PKG);
        String daoClassSuffix = am.get("daoClassSuffix", DEFAULT_DAO_CLASS_SUFFIX);
        String schemaClassSuffix = am.get("schemaClassSuffix", DEFAULT_SCHEMA_CLASS_SUFFIX);
        String cfConstSuffix = am.get("cfConstSuffix", DEFAULT_CF_CONST_SUFFIX);
        String daoBaseClass = am.get("daoBaseClass", DEFAULT_DAO_BASE_CLASS);

        String protoMapperPkg = am.get("protoMapperPkg", DEFAULT_PROTO_MAPPER_PKG);
        String protoMapperSuffix = am.get("protoMapperSuffix", DEFAULT_PROTO_MAPPER_SUFFIX);

        return new Config(
                driver, url, user, pass,
                outBase, overwrite,
                schemaInc, schemaExc, tableInc, tableExc,
                runMode,
                protoOutProto, protoOutJava,
                protoJavaPkg, protoPkgBase, protoOuterSuffix,
                protoPerTable, protoRunProtoc, protoExperimentalOptional,
                protocPath, protocInclude,
                beanPkg, metaPkg, baseMetaPkg, codegenOutJava,
                daoPkg, schemaPkg, daoClassSuffix, schemaClassSuffix, cfConstSuffix, daoBaseClass,
                protoMapperPkg, protoMapperSuffix
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
