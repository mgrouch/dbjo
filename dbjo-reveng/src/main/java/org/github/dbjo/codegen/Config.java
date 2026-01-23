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
        String metaSuffix,          // <-- NEW (e.g. "Meta")
        Path codegenOutJava,

        // query terms
        String queryPkg,            // <-- NEW (where <Entity>Q goes)
        String querySuffix,         // <-- NEW (e.g. "Q")
        String termsFqn,            // <-- NEW (FQN of Terms class)
        String propertyTermFqn,     // <-- NEW (FQN of PropertyTerm)

        // RocksDB DAO generator
        String daoPkg,
        String schemaPkg,
        String daoClassSuffix,
        String schemaClassSuffix,
        String cfConstSuffix,
        String daoBaseClass,

        // Protobuf mapper generator
        String protoMapperPkg,
        String protoMapperSuffix,

        // SQL DB mappings
        String dbMetaPkg,

        // DB enums
        String enumPkg,
        boolean enumEnabled,
        boolean enumIncludeViews,
        boolean enumOrderBySortOrderIfPresent
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
    public static final String DEFAULT_META_SUFFIX = "Meta";

    public static final String DEFAULT_QUERY_PKG = "org.github.dbjo.generated.model.query";
    public static final String DEFAULT_QUERY_SUFFIX = "Q";

    // Adjust these to your actual query API package/classes:
    public static final String DEFAULT_TERMS_FQN = "org.github.dbjo.criteria.Terms";
    public static final String DEFAULT_PROPERTY_TERM_FQN = "org.github.dbjo.criteria.PropertyTerm";

    public static final String DEFAULT_PROTO_JAVA_PKG  = "org.github.dbjo.generated.proto";
    public static final String DEFAULT_PROTO_PKG_BASE  = "dbjo";
    public static final String DEFAULT_PROTO_OUTER_SUFFIX = "Proto";
    public static final boolean DEFAULT_PROTO_PER_TABLE = true;

    public static final String DEFAULT_DAO_PKG = "org.github.dbjo.generated.rdb.dao";
    public static final String DEFAULT_SCHEMA_PKG = "org.github.dbjo.generated.rdb.schema";
    public static final String DEFAULT_DAO_CLASS_SUFFIX = "Dao";
    public static final String DEFAULT_SCHEMA_CLASS_SUFFIX = "Schema";
    public static final String DEFAULT_CF_CONST_SUFFIX = "_CF";
    public static final String DEFAULT_DAO_BASE_CLASS = "IndexedRocksDao";

    public static final String DEFAULT_PROTO_MAPPER_PKG = "org.github.dbjo.generated.rdb.mapper";
    public static final String DEFAULT_PROTO_MAPPER_SUFFIX = "ProtoMapper";

    private static final String DEFAULT_SQL_DB_MAPPER_PKG = "org.github.dbjo.generated.db.meta";

    public enum RunMode {
        ALL, PROTO, ENTITY, DAO, MAPPER, RDB, QUERY;

        public boolean runProto()   { return this == ALL || this == PROTO; }
        public boolean runEntity()  { return this == ALL || this == ENTITY; }
        public boolean runDao()     { return this == ALL || this == DAO || this == RDB; }
        public boolean runMapper()  { return this == ALL || this == MAPPER || this == RDB; }
        public boolean runQuery()   { return this == ALL || this == QUERY || this == ENTITY; } // often tied to meta

        public static RunMode parse(String s) {
            if (s == null) return ALL;
            return switch (s.trim().toLowerCase(Locale.ROOT)) {
                case "all", "both" -> ALL;
                case "proto" -> PROTO;
                case "entity", "entities" -> ENTITY;
                case "dao", "daos" -> DAO;
                case "mapper", "mappers" -> MAPPER;
                case "rdb", "rocks", "rocksdb" -> RDB;
                case "query", "q" -> QUERY;
                default -> throw new IllegalArgumentException("Unknown --run=" + s + " (use all|proto|entity|dao|mapper|rdb|query)");
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

        Path codegenOutJava  = Paths.get(am.get("codegenOutJava", outBase.resolve("codegen-java").toString()));

        String beanPkg = am.get("beanPkg", DEFAULT_BEAN_PKG);
        String metaPkg = am.get("metaPkg", DEFAULT_META_PKG);
        String baseMetaPkg = am.get("baseMetaPkg", DEFAULT_BASE_META_PKG);
        String metaSuffix = am.get("metaSuffix", DEFAULT_META_SUFFIX);

        String queryPkg = am.get("queryPkg", DEFAULT_QUERY_PKG);
        String querySuffix = am.get("querySuffix", DEFAULT_QUERY_SUFFIX);
        String termsFqn = am.get("termsFqn", DEFAULT_TERMS_FQN);
        String propertyTermFqn = am.get("propertyTermFqn", DEFAULT_PROPERTY_TERM_FQN);

        String daoPkg = am.get("daoPkg", DEFAULT_DAO_PKG);
        String schemaPkg = am.get("schemaPkg", DEFAULT_SCHEMA_PKG);
        String daoClassSuffix = am.get("daoClassSuffix", DEFAULT_DAO_CLASS_SUFFIX);
        String schemaClassSuffix = am.get("schemaClassSuffix", DEFAULT_SCHEMA_CLASS_SUFFIX);
        String cfConstSuffix = am.get("cfConstSuffix", DEFAULT_CF_CONST_SUFFIX);
        String daoBaseClass = am.get("daoBaseClass", DEFAULT_DAO_BASE_CLASS);

        String protoMapperPkg = am.get("protoMapperPkg", DEFAULT_PROTO_MAPPER_PKG);
        String protoMapperSuffix = am.get("protoMapperSuffix", DEFAULT_PROTO_MAPPER_SUFFIX);

        String dbMetaPkg = am.get("dbMetaPkg", DEFAULT_SQL_DB_MAPPER_PKG);

        String enumPkgDefault = beanPkg.endsWith(".entity")
                ? beanPkg.substring(0, beanPkg.length() - ".entity".length()) + ".enum"
                : (beanPkg + ".enum");

        String enumPkg = am.get("enumPkg", enumPkgDefault);
        boolean enumEnabled = am.getBool("enumEnabled", true);
        boolean enumIncludeViews = am.getBool("enumIncludeViews", false);
        boolean enumOrderBySortOrderIfPresent = am.getBool("enumOrderBySortOrderIfPresent", true);

        return new Config(
                driver, url, user, pass,
                outBase, overwrite,
                schemaInc, schemaExc, tableInc, tableExc,
                runMode,
                protoOutProto, protoOutJava,
                protoJavaPkg, protoPkgBase, protoOuterSuffix,
                protoPerTable, protoRunProtoc, protoExperimentalOptional,
                protocPath, protocInclude,
                beanPkg, metaPkg, baseMetaPkg, metaSuffix, codegenOutJava,
                queryPkg, querySuffix, termsFqn, propertyTermFqn,
                daoPkg, schemaPkg, daoClassSuffix, schemaClassSuffix, cfConstSuffix, daoBaseClass,
                protoMapperPkg, protoMapperSuffix, dbMetaPkg,
                enumPkg, enumEnabled, enumIncludeViews, enumOrderBySortOrderIfPresent
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
