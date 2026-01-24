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
        String metaSuffix,
        Path codegenOutJava,

        // query terms
        String queryPkg,
        String querySuffix,
        String termsFqn,
        String propertyTermFqn,

        // enum generation
        boolean enumEnabled,
        String enumPkg,
        boolean enumIncludeViews,
        boolean enumOrderBySortOrderIfPresent,

        // enum overrides
        Path enumOverridesFile,
        boolean enumStrictUnique,

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
        String dbMetaPkg
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

    public static final String DEFAULT_QUERY_SUFFIX = "Q";
    public static final String DEFAULT_TERMS_FQN = "org.github.dbjo.criteria.Terms";
    public static final String DEFAULT_PROPERTY_TERM_FQN = "org.github.dbjo.criteria.PropertyTerm";

    public static final String DEFAULT_PROTO_JAVA_PKG  = "org.github.dbjo.generated.proto";
    public static final String DEFAULT_PROTO_PKG_BASE  = "dbjo";
    public static final String DEFAULT_PROTO_OUTER_SUFFIX = "Proto";
    public static final boolean DEFAULT_PROTO_PER_TABLE = true;

    public static final boolean DEFAULT_ENUM_ENABLED = false;
    public static final String DEFAULT_ENUM_PKG = "org.github.dbjo.generated.model.enums";
    public static final boolean DEFAULT_ENUM_INCLUDE_VIEWS = false;
    public static final boolean DEFAULT_ENUM_ORDER_BY_SORT_ORDER = true;

    public static final String DEFAULT_DAO_PKG = "org.github.dbjo.generated.rdb.dao";
    public static final String DEFAULT_SCHEMA_PKG = "org.github.dbjo.generated.rdb.schema";
    public static final String DEFAULT_DAO_CLASS_SUFFIX = "Dao";
    public static final String DEFAULT_SCHEMA_CLASS_SUFFIX = "Schema";
    public static final String DEFAULT_CF_CONST_SUFFIX = "_CF";
    public static final String DEFAULT_DAO_BASE_CLASS = "IndexedRocksDao";

    public static final String DEFAULT_PROTO_MAPPER_PKG = "org.github.dbjo.generated.rdb.mapper";
    public static final String DEFAULT_PROTO_MAPPER_SUFFIX = "ProtoMapper";

    public static final String DEFAULT_SQL_DB_MAPPER_PKG = "org.github.dbjo.generated.model.dbmeta";

    public enum RunMode {
        ALL, PROTO, ENUMS, ENTITY, QUERY, DAO, MAPPER, RDB, DBMETA;

        public boolean runProto()  { return this == ALL || this == PROTO; }
        public boolean runEnums()  { return this == ALL || this == ENUMS; }
        public boolean runEntity() { return this == ALL || this == ENTITY || this == RDB; }
        public boolean runQuery()  { return this == ALL || this == QUERY || this == ENTITY || this == RDB; }
        public boolean runDao()    { return this == ALL || this == DAO || this == RDB; }
        public boolean runMapper() { return this == ALL || this == MAPPER || this == RDB; }
        public boolean runDbMeta() { return this == ALL || this == DBMETA; }

        public static RunMode parse(String s) {
            if (s == null) return ALL;
            return switch (s.trim().toLowerCase(Locale.ROOT)) {
                case "all", "both" -> ALL;
                case "proto" -> PROTO;
                case "enums", "enum" -> ENUMS;
                case "entity", "entities" -> ENTITY;
                case "query", "criteria" -> QUERY;
                case "dao", "daos" -> DAO;
                case "mapper", "mappers" -> MAPPER;
                case "rdb", "rocks", "rocksdb" -> RDB;
                case "dbmeta", "jdbc" -> DBMETA;
                default -> throw new IllegalArgumentException("Unknown --run=" + s +
                        " (use all|proto|enums|entity|query|dao|mapper|rdb|dbmeta)");
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

        // query pkg default: metaPkg -> replace ".meta" with ".query" if present, else append ".query"
        String queryPkgDefault = metaPkg.contains(".meta")
                ? metaPkg.replace(".meta", ".query")
                : metaPkg + ".query";
        String queryPkg = am.get("queryPkg", System.getProperty("dbjo.queryPkg", queryPkgDefault));
        String querySuffix = am.get("querySuffix", System.getProperty("dbjo.querySuffix", DEFAULT_QUERY_SUFFIX));
        String termsFqn = am.get("termsFqn", System.getProperty("dbjo.termsFqn", DEFAULT_TERMS_FQN));
        String propertyTermFqn = am.get("propertyTermFqn", System.getProperty("dbjo.propertyTermFqn", DEFAULT_PROPERTY_TERM_FQN));

        boolean enumEnabled = am.getBool("enumEnabled", Boolean.parseBoolean(System.getProperty("dbjo.enumEnabled", String.valueOf(DEFAULT_ENUM_ENABLED))));
        String enumPkg = am.get("enumPkg", System.getProperty("dbjo.enumPkg", DEFAULT_ENUM_PKG));
        boolean enumIncludeViews = am.getBool("enumIncludeViews", Boolean.parseBoolean(System.getProperty("dbjo.enumIncludeViews", String.valueOf(DEFAULT_ENUM_INCLUDE_VIEWS))));
        boolean enumOrderBySortOrder = am.getBool("enumOrderBySortOrderIfPresent",
                Boolean.parseBoolean(System.getProperty("dbjo.enumOrderBySortOrderIfPresent", String.valueOf(DEFAULT_ENUM_ORDER_BY_SORT_ORDER))));

        String enumOverridesPath = am.get("enumOverridesFile", System.getProperty("dbjo.enumOverridesFile", ""));
        Path enumOverridesFile = (enumOverridesPath == null || enumOverridesPath.isBlank()) ? null : Paths.get(enumOverridesPath.trim());

        boolean enumStrictUnique = am.getBool("strictUnique",
                Boolean.parseBoolean(System.getProperty("dbjo.strictUnique", "false")));

        String daoPkg = am.get("daoPkg", DEFAULT_DAO_PKG);
        String schemaPkg = am.get("schemaPkg", DEFAULT_SCHEMA_PKG);
        String daoClassSuffix = am.get("daoClassSuffix", DEFAULT_DAO_CLASS_SUFFIX);
        String schemaClassSuffix = am.get("schemaClassSuffix", DEFAULT_SCHEMA_CLASS_SUFFIX);
        String cfConstSuffix = am.get("cfConstSuffix", DEFAULT_CF_CONST_SUFFIX);
        String daoBaseClass = am.get("daoBaseClass", DEFAULT_DAO_BASE_CLASS);

        String protoMapperPkg = am.get("protoMapperPkg", DEFAULT_PROTO_MAPPER_PKG);
        String protoMapperSuffix = am.get("protoMapperSuffix", DEFAULT_PROTO_MAPPER_SUFFIX);

        String dbMetaPkg = am.get("dbMetaPkg", DEFAULT_SQL_DB_MAPPER_PKG);

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

                enumEnabled, enumPkg, enumIncludeViews, enumOrderBySortOrder,

                enumOverridesFile, enumStrictUnique,

                daoPkg, schemaPkg, daoClassSuffix, schemaClassSuffix, cfConstSuffix, daoBaseClass,

                protoMapperPkg, protoMapperSuffix,

                dbMetaPkg
        );
    }

    private static Path resolveProtocPath(ArgMap am) {
        String p = am.get("protoc", System.getProperty("protoc"));
        if (p != null && !p.isBlank()) return Paths.get(p);
        boolean win = System.getProperty("os.name", "").toLowerCase(Locale.ROOT).contains("win");
        return Paths.get("target", "tools", "protoc", win ? "protoc.exe" : "protoc");
    }

    private static Path resolveProtocIncludeDir(ArgMap am) {
        String p = am.get("protocInclude", System.getProperty("protoc.include"));
        if (p != null && !p.isBlank()) return Paths.get(p);
        return Paths.get("target", "tools", "protoc", "include");
    }
}
