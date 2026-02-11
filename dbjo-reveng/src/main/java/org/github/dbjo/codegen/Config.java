package org.github.dbjo.codegen;

import org.github.dbjo.criteria.PropertyTerm;
import org.github.dbjo.criteria.Terms;
import org.github.dbjo.dao.jdbc.BaseJdbcDAO;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Locale;
import java.util.regex.Pattern;

public record Config(
        Db db,
        Output output,
        Filters filters,
        RunMode runMode,
        Proto proto,
        Protoc protoc,
        Entity entity,
        Query query,
        Enums enums,
        EnumOverrides enumOverrides,
        Rocks rocks,
        ProtoMapper protoMapper,
        Validator validator,
        JdbcMeta jdbcMeta,
        JdbcDao jdbcDao,
        DbSchema dbSchema,
        Outbox outbox
) {
    // defaults
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

    // Use classpath (compile-time) instead of string literals:
    public static final String DEFAULT_TERMS_FQN = Terms.class.getName();
    public static final String DEFAULT_PROPERTY_TERM_FQN = PropertyTerm.class.getName();

    public static final String DEFAULT_PROTO_JAVA_PKG  = "org.github.dbjo.generated.proto";
    public static final String DEFAULT_PROTO_PKG_BASE  = "dbjo";
    public static final String DEFAULT_PROTO_OUTER_SUFFIX = "Proto";
    public static final boolean DEFAULT_PROTO_PER_TABLE = true;

    public static final boolean DEFAULT_ENUM_ENABLED = false;
    public static final String DEFAULT_ENUM_PKG = "org.github.dbjo.generated.model.enums";
    public static final boolean DEFAULT_ENUM_INCLUDE_VIEWS = false;
    public static final boolean DEFAULT_ENUM_ORDER_BY_SORT_ORDER = true;

    public static final String DEFAULT_DAO_PKG = "org.github.dbjo.generated.model.dao.rdb";
    public static final String DEFAULT_SCHEMA_PKG = "org.github.dbjo.generated.rdb.schema";
    public static final String DEFAULT_DAO_CLASS_SUFFIX = "Dao";
    public static final String DEFAULT_SCHEMA_CLASS_SUFFIX = "Schema";
    public static final String DEFAULT_CF_CONST_SUFFIX = "_CF";
    public static final String DEFAULT_DAO_BASE_CLASS = "IndexedRocksDao";

    public static final String DEFAULT_PROTO_MAPPER_PKG = "org.github.dbjo.generated.rdb.mapper";
    public static final String DEFAULT_PROTO_MAPPER_SUFFIX = "ProtoMapper";

    public static final String DEFAULT_VALIDATOR_PKG = "org.github.dbjo.generated.model.validator";
    public static final String DEFAULT_VALIDATOR_SUFFIX = "Validator";

    public static final String DEFAULT_SQL_DB_MAPPER_PKG = "org.github.dbjo.generated.model.dbmeta";

    // JDBC DAO generator defaults
    public static final String DEFAULT_JDBC_DAO_PKG = "org.github.dbjo.generated.model.dao.jdbc";
    public static final String DEFAULT_JDBC_DAO_CLASS_SUFFIX = "JdbcDao";

    // Use classpath (compile-time) instead of string literal:
    public static final String DEFAULT_JDBC_DAO_BASE_CLASS = BaseJdbcDAO.class.getName();

    // DB schema meta generator package (NOT Rocks schema)
    public static final String DEFAULT_DB_SCHEMA_PKG = "org.github.dbjo.generated.model.dbschema";

    public static final String DEFAULT_OUTBOX_SQL_DIR = "outbox-sql";

    // sections

    public record Db(String driver, String url, String user, String pass) {}

    public record Output(Path outBase, boolean overwrite, Path codegenOutJava) {}

    public record Filters(Pattern schemaInclude, Pattern schemaExclude, Pattern tableInclude, Pattern tableExclude) {}

    public record Proto(
            Path protoOutProto,
            Path protoOutJava,
            String protoJavaPkg,
            String protoPkgBase,
            String protoOuterSuffix,
            boolean protoPerTable,
            boolean protoRunProtoc,
            boolean protoExperimentalOptional
    ) {}

    public record Protoc(Path protocPath, Path protocInclude) {}

    public record Entity(
            String beanPkg,
            String metaPkg,
            String baseMetaPkg,
            String metaSuffix
    ) {}

    public record Query(
            String queryPkg,
            String querySuffix,
            String termsFqn,
            String propertyTermFqn
    ) {}

    public record Enums(
            boolean enumEnabled,
            String enumPkg,
            boolean enumIncludeViews,
            boolean enumOrderBySortOrderIfPresent
    ) {}

    public record EnumOverrides(Path enumOverridesFile, boolean enumStrictUnique) {}

    public record Rocks(
            String daoPkg,
            String schemaPkg,
            String daoClassSuffix,
            String schemaClassSuffix,
            String cfConstSuffix,
            String daoBaseClass
    ) {}

    public record ProtoMapper(String protoMapperPkg, String protoMapperSuffix) {}

    public record Validator(String validatorPkg, String validatorSuffix) {}

    public record JdbcMeta(String dbMetaPkg) {}

    public record JdbcDao(String jdbcDaoPkg, String jdbcDaoClassSuffix, String jdbcDaoBaseClass) {}

    public record DbSchema(String dbSchemaPkg) {}

    public record Outbox(Path outboxSqlDir, String outboxTableFqn, String outboxEntity) {}

    // run mode
    public enum RunMode {
        ALL, PROTO, ENUMS, ENTITY, QUERY, DAO, MAPPER, VALIDATOR, RDB, DBMETA, JDBCDAO, SCHEMA, OUTBOXSQL;

        public boolean runProto()   { return this == ALL || this == PROTO; }
        public boolean runEnums()   { return this == ALL || this == ENUMS; }
        public boolean runEntity()  { return this == ALL || this == ENTITY || this == RDB; }
        public boolean runQuery()   { return this == ALL || this == QUERY || this == ENTITY || this == RDB; }
        public boolean runDao()     { return this == ALL || this == DAO || this == RDB; }
        public boolean runMapper()  { return this == ALL || this == MAPPER || this == RDB; }
        public boolean runValidator() { return this == ALL || this == VALIDATOR || this == RDB; }

        public boolean runDbMeta()  { return this == ALL || this == DBMETA; }
        public boolean runJdbcDao() { return this == ALL || this == JDBCDAO; }
        public boolean runSchema()  { return this == ALL || this == SCHEMA; }
        public boolean runOutboxSql() { return this == ALL || this == OUTBOXSQL; }

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
                case "validator", "validators", "validate" -> VALIDATOR;
                case "rdb", "rocks", "rocksdb" -> RDB;
                case "dbmeta", "jdbc" -> DBMETA;
                case "jdbcdao", "jdbc-daos", "jdbcdaos" -> JDBCDAO;
                case "schema", "dbschema" -> SCHEMA;
                case "outboxsql", "outbox-sql", "outbox" -> OUTBOXSQL;
                default -> throw new IllegalArgumentException("Unknown --run=" + s +
                        " (use all|proto|enums|entity|query|dao|mapper|validator|rdb|dbmeta|jdbcdao|schema|outboxsql)");
            };
        }
    }

    // flat compatibility accessors

    // DB
    public String driver() { return db.driver(); }
    public String url()    { return db.url(); }
    public String user()   { return db.user(); }
    public String pass()   { return db.pass(); }

    // output
    public Path outBase()        { return output.outBase(); }
    public boolean overwrite()   { return output.overwrite(); }
    public Path codegenOutJava() { return output.codegenOutJava(); }

    // filters
    public Pattern schemaInclude(){ return filters.schemaInclude(); }
    public Pattern schemaExclude(){ return filters.schemaExclude(); }
    public Pattern tableInclude() { return filters.tableInclude(); }
    public Pattern tableExclude() { return filters.tableExclude(); }

    // proto
    public Path protoOutProto() { return proto.protoOutProto(); }
    public Path protoOutJava()  { return proto.protoOutJava(); }
    public String protoJavaPkg(){ return proto.protoJavaPkg(); }
    public String protoPkgBase(){ return proto.protoPkgBase(); }
    public String protoOuterSuffix(){ return proto.protoOuterSuffix(); }
    public boolean protoPerTable(){ return proto.protoPerTable(); }
    public boolean protoRunProtoc(){ return proto.protoRunProtoc(); }
    public boolean protoExperimentalOptional(){ return proto.protoExperimentalOptional(); }

    // protoc
    public Path protocPath(){ return protoc.protocPath(); }
    public Path protocInclude(){ return protoc.protocInclude(); }

    // entity/meta
    public String beanPkg(){ return entity.beanPkg(); }
    public String metaPkg(){ return entity.metaPkg(); }
    public String baseMetaPkg(){ return entity.baseMetaPkg(); }

    // enums
    public boolean enumEnabled(){ return enums.enumEnabled(); }
    public String enumPkg(){ return enums.enumPkg(); }
    public boolean enumIncludeViews(){ return enums.enumIncludeViews(); }
    public boolean enumOrderBySortOrderIfPresent(){ return enums.enumOrderBySortOrderIfPresent(); }

    // enum overrides
    public Path enumOverridesFile(){ return enumOverrides.enumOverridesFile(); }
    public boolean enumStrictUnique(){ return enumOverrides.enumStrictUnique(); }

    // rocks dao/schema
    public String daoPkg(){ return rocks.daoPkg(); }
    public String schemaPkg(){ return rocks.schemaPkg(); }
    public String daoClassSuffix(){ return rocks.daoClassSuffix(); }
    public String schemaClassSuffix(){ return rocks.schemaClassSuffix(); }
    public String cfConstSuffix(){ return rocks.cfConstSuffix(); }

    // proto mapper
    public String protoMapperPkg(){ return protoMapper.protoMapperPkg(); }
    public String protoMapperSuffix(){ return protoMapper.protoMapperSuffix(); }

    // pojo validator
    public String validatorPkg(){ return validator.validatorPkg(); }
    public String validatorSuffix(){ return validator.validatorSuffix(); }

    // jdbc meta
    public String dbMetaPkg(){ return jdbcMeta.dbMetaPkg(); }

    // jdbc dao config
    public String jdbcDaoPkg(){ return jdbcDao.jdbcDaoPkg(); }
    public String jdbcDaoClassSuffix(){ return jdbcDao.jdbcDaoClassSuffix(); }
    public String jdbcDaoBaseClass(){ return jdbcDao.jdbcDaoBaseClass(); }

    // db schema meta pkg
    public String dbSchemaPkg(){ return dbSchema.dbSchemaPkg(); }

    // outbox
    public Path outboxSqlDir() { return outbox.outboxSqlDir(); }
    public String outboxTableFqn() { return outbox.outboxTableFqn(); }
    public String outboxEntity() { return outbox.outboxEntity(); }

    // factory
    public static Config from(ArgMap am) {
        // DB
        String driver = am.get("driver", System.getProperty("db.driver", DEFAULT_DRIVER));
        String url    = am.get("url",    System.getProperty("db.url",    DEFAULT_URL));
        String user   = am.get("user",   System.getProperty("db.user",   DEFAULT_USER));
        String pass   = am.get("pass",   System.getProperty("db.pass",   DEFAULT_PASS));

        // Output
        Path outBase = Paths.get(am.get("outBase", DEFAULT_OUT_BASE.toString()));
        boolean overwrite = am.getBool("overwrite", false);
        Path codegenOutJava = Paths.get(am.get("codegenOutJava", outBase.resolve("codegen-java").toString()));

        // Filters
        Pattern schemaInc = am.getRegex("schemaInclude", null);
        Pattern schemaExc = am.getRegex("schemaExclude", null);
        Pattern tableInc  = am.getRegex("tableInclude", null);
        Pattern tableExc  = am.getRegex("tableExclude", null);

        // Run mode
        RunMode runMode = RunMode.parse(am.get("run", "all"));

        // Proto output dirs
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

        // Entity/meta
        String beanPkg = am.get("beanPkg", DEFAULT_BEAN_PKG);
        String metaPkg = am.get("metaPkg", DEFAULT_META_PKG);
        String baseMetaPkg = am.get("baseMetaPkg", DEFAULT_BASE_META_PKG);
        String metaSuffix = am.get("metaSuffix", DEFAULT_META_SUFFIX);

        // Query
        String queryPkgDefault = metaPkg.contains(".meta")
                ? metaPkg.replace(".meta", ".query")
                : metaPkg + ".query";
        String queryPkg = am.get("queryPkg", System.getProperty("dbjo.queryPkg", queryPkgDefault));
        String querySuffix = am.get("querySuffix", System.getProperty("dbjo.querySuffix", DEFAULT_QUERY_SUFFIX));
        String termsFqn = am.get("termsFqn", System.getProperty("dbjo.termsFqn", DEFAULT_TERMS_FQN));
        String propertyTermFqn = am.get("propertyTermFqn", System.getProperty("dbjo.propertyTermFqn", DEFAULT_PROPERTY_TERM_FQN));

        // Enums
        boolean enumEnabled = am.getBool("enumEnabled",
                Boolean.parseBoolean(System.getProperty("dbjo.enumEnabled", String.valueOf(DEFAULT_ENUM_ENABLED))));
        String enumPkg = am.get("enumPkg", System.getProperty("dbjo.enumPkg", DEFAULT_ENUM_PKG));
        boolean enumIncludeViews = am.getBool("enumIncludeViews",
                Boolean.parseBoolean(System.getProperty("dbjo.enumIncludeViews", String.valueOf(DEFAULT_ENUM_INCLUDE_VIEWS))));
        boolean enumOrderBySortOrder = am.getBool("enumOrderBySortOrderIfPresent",
                Boolean.parseBoolean(System.getProperty("dbjo.enumOrderBySortOrderIfPresent", String.valueOf(DEFAULT_ENUM_ORDER_BY_SORT_ORDER))));

        // Enum overrides
        String enumOverridesPath = am.get("enumOverridesFile", System.getProperty("dbjo.enumOverridesFile", ""));
        Path enumOverridesFile = (enumOverridesPath == null || enumOverridesPath.isBlank()) ? null : Paths.get(enumOverridesPath.trim());

        boolean enumStrictUnique = am.getBool("enumStrictUnique",
                am.getBool("strictUnique", Boolean.parseBoolean(System.getProperty("dbjo.strictUnique", "false"))));

        // Rocks
        String daoPkg = am.get("daoPkg", DEFAULT_DAO_PKG);
        String schemaPkg = am.get("schemaPkg", DEFAULT_SCHEMA_PKG);
        String daoClassSuffix = am.get("daoClassSuffix", DEFAULT_DAO_CLASS_SUFFIX);
        String schemaClassSuffix = am.get("schemaClassSuffix", DEFAULT_SCHEMA_CLASS_SUFFIX);
        String cfConstSuffix = am.get("cfConstSuffix", DEFAULT_CF_CONST_SUFFIX);
        String daoBaseClass = am.get("daoBaseClass", DEFAULT_DAO_BASE_CLASS);

        // Proto mapper
        String protoMapperPkg = am.get("protoMapperPkg", DEFAULT_PROTO_MAPPER_PKG);
        String protoMapperSuffix = am.get("protoMapperSuffix", DEFAULT_PROTO_MAPPER_SUFFIX);

        // Pojo validator
        String validatorPkg = am.get("validatorPkg", DEFAULT_VALIDATOR_PKG);
        String validatorSuffix = am.get("validatorSuffix", DEFAULT_VALIDATOR_SUFFIX);

        // SQL/JDBC meta
        String dbMetaPkg = am.get("dbMetaPkg", DEFAULT_SQL_DB_MAPPER_PKG);

        // JDBC DAO settings
        String jdbcDaoPkg = am.get("jdbcDaoPkg",
                System.getProperty("dbjo.jdbcDaoPkg", DEFAULT_JDBC_DAO_PKG));
        String jdbcDaoClassSuffix = am.get("jdbcDaoClassSuffix",
                System.getProperty("dbjo.jdbcDaoClassSuffix", DEFAULT_JDBC_DAO_CLASS_SUFFIX));
        String jdbcDaoBaseClass = am.get("jdbcDaoBaseClass",
                System.getProperty("dbjo.jdbcDaoBaseClass", DEFAULT_JDBC_DAO_BASE_CLASS));

        // DB schema meta package
        String dbSchemaPkg = am.get("dbSchemaPkg", System.getProperty("dbjo.dbSchemaPkg", DEFAULT_DB_SCHEMA_PKG));

        // Outbox SQL
        Path outboxSqlDir = Paths.get(am.get("outboxSqlDir", outBase.resolve(DEFAULT_OUTBOX_SQL_DIR).toString()));
        String outboxTableFqn = am.get("outboxTableFqn", "").trim();
        String outboxEntity = am.get("outboxEntity", "").trim();

        return new Config(
                new Db(driver, url, user, pass),
                new Output(outBase, overwrite, codegenOutJava),
                new Filters(schemaInc, schemaExc, tableInc, tableExc),
                runMode,
                new Proto(protoOutProto, protoOutJava, protoJavaPkg, protoPkgBase, protoOuterSuffix,
                        protoPerTable, protoRunProtoc, protoExperimentalOptional),
                new Protoc(protocPath, protocInclude),
                new Entity(beanPkg, metaPkg, baseMetaPkg, metaSuffix),
                new Query(queryPkg, querySuffix, termsFqn, propertyTermFqn),
                new Enums(enumEnabled, enumPkg, enumIncludeViews, enumOrderBySortOrder),
                new EnumOverrides(enumOverridesFile, enumStrictUnique),
                new Rocks(daoPkg, schemaPkg, daoClassSuffix, schemaClassSuffix, cfConstSuffix, daoBaseClass),
                new ProtoMapper(protoMapperPkg, protoMapperSuffix),
                new Validator(validatorPkg, validatorSuffix),
                new JdbcMeta(dbMetaPkg),
                new JdbcDao(jdbcDaoPkg, jdbcDaoClassSuffix, jdbcDaoBaseClass),
                new DbSchema(dbSchemaPkg),
                new Outbox(outboxSqlDir, outboxTableFqn, outboxEntity)
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
