package org.github.dbjo.codegen;

import org.github.dbjo.codegen.db.DbIntrospector;
import org.github.dbjo.codegen.db.DbMetaGenerator;
import org.github.dbjo.codegen.db.DbSchemaGenerator;
import org.github.dbjo.codegen.entity.EntityGenerator;
import org.github.dbjo.codegen.jdbc.JdbcDaoGenerator;
import org.github.dbjo.codegen.proto.ProtoGenerator;
import org.github.dbjo.codegen.proto.ProtocInvoker;
import org.github.dbjo.codegen.query.QueryTermsGenerator;
import org.github.dbjo.codegen.rdb.ProtoMapperGenerator;
import org.github.dbjo.codegen.rdb.RocksDaoGenerator;
import org.github.dbjo.codegen.rdb.RocksSchemaGenerator;
import org.github.dbjo.codegen.registry.MetaRegistryGenerator;
import org.github.dbjo.meta.db.TableModel;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.util.List;

public final class DbjoCodegen {
    private DbjoCodegen() {}

    public static void main(String[] args) throws Exception {
        ArgMap am = ArgMap.parse(args);
        Config cfg = Config.from(am);

        System.out.println("DBJO codegen");
        System.out.println("  run        = " + cfg.runMode());
        System.out.println("  url        = " + cfg.url());
        System.out.println("  user       = " + cfg.user());
        System.out.println("  outBase    = " + cfg.outBase().toAbsolutePath());
        System.out.println("  overwrite  = " + cfg.overwrite());
        if (cfg.schemaInclude() != null) System.out.println("  schemaInc  = " + cfg.schemaInclude());
        if (cfg.schemaExclude() != null) System.out.println("  schemaExc  = " + cfg.schemaExclude());
        if (cfg.tableInclude()  != null) System.out.println("  tableInc   = " + cfg.tableInclude());
        if (cfg.tableExclude()  != null) System.out.println("  tableExc   = " + cfg.tableExclude());
        System.out.println();

        Class.forName(cfg.driver());

        try (Connection conn = DriverManager.getConnection(cfg.url(), cfg.user(), cfg.pass())) {
            DatabaseMetaData md = conn.getMetaData();
            System.out.println("Connected:");
            System.out.println("  db = " + md.getDatabaseProductName() + " " + md.getDatabaseProductVersion());
            System.out.println();

            DbIntrospector di = new DbIntrospector(cfg);
            List<TableModel> tables = di.loadTables(md);

            if (tables.isEmpty()) {
                System.out.println("No user tables found after filtering.");
                return;
            }

            System.out.println("Tables: " + tables.size());
            System.out.println();

            // 0) DB SCHEMA (runtime metadata of tables/columns/indexes)
            // Runs when --run=schema or --run=all
            if (cfg.runMode().runSchema()) {
                int n = new DbSchemaGenerator(
                        cfg.codegenOutJava(),
                        cfg.dbSchemaPkg(),
                        cfg.overwrite()
                ).generateAll(tables);

                System.out.println("Generated DbSchema: " + n + " file(s)");
                System.out.println();
            }

            // 1) PROTO
            if (cfg.runMode().runProto()) {
                ProtoGenerator pg = new ProtoGenerator(cfg);
                var protos = pg.generateAll(tables);
                System.out.println("Wrote " + protos.size() + " proto file(s) into: " + cfg.protoOutProto().toAbsolutePath());

                if (!protos.isEmpty() && cfg.protoRunProtoc()) {
                    new ProtocInvoker(cfg).runProtoc(protos);
                    System.out.println("Generated Java into: " + cfg.protoOutJava().toAbsolutePath());
                }
                System.out.println();
            }

            // 2) ENTITY + META (<Entity>Meta)
            if (cfg.runMode().runEntity()) {
                int n = new EntityGenerator(cfg).generateAll(tables);
                System.out.println("Generated entity/meta for " + n + " table(s) into: " + cfg.outBase().toAbsolutePath());
                System.out.println();

                int r = new MetaRegistryGenerator(cfg).generate(tables);
                System.out.println("Generated meta registry file(s): " + r);
                System.out.println();
            }

            // 3) QUERY TERMS (<Entity>Q) — requires <Entity>Meta, so run after entity/meta
            if (cfg.runMode().runQuery()) {
                int n = new QueryTermsGenerator(cfg).generateAll(tables);
                System.out.println("Generated Query terms for " + n + " table(s) into: " + cfg.outBase().toAbsolutePath());
                System.out.println();
            }

            // 4) ROCKS SCHEMAS (needed by DAO + mapper)
            if (cfg.runMode().runDao() || cfg.runMode().runMapper()) {
                int ns = new RocksSchemaGenerator(cfg).generateAll(tables);
                System.out.println("Generated RocksDB Schema(s): " + ns);
                System.out.println();
            }

            // 4b) ENUM TABLES (Java enums)
            if (cfg.enumEnabled() && (cfg.runMode().runEntity() || cfg.runMode().runDao() || cfg.runMode().runMapper())) {
                int en = new org.github.dbjo.codegen.db.DbEnumCodeGenerator(cfg).generateAll(conn);
                System.out.println("Generated enum(s): " + en + " into: " +
                        cfg.codegenOutJava().resolve(cfg.enumPkg().replace('.', '/')).toAbsolutePath());
                System.out.println();
            }

            // 5) ROCKS DAOs
            if (cfg.runMode().runDao()) {
                int n = new RocksDaoGenerator(cfg).generateAll(tables);
                System.out.println("Generated RocksDB DAO(s): " + n);
                System.out.println();
            }

            // 5b) JDBC DB META (DbMeta classes)
            // Run if --run=dbmeta OR if you are already generating DAOs (historical behavior)
            boolean wantDbMeta = cfg.runMode().runDbMeta() || cfg.runMode().runDao();
            if (wantDbMeta) {
                int d = new DbMetaGenerator(cfg).generateAll(tables);
                System.out.println("Generated DbMeta: " + d);
                System.out.println();
            }

            // 5c) JDBC DAOs (BaseJdbcDAO subclasses)
            // Run if explicitly requested OR when producing DbMeta (typical pairing).
            boolean wantJdbcDao = cfg.runMode().runJdbcDao() || wantDbMeta;
            if (wantJdbcDao) {
                int j = new JdbcDaoGenerator(cfg).generateAll(tables);
                System.out.println("Generated JdbcDao(s): " + j + " into: " +
                        cfg.codegenOutJava().resolve(cfg.jdbcDaoPkg().replace('.', '/')).toAbsolutePath());
                System.out.println();
            }

            // 6) PROTO MAPPERS
            if (cfg.runMode().runMapper()) {
                int n = new ProtoMapperGenerator(cfg).generateAll(tables);
                System.out.println("Generated Proto mapper(s): " + n);
                System.out.println();
            }
        }
    }
}
