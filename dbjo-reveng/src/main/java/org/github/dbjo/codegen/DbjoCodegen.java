package org.github.dbjo.codegen;

import org.github.dbjo.codegen.db.DbIntrospector;
import org.github.dbjo.codegen.db.DbMetaGenerator;
import org.github.dbjo.codegen.db.DbSchemaGenerator;
import org.github.dbjo.codegen.db.IdentifierQuoter;
import org.github.dbjo.codegen.db.SqlQuoteMode;
import org.github.dbjo.codegen.entity.EntityGenerator;
import org.github.dbjo.codegen.entity.PojoValidatorGenerator;
import org.github.dbjo.meta.db.TableModel;
import org.github.dbjo.codegen.outbox.OutboxCreateTableSqlGenerator;
import org.github.dbjo.codegen.proto.ProtoGenerator;
import org.github.dbjo.codegen.proto.ProtocInvoker;
import org.github.dbjo.codegen.query.QueryTermsGenerator;
import org.github.dbjo.codegen.rdb.ProtoMapperGenerator;
import org.github.dbjo.codegen.rdb.RocksDaoGenerator;
import org.github.dbjo.codegen.rdb.RocksJdbcCatalogGenerator;
import org.github.dbjo.codegen.rdb.RocksSchemaGenerator;
import org.github.dbjo.codegen.registry.MetaRegistryGenerator;
import org.github.dbjo.codegen.util.EnumIndex;

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
            IdentifierQuoter quotedSqlIdentifiers = IdentifierQuoter.ansiFromMeta(md, SqlQuoteMode.ALWAYS);
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

            EnumIndex enumIndex = null;
            if (cfg.enumEnabled()) {
                enumIndex = EnumIndex.fromTables(tables);
                enumIndex.loadOverrides(cfg.enumOverridesFile());
                enumIndex.withEnumJavaPackage(cfg.enumPkg());
            }

            // 1) PROTO
            if (cfg.runMode().runProto()) {
                ProtoGenerator pg = new ProtoGenerator(cfg, enumIndex);
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
                int n = new EntityGenerator(cfg, enumIndex).generateAll(tables);
                System.out.println("Generated entity/meta for " + n + " table(s) into: " + cfg.outBase().toAbsolutePath());
                System.out.println();

                int r = new MetaRegistryGenerator(cfg).generate(tables);
                System.out.println("Generated meta registry file(s): " + r);
                System.out.println();
            }

            // 3) QUERY TERMS (<Entity>Q)
            if (cfg.runMode().runQuery()) {
                int n = new QueryTermsGenerator(cfg).generateAll(tables);
                System.out.println("Generated Query terms for " + n + " table(s) into: " + cfg.outBase().toAbsolutePath());
                System.out.println();
            }

            // 4) ROCKS SCHEMAS
            if (cfg.runMode().runDao() || cfg.runMode().runMapper()) {
                int ns = new RocksSchemaGenerator(cfg).generateAll(tables);
                System.out.println("Generated RocksDB Schema(s): " + ns);
                System.out.println();

                // JDBC catalog for Rocks JDBC driver
                int cat = new RocksJdbcCatalogGenerator(cfg).generate(tables);
                System.out.println("Generated Rocks JDBC catalog file(s): " + cat);
                System.out.println();
            }

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

            // 6) JDBC DbMeta (always quote table/column identifiers in SQL)
            if (cfg.runMode().runDbMeta()) {
                int d = new DbMetaGenerator(cfg, null, quotedSqlIdentifiers).generateAll(tables);
                System.out.println("Generated DbMeta: " + d);
                System.out.println();
            }

            if (cfg.runMode().runJdbcDao()) {
                int n = new org.github.dbjo.codegen.jdbc.JdbcDaoGenerator(cfg).generateAll(tables);
                System.out.println("Generated JDBC DAO(s): " + n);
                System.out.println();
            }

            if (cfg.runMode().runSchema() || cfg.runMode().runValidator()) {
                int n = new DbSchemaGenerator(cfg.codegenOutJava(), cfg.dbSchemaPkg(), cfg.overwrite()).generateAll(tables);
                System.out.println("Generated DB schema table metadata: " + n);
                System.out.println();
            }

            if (cfg.runMode().runValidator()) {
                int n = new PojoValidatorGenerator(cfg).generateAll(tables);
                System.out.println("Generated Pojo validator(s): " + n);
                System.out.println();
            }

            // 7) PROTO MAPPERS
            if (cfg.runMode().runMapper()) {
                int n = new ProtoMapperGenerator(cfg, enumIndex).generateAll(tables);
                System.out.println("Generated Proto mapper(s): " + n);
                System.out.println();
            }

            if (cfg.runMode().runOutboxSql()) {
                var outFile = new OutboxCreateTableSqlGenerator(cfg).generate(tables);
                System.out.println("Generated outbox CREATE TABLE SQL: " + outFile.toAbsolutePath());
                System.out.println();
            }
        }
    }
}
