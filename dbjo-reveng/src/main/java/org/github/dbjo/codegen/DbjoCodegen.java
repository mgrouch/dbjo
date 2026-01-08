package org.github.dbjo.codegen;

import org.github.dbjo.codegen.db.DbIntrospector;
import org.github.dbjo.codegen.entity.EntityGenerator;
import org.github.dbjo.codegen.model.TableModel;
import org.github.dbjo.codegen.proto.ProtoGenerator;
import org.github.dbjo.codegen.proto.ProtocInvoker;

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
            List<TableModel> tables = di.loadTables(conn.getMetaData());

            if (tables.isEmpty()) {
                System.out.println("No user tables found after filtering.");
                return;
            }

            System.out.println("Tables: " + tables.size());
            System.out.println();

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

            if (cfg.runMode().runEntity()) {
                int n = new EntityGenerator(cfg).generateAll(tables);
                System.out.println("Generated entity/meta for " + n + " table(s) into: " + cfg.outBase().toAbsolutePath());
                System.out.println();
            }
        }
    }
}
