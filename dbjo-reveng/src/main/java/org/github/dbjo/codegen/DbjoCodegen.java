package org.github.dbjo.codegen;

import org.github.dbjo.codegen.db.DbEnumCodeGenerator;
import org.github.dbjo.codegen.db.DbIntrospector;
import org.github.dbjo.codegen.db.DbMetaGenerator;
import org.github.dbjo.codegen.db.EnumOverrideIndex;
import org.github.dbjo.codegen.db.IdentifierQuoter;
import org.github.dbjo.codegen.db.SqlQuoteMode;
import org.github.dbjo.meta.db.TableModel;

import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.util.List;

public final class DbjoCodegen {
    private DbjoCodegen() {}

    public static void main(String[] args) throws Exception {
        ArgMap am = ArgMap.parse(args);
        Config cfg = Config.from(am);

        System.out.println("DbjoCodegen:");
        System.out.println("  url        = " + cfg.url());
        System.out.println("  user       = " + cfg.user());
        System.out.println("  outBase    = " + cfg.outBase().toAbsolutePath());
        System.out.println("  codegenJava= " + cfg.codegenOutJava().toAbsolutePath());

        if (cfg.schemaInclude() != null) System.out.println("  schemaInc  = " + cfg.schemaInclude());
        if (cfg.schemaExclude() != null) System.out.println("  schemaExc  = " + cfg.schemaExclude());
        if (cfg.tableInclude()  != null) System.out.println("  tableInc   = " + cfg.tableInclude());
        if (cfg.tableExclude()  != null) System.out.println("  tableExc   = " + cfg.tableExclude());

        SqlQuoteMode sqlQuote = SqlQuoteMode.parse(am.get("sqlQuote", "auto"));
        System.out.println("  sqlQuote   = " + sqlQuote);
        System.out.println();

        // Ensure output dirs exist
        Files.createDirectories(cfg.outBase());
        Files.createDirectories(cfg.codegenOutJava());

        Class.forName(cfg.driver());

        try (Connection c = DriverManager.getConnection(cfg.url(), cfg.user(), cfg.pass())) {
            DatabaseMetaData md = c.getMetaData();

            // Introspect user tables (applies cfg schema/table include/exclude filters)
            DbIntrospector in = new DbIntrospector(cfg);
            List<TableModel> tables = in.loadTables(md);

            if (tables.isEmpty()) {
                System.out.println("No user tables found after filtering.");
                return;
            }

            // Enum overrides (optional; returns empty index if file missing)
            EnumOverrideIndex enumOverrides = EnumOverrideIndex.loadAndValidate(cfg, c);

            // Generate enums (generator discovers enum tables itself)
            int enums = new DbEnumCodeGenerator(cfg).generateAll(c);
            if (enums > 0) {
                System.out.println("Generated enum(s): " + enums);
            }

            // Identifier quoting config for SQL generation
            IdentifierQuoter quoter = IdentifierQuoter.ansiFromMeta(md, sqlQuote);

            // Generate DbMeta (uses enumOverrides + quoter)
            DbMetaGenerator metaGen = new DbMetaGenerator(cfg, enumOverrides, quoter);
            int count = metaGen.generateAll(tables);

            System.out.println("Generated DbMeta: " + count + " file(s).");
        }
    }
}
