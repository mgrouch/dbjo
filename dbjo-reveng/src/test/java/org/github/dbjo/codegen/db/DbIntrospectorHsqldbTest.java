package org.github.dbjo.codegen.db;

import org.github.dbjo.codegen.ArgMap;
import org.github.dbjo.codegen.Config;
import org.github.dbjo.meta.db.TableModel;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

final class DbIntrospectorHsqldbTest {

    @Test
    void introspectsSimpleTablePkAndIndex() throws Exception {
        try (Connection c = DriverManager.getConnection("jdbc:hsqldb:mem:dbjo_reveng_test", "SA", "")) {
            c.createStatement().executeUpdate("""
                    create table CLIENT (
                        ID    bigint not null,
                        EMAIL varchar(64),
                        primary key (ID)
                    )
                    """);
            c.createStatement().executeUpdate("""
                    create unique index UX_CLIENT_EMAIL on CLIENT(EMAIL)
                    """);

            Config cfg = Config.from(ArgMap.parse(new String[0]));
            DbIntrospector di = new DbIntrospector(cfg);

            List<TableModel> tables = di.loadTables(c.getMetaData());

            TableModel tm = tables.stream()
                    .filter(t -> t.table().table().equalsIgnoreCase("CLIENT"))
                    .findFirst()
                    .orElseThrow();

            assertTrue(tm.pkColsUpper().contains("ID"));
            assertTrue(tm.cols().stream().anyMatch(col -> col.colName().equalsIgnoreCase("EMAIL")));
            assertTrue(tm.indexes().stream().anyMatch(ix ->
                    ix.columnNames().stream().anyMatch(n -> n.equalsIgnoreCase("EMAIL"))
            ));
        }
    }
}
