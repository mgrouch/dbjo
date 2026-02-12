package org.github.dbjo.codegen.outbox;

import org.github.dbjo.codegen.ArgMap;
import org.github.dbjo.codegen.Config;
import org.github.dbjo.meta.db.Col;
import org.github.dbjo.meta.db.Nullability;
import org.github.dbjo.meta.db.TableModel;
import org.github.dbjo.meta.db.TableRef;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Types;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OutboxCreateTableSqlGeneratorTest {

    @TempDir
    Path tmp;

    @Test
    void generatesOutboxSqlForSelectedEntity() throws Exception {
        Config cfg = Config.from(ArgMap.parse(new String[] {
            "--run=outboxsql",
            "--outboxSqlDir=" + tmp,
            "--outboxEntity=orders"
        }));

        TableModel orders = table("dbo", "orders", "id", "status");
        TableModel users = table("dbo", "users", "id", "email");

        Path file = new OutboxCreateTableSqlGenerator(cfg).generate(List.of(orders, users));
        String sql = Files.readString(file);

        assertTrue(sql.contains("CREATE TABLE dbo.orders_outbox"));
        assertTrue(sql.contains("id VARCHAR(20) NULL"));
        assertTrue(sql.contains("status VARCHAR(20) NULL"));
        assertTrue(sql.contains("partition_key NVARCHAR(40) NULL"));
        assertTrue(sql.contains("published_at_utc DATETIME2 NULL"));
        assertTrue(!sql.contains("lock_owner"));
        assertTrue(!sql.contains("locked_at_utc"));
        assertTrue(!sql.contains("published_topic"));
        assertTrue(file.getFileName().toString().equals("orders-outbox-create.sql"));
    }

    @Test
    void failsWhenNoEntityAndMoreThanOneTable() {
        Config cfg = Config.from(ArgMap.parse(new String[] {
            "--run=outboxsql",
            "--outboxSqlDir=" + tmp
        }));

        TableModel orders = table("dbo", "orders", "id");
        TableModel users = table("dbo", "users", "id");

        assertThrows(IllegalArgumentException.class,
            () -> new OutboxCreateTableSqlGenerator(cfg).generate(List.of(orders, users)));
    }

    @Test
    void skipsPayloadColumnsThatConflictWithOutboxColumns() throws Exception {
        Config cfg = Config.from(ArgMap.parse(new String[] {
            "--run=outboxsql",
            "--outboxSqlDir=" + tmp,
            "--outboxEntity=orders"
        }));

        TableModel orders = table("dbo", "orders", "id", "partition_key", "published_at_utc", "status");

        Path file = new OutboxCreateTableSqlGenerator(cfg).generate(List.of(orders));
        String sql = Files.readString(file);

        assertTrue(sql.contains("id VARCHAR(20) NULL"));
        assertTrue(sql.contains("status VARCHAR(20) NULL"));
        assertTrue(!sql.contains("partition_key VARCHAR(20) NULL"));
        assertTrue(!sql.contains("published_at_utc VARCHAR(20) NULL"));
        assertTrue(sql.contains("partition_key NVARCHAR(40) NULL"));
        assertTrue(sql.contains("published_at_utc DATETIME2 NULL"));
    }

    private static TableModel table(String schema, String table, String... cols) {
        List<Col> modelCols = java.util.stream.IntStream.range(0, cols.length)
            .mapToObj(i -> new Col(i + 1, cols[i], Types.VARCHAR, "VARCHAR", 20, 0, Nullability.NULLABLE, false, null))
            .toList();
        return new TableModel(new TableRef(schema, table), modelCols, Set.of(), List.of());
    }
}
