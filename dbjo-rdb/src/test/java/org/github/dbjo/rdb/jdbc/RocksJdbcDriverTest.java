package org.github.dbjo.rdb.jdbc;

import org.github.dbjo.rdb.jdbc.catalog.RocksJdbcCatalog;
import org.github.dbjo.rdb.jdbc.catalog.RocksJdbcColumn;
import org.github.dbjo.rdb.jdbc.catalog.RocksJdbcDecoder;
import org.github.dbjo.rdb.jdbc.catalog.RocksJdbcIndex;
import org.github.dbjo.rdb.jdbc.catalog.RocksJdbcTable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;

import javax.sql.rowset.CachedRowSet;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class RocksJdbcDriverTest {

    @TempDir
    Path dir;

    @Test
    void countAndWhereQueriesWork() throws Exception {
        try (RocksJdbcEngine engine = new RocksJdbcEngine(new TestCatalog(), dir.toString(), false)) {
            insertRows(engine);

            CachedRowSet rs = engine.query("select count(*) from items where amount >= 20", 0);
            assertTrue(rs.next());
            assertEquals(2L, rs.getLong(1));
        }
    }

    @Test
    void groupByHavingOrderByWorks() throws Exception {
        try (RocksJdbcEngine engine = new RocksJdbcEngine(new TestCatalog(), dir.toString(), false)) {
            insertRows(engine);

            String sql = "select category, count(*) from items group by category having count > 1 order by category desc";
            CachedRowSet rs = engine.query(sql, 0);

            assertTrue(rs.next());
            assertEquals("B", rs.getString(1));
            assertEquals(2L, rs.getLong(2));
            assertTrue(!rs.next());
        }
    }

    @Test
    void orderByAndLimitApply() throws Exception {
        try (RocksJdbcEngine engine = new RocksJdbcEngine(new TestCatalog(), dir.toString(), false)) {
            insertRows(engine);

            String sql = "select id, amount from items where amount >= 10 order by amount desc limit 2";
            CachedRowSet rs = engine.query(sql, 0);

            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));
            assertEquals(30L, rs.getLong(2));

            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            assertEquals(20L, rs.getLong(2));
            assertTrue(!rs.next());
        }
    }

    private static void insertRows(RocksJdbcEngine engine) throws Exception {
        RocksDB db = engine.db();
        ColumnFamilyHandle cf = engine.cfsByName().get("items");
        if (cf == null) throw new SQLException("Missing CF: items");

        putRow(db, cf, 1, "A", 10);
        putRow(db, cf, 2, "B", 20);
        putRow(db, cf, 3, "B", 30);
    }

    private static void putRow(RocksDB db, ColumnFamilyHandle cf, int id, String category, long amount)
            throws Exception {
        String payload = id + "," + category + "," + amount;
        db.put(cf, String.valueOf(id).getBytes(StandardCharsets.UTF_8),
                payload.getBytes(StandardCharsets.UTF_8));
    }

    private static final class ItemRow {
        private final int id;
        private final String category;
        private final long amount;

        private ItemRow(int id, String category, long amount) {
            this.id = id;
            this.category = category;
            this.amount = amount;
        }

        public int getId() { return id; }
        public String getCategory() { return category; }
        public long getAmount() { return amount; }
    }

    private static final class TestCatalog implements RocksJdbcCatalog {
        private final RocksJdbcTable table;

        private TestCatalog() {
            RocksJdbcColumn[] cols = new RocksJdbcColumn[] {
                    new RocksJdbcColumn(1, "id", Types.INTEGER, "INTEGER", 0, 0, false, "NO", null, "getId"),
                    new RocksJdbcColumn(2, "category", Types.VARCHAR, "VARCHAR", 0, 0, true, "NO", null, "getCategory"),
                    new RocksJdbcColumn(3, "amount", Types.BIGINT, "BIGINT", 0, 0, false, "NO", null, "getAmount")
            };

            RocksJdbcDecoder decoder = bytes -> {
                String s = new String(bytes, StandardCharsets.UTF_8);
                String[] parts = s.split(",", -1);
                int id = Integer.parseInt(parts[0]);
                String category = parts[1];
                long amount = Long.parseLong(parts[2]);
                return new ItemRow(id, category, amount);
            };

            this.table = new RocksJdbcTable(
                    "PUBLIC",
                    "items",
                    "items",
                    ItemRow.class,
                    cols,
                    new String[] { "id" },
                    new RocksJdbcIndex[0],
                    decoder,
                    Map.of(),
                    new String[] { "items" }
            );
        }

        @Override
        public List<RocksJdbcTable> tables() {
            return List.of(table);
        }

        @Override
        public RocksJdbcTable table(String name) {
            if (name == null) return null;
            return table.nameMatches(name) ? table : null;
        }

        @Override
        public RocksJdbcTable requireTable(String name) throws SQLException {
            RocksJdbcTable t = table(name);
            if (t != null) return t;
            throw new SQLException("Unknown table: " + name);
        }
    }
}
