package org.github.dbjo.rdb.jdbc;

import org.github.dbjo.rdb.jdbc.catalog.RocksJdbcCatalog;
import org.github.dbjo.rdb.jdbc.catalog.RocksJdbcColumn;
import org.github.dbjo.rdb.jdbc.catalog.RocksJdbcIndex;
import org.github.dbjo.rdb.jdbc.catalog.RocksJdbcTable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

final class RocksJdbcDatabaseMetaDataTest {

    @TempDir
    Path dir;

    @Test
    void getSchemasListsDistinctSchemas() throws Exception {
        try (Connection conn = openConnection(new SchemaCatalog())) {
            DatabaseMetaData metaData = conn.getMetaData();
            List<String> schemas = new ArrayList<>();
            List<String> catalogs = new ArrayList<>();

            try (ResultSet rs = metaData.getSchemas()) {
                while (rs.next()) {
                    schemas.add(rs.getString("TABLE_SCHEM"));
                    catalogs.add(rs.getString("TABLE_CATALOG"));
                }
            }

            assertEquals(List.of("analytics", "PUBLIC"), schemas);
            catalogs.forEach(catalog -> assertNull(catalog));
        }
    }

    @Test
    void getSchemasWithPatternFiltersResults() throws Exception {
        try (Connection conn = openConnection(new SchemaCatalog())) {
            DatabaseMetaData metaData = conn.getMetaData();
            List<String> schemas = new ArrayList<>();

            try (ResultSet rs = metaData.getSchemas(null, "pub%")) {
                while (rs.next()) {
                    schemas.add(rs.getString(1));
                }
            }

            assertEquals(List.of("PUBLIC"), schemas);
        }
    }

    private Connection openConnection(RocksJdbcCatalog catalog) throws SQLException {
        RocksJdbcDriver driver = new RocksJdbcDriver(catalog);
        Properties props = new Properties();
        props.setProperty("rebuildIndexes", "false");
        return driver.connect("jdbc:dbjo-rocks:" + dir, props);
    }

    private static final class SampleRow {
        private final int id;

        private SampleRow(int id) {
            this.id = id;
        }

        public int getId() { return id; }
    }

    private static final class SchemaCatalog implements RocksJdbcCatalog {
        private final List<RocksJdbcTable> tables;

        private SchemaCatalog() {
            RocksJdbcColumn[] cols = new RocksJdbcColumn[] {
                    new RocksJdbcColumn(1, "id", Types.INTEGER, "INTEGER", 0, 0, false, "NO", null, "getId")
            };

            RocksJdbcTable publicTable = new RocksJdbcTable(
                    "PUBLIC",
                    "items",
                    "items",
                    SampleRow.class,
                    cols,
                    new String[] { "id" },
                    new RocksJdbcIndex[0],
                    bytes -> new SampleRow(Integer.parseInt(new String(bytes, StandardCharsets.UTF_8))),
                    Map.of(),
                    new String[] { "items" }
            );

            RocksJdbcTable analyticsTable = new RocksJdbcTable(
                    "analytics",
                    "audit",
                    "audit",
                    SampleRow.class,
                    cols,
                    new String[] { "id" },
                    new RocksJdbcIndex[0],
                    bytes -> new SampleRow(Integer.parseInt(new String(bytes, StandardCharsets.UTF_8))),
                    Map.of(),
                    new String[] { "audit" }
            );

            RocksJdbcTable duplicateSchemaTable = new RocksJdbcTable(
                    "public",
                    "extra",
                    "extra",
                    SampleRow.class,
                    cols,
                    new String[] { "id" },
                    new RocksJdbcIndex[0],
                    bytes -> new SampleRow(Integer.parseInt(new String(bytes, StandardCharsets.UTF_8))),
                    Map.of(),
                    new String[] { "extra" }
            );

            this.tables = List.of(publicTable, analyticsTable, duplicateSchemaTable);
        }

        @Override
        public List<RocksJdbcTable> tables() {
            return tables;
        }

        @Override
        public RocksJdbcTable table(String name) {
            if (name == null) return null;
            for (RocksJdbcTable table : tables) {
                if (table.nameMatches(name)) return table;
            }
            return null;
        }

        @Override
        public RocksJdbcTable requireTable(String name) throws SQLException {
            RocksJdbcTable t = table(name);
            if (t != null) return t;
            throw new SQLException("Unknown table: " + name);
        }
    }
}
