package org.github.dbjo.rdb.jdbc.remote;

import org.github.dbjo.rdb.jdbc.catalog.RocksJdbcCatalog;
import org.github.dbjo.rdb.jdbc.catalog.RocksJdbcColumn;
import org.github.dbjo.rdb.jdbc.catalog.RocksJdbcIndex;
import org.github.dbjo.rdb.jdbc.catalog.RocksJdbcTable;
import org.github.dbjo.rdb.jdbc.remote.dto.RemoteRocksJdbcCatalogDto;
import org.junit.jupiter.api.Test;

import java.sql.Types;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class RemoteRocksJdbcCatalogMapperTest {

    @Test
    void roundTripsCatalogMetadata() throws Exception {
        RocksJdbcColumn id = new RocksJdbcColumn(1, "id", Types.INTEGER, "INTEGER", 0, 0, false, "NO", null, "getId");
        RocksJdbcColumn name = new RocksJdbcColumn(2, "name", Types.VARCHAR, "VARCHAR", 0, 0, true, "NO", null, "getName");
        RocksJdbcIndex index = new RocksJdbcIndex("idx_name", false, new String[]{"name"});
        RocksJdbcTable table = new RocksJdbcTable(
                "PUBLIC",
                "client",
                "client_cf",
                Object.class,
                new RocksJdbcColumn[]{id, name},
                new String[]{"id"},
                new RocksJdbcIndex[]{index},
                bytes -> null,
                Map.of(),
                new String[]{"client", "CLIENT"}
        );
        RocksJdbcCatalog catalog = new RocksJdbcCatalog() {
            @Override public List<RocksJdbcTable> tables() { return List.of(table); }
            @Override public RocksJdbcTable table(String name) { return table.nameMatches(name) ? table : null; }
            @Override public RocksJdbcTable requireTable(String name) { return table; }
        };

        RemoteRocksJdbcCatalogDto dto = RemoteRocksJdbcCatalogMapper.fromCatalog(catalog);
        RocksJdbcCatalog restored = RemoteRocksJdbcCatalogMapper.toCatalog(dto);

        assertThat(restored.tables()).hasSize(1);
        RocksJdbcTable restoredTable = restored.tables().getFirst();
        assertThat(restoredTable.tableName()).isEqualTo("client");
        assertThat(restoredTable.cfName()).isEqualTo("client_cf");
        assertThat(restoredTable.pkColumns()).containsExactly("id");
        assertThat(restoredTable.columns()).extracting(RocksJdbcColumn::name)
                .containsExactly("id", "name");
        assertThat(restoredTable.indexes()).hasSize(1);
        assertThat(restoredTable.indexes()[0].indexName()).isEqualTo("idx_name");
    }
}
