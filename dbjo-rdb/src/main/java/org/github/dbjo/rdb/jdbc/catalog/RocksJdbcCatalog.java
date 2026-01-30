package org.github.dbjo.rdb.jdbc.catalog;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

/**
 * Metadata-only catalog: generated code implements this.
 *
 * Execution is performed by RocksJdbcEngine (which uses this catalog + RocksDB).
 */
public interface RocksJdbcCatalog {
    List<RocksJdbcTable> tables();

    RocksJdbcTable table(String name);

    RocksJdbcTable requireTable(String name) throws SQLException;

    default List<String> schemaNames() {
        Set<String> names = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        for (RocksJdbcTable table : tables()) {
            String schema = table.schemaName();
            if (schema == null || schema.isBlank()) continue;
            names.add(schema);
        }
        if (names.isEmpty()) {
            names.add("PUBLIC");
        }
        return List.copyOf(names);
    }

    /**
     * Table meta for DatabaseMetaData / tooling.
     * Uses RocksJdbcTable accessors that actually exist:
     *  - columns()
     *  - pkColumns()
     *  - indexes()
     */
    default RocksJdbcTableMeta tableMeta(String name) throws SQLException {
        RocksJdbcTable t = requireTable(name);

        ArrayList<String> cols = new ArrayList<>();
        for (RocksJdbcColumn c : t.columns()) {
            cols.add(c.name());
        }

        ArrayList<RocksJdbcTableMeta.IndexMeta> idx = new ArrayList<>();

        // Primary key as a "PRIMARY" index meta (best-effort)
        List<String> pk = List.of(t.pkColumns());
        if (!pk.isEmpty()) {
            idx.add(new RocksJdbcTableMeta.IndexMeta("PRIMARY", pk, true, true));
        }

        for (RocksJdbcIndex ix : t.indexes()) {
            if (ix == null) continue;
            idx.add(new RocksJdbcTableMeta.IndexMeta(
                    Objects.requireNonNullElse(ix.indexName(), "IDX"),
                    List.of(ix.columnNames()),
                    ix.unique(),
                    false
            ));
        }

        return new RocksJdbcTableMeta(t.tableName(), cols, idx);
    }
}
