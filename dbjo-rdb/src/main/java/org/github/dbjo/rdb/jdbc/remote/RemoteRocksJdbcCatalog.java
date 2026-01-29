package org.github.dbjo.rdb.jdbc.remote;

import org.github.dbjo.rdb.jdbc.catalog.RocksJdbcCatalog;
import org.github.dbjo.rdb.jdbc.catalog.RocksJdbcTable;

import java.sql.SQLException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

final class RemoteRocksJdbcCatalog implements RocksJdbcCatalog {
    private final List<RocksJdbcTable> tables;
    private final Map<String, RocksJdbcTable> byName;

    RemoteRocksJdbcCatalog(List<RocksJdbcTable> tables) {
        this.tables = List.copyOf(Objects.requireNonNull(tables, "tables"));
        this.byName = tables.stream()
                .collect(java.util.stream.Collectors.toUnmodifiableMap(
                        t -> t.tableName().toLowerCase(Locale.ROOT),
                        t -> t,
                        (a, b) -> a
                ));
    }

    @Override
    public List<RocksJdbcTable> tables() {
        return tables;
    }

    @Override
    public RocksJdbcTable table(String name) {
        if (name == null) return null;
        RocksJdbcTable direct = byName.get(name.toLowerCase(Locale.ROOT));
        if (direct != null) return direct;
        for (RocksJdbcTable t : tables) {
            if (t.nameMatches(name)) return t;
        }
        return null;
    }

    @Override
    public RocksJdbcTable requireTable(String name) throws SQLException {
        RocksJdbcTable table = table(name);
        if (table == null) throw new SQLException("Unknown table: " + name);
        return table;
    }
}
