package org.github.dbjo.rdb.jdbc.remote;

import org.github.dbjo.rdb.jdbc.catalog.RocksJdbcCatalog;
import org.github.dbjo.rdb.jdbc.catalog.RocksJdbcColumn;
import org.github.dbjo.rdb.jdbc.catalog.RocksJdbcIndex;
import org.github.dbjo.rdb.jdbc.catalog.RocksJdbcTable;
import org.github.dbjo.rdb.jdbc.remote.dto.RemoteRocksJdbcCatalogDto;
import org.github.dbjo.rdb.jdbc.remote.dto.RemoteRocksJdbcColumnDto;
import org.github.dbjo.rdb.jdbc.remote.dto.RemoteRocksJdbcIndexDto;
import org.github.dbjo.rdb.jdbc.remote.dto.RemoteRocksJdbcTableDto;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public final class RemoteRocksJdbcCatalogMapper {
    private RemoteRocksJdbcCatalogMapper() {}

    public static RemoteRocksJdbcCatalogDto fromCatalog(RocksJdbcCatalog catalog) {
        List<RemoteRocksJdbcTableDto> tables = catalog.tables().stream()
                .map(RemoteRocksJdbcCatalogMapper::toDto)
                .toList();
        return new RemoteRocksJdbcCatalogDto(tables);
    }

    public static RocksJdbcCatalog toCatalog(RemoteRocksJdbcCatalogDto dto) {
        return new RemoteRocksJdbcCatalog(dto.tables().stream()
                .map(RemoteRocksJdbcCatalogMapper::fromDto)
                .toList());
    }

    private static RemoteRocksJdbcTableDto toDto(RocksJdbcTable table) {
        List<RemoteRocksJdbcColumnDto> columns = Arrays.stream(table.columns())
                .map(RemoteRocksJdbcCatalogMapper::toDto)
                .toList();
        List<RemoteRocksJdbcIndexDto> indexes = Arrays.stream(table.indexes())
                .map(RemoteRocksJdbcCatalogMapper::toDto)
                .toList();
        return new RemoteRocksJdbcTableDto(
                table.schemaName(),
                table.tableName(),
                table.cfName(),
                columns,
                List.of(table.pkColumns()),
                indexes,
                table.names()
        );
    }

    private static RemoteRocksJdbcColumnDto toDto(RocksJdbcColumn column) {
        return new RemoteRocksJdbcColumnDto(
                column.pos(),
                column.name(),
                column.sqlType(),
                column.typeName(),
                column.size(),
                column.scale(),
                column.nullableBool(),
                column.isAutoIncrement(),
                column.defaultValue(),
                column.getterName()
        );
    }

    private static RemoteRocksJdbcIndexDto toDto(RocksJdbcIndex index) {
        return new RemoteRocksJdbcIndexDto(index.indexName(), index.unique(), List.of(index.columnNames()));
    }

    private static RocksJdbcTable fromDto(RemoteRocksJdbcTableDto dto) {
        RocksJdbcColumn[] columns = dto.columns().stream()
                .map(RemoteRocksJdbcCatalogMapper::fromDto)
                .toArray(RocksJdbcColumn[]::new);
        RocksJdbcIndex[] indexes = dto.indexes().stream()
                .map(RemoteRocksJdbcCatalogMapper::fromDto)
                .toArray(RocksJdbcIndex[]::new);
        String[] names = dto.names().toArray(new String[0]);
        String[] pkColumns = dto.pkColumns().toArray(new String[0]);
        return new RocksJdbcTable(
                dto.schemaName(),
                dto.tableName(),
                dto.cfName(),
                Object.class,
                columns,
                pkColumns,
                indexes,
                bytes -> null,
                Map.of(),
                names
        );
    }

    private static RocksJdbcColumn fromDto(RemoteRocksJdbcColumnDto dto) {
        return new RocksJdbcColumn(
                dto.pos(),
                dto.name(),
                dto.sqlType(),
                dto.typeName(),
                dto.size(),
                dto.scale(),
                dto.nullable(),
                dto.isAutoIncrement(),
                dto.defaultValue(),
                dto.getterName()
        );
    }

    private static RocksJdbcIndex fromDto(RemoteRocksJdbcIndexDto dto) {
        return new RocksJdbcIndex(dto.indexName(), dto.unique(), dto.columnNames().toArray(new String[0]));
    }
}
