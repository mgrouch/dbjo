package org.github.dbjo.rdb.jdbc.catalog;

public interface RocksJdbcCriteriaCatalog extends RocksJdbcCatalog {
    RocksJdbcCriteriaBinding<?> bindingFor(String tableName);
}
