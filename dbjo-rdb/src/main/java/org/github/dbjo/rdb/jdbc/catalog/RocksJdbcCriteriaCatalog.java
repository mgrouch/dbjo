package org.github.dbjo.rdb.jdbc.catalog;

/**
 * Optional catalog extension: provides dbjo-criteria bindings per table,
 * enabling WHERE compilation and index-aware fast path (via IndexedRocksDao.select()).
 */
public interface RocksJdbcCriteriaCatalog extends RocksJdbcCatalog {

    /**
     * Find binding by table name or alias (case-insensitive).
     * Common keys you should support: logical table name + cf name.
     */
    RocksJdbcCriteriaBinding<?> bindingFor(String tableNameOrAlias);
}
