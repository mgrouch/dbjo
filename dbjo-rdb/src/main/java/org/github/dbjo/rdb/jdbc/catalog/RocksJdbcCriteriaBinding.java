package org.github.dbjo.rdb.jdbc.catalog;

import org.github.dbjo.criteria.PropertyTerm;
import org.github.dbjo.meta.entity.EntityMeta;
import org.github.dbjo.rdb.IndexedRocksDao;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;

/**
 * Per-table wiring for:
 *  - SQL WHERE compilation: column -> PropertyTerm (from *Q)
 *  - criteria Query construction: EntityMeta
 *  - optional index-aware execution: DAO class (expected ctor (RocksSessions, DaoRegistry))
 */
public record RocksJdbcCriteriaBinding<B extends Serializable>(
        RocksJdbcTable table,
        EntityMeta<B> meta,
        Map<String, PropertyTerm<B, ? extends Serializable>> termsByColumnLower,
        @SuppressWarnings("rawtypes") Class<? extends IndexedRocksDao> daoClass
) {
    public RocksJdbcCriteriaBinding {
        Objects.requireNonNull(table, "table");
        Objects.requireNonNull(meta, "meta");
        Objects.requireNonNull(termsByColumnLower, "termsByColumnLower");
        // daoClass may be null (then you still get scan+filter fallback)
    }
}
