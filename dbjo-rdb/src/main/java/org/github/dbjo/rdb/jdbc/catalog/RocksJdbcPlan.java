package org.github.dbjo.rdb.jdbc.catalog;

import java.io.Serializable;
import java.util.List;

public interface RocksJdbcPlan {

    record ListTables(Integer limit) implements RocksJdbcPlan {}

    record Select(
            String table,
            List<String> projection,   // null => "*"
            Integer limit,
            AccessPath accessPath,
            String whereSql
    ) implements RocksJdbcPlan {}

    record Count(
            String table,
            Integer limit,
            AccessPath accessPath,
            String whereSql
    ) implements RocksJdbcPlan {}

    interface AccessPath {}

    record FullScan() implements AccessPath {}

    record IndexEq(String indexName, List<String> indexCols, List<Serializable> prefixValues) implements AccessPath {}

    record IndexRange(String indexName, List<String> indexCols, List<Serializable> eqPrefixValues,
                      Serializable rangeLo, Serializable rangeHi) implements AccessPath {}

    record IndexIn(String indexName, List<String> indexCols, List<Serializable> eqPrefixValues,
                   List<Serializable> inValues) implements AccessPath {}
}
