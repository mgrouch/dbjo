package org.github.dbjo.rdb;

import java.util.Collection;
import java.util.List;

public interface RocksSchema {
    Collection<String> columnFamilies();

    static RocksSchema of(String... cfs) {
        return () -> List.of(cfs);
    }
}
