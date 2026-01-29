package org.github.dbjo.rdb;

import java.util.Collection;
import java.util.List;

public interface RocksSchema<T> {
    Collection<String> columnFamilies();

    static <T> RocksSchema<T> of(String... cfs) {
        return () -> List.of(cfs);
    }
}
