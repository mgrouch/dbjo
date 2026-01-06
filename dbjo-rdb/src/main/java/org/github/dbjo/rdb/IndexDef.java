package org.github.dbjo.rdb;

import org.rocksdb.ColumnFamilyHandle;

import java.util.List;
import java.util.Objects;
import java.util.function.Function;

public record IndexDef<T>(
        String name,
        ColumnFamilyHandle cf,
        Function<T, List<byte[]>> valueBytesExtractor // can return 0..N values (multi-valued index)
) {
    public IndexDef {
        Objects.requireNonNull(name);
        Objects.requireNonNull(cf);
        Objects.requireNonNull(valueBytesExtractor);
    }

    public static <T> IndexDef<T> unique(String name, ColumnFamilyHandle cf, Function<T, byte[]> extractor) {
        return new IndexDef<>(name, cf, t -> {
            byte[] v = extractor.apply(t);
            return (v == null) ? List.of() : List.of(v);
        });
    }
}
