package org.github.dbjo.meta.jdbc;

public interface JdbcCodec<V> {
    Object toJdbc(V v);
}
