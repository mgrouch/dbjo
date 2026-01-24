package org.github.dbjo.meta.jdbc;

import java.util.Objects;
import java.util.function.Function;

public final class EnumJdbcCodec<E, K> implements JdbcCodec<E> {
    private final Function<E, K> keyGetter;

    public EnumJdbcCodec(Function<E, K> keyGetter) {
        this.keyGetter = Objects.requireNonNull(keyGetter, "keyGetter");
    }

    @Override
    public Object toJdbc(E v) {
        return v == null ? null : keyGetter.apply(v);
    }
}
