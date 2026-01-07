package org.github.dbjo.rdb;

import java.util.*;
import java.util.function.Function;

public final class IndexDef<T> {
    private final String name;
    private final Function<T, Iterable<byte[]>> valueKeys; // raw encoded value keys

    private IndexDef(String name, Function<T, Iterable<byte[]>> valueKeys) {
        this.name = Objects.requireNonNull(name);
        this.valueKeys = Objects.requireNonNull(valueKeys);
    }

    public String name() { return name; }

    public Iterable<byte[]> valueKeysOrEmpty(T entityOrNull) {
        if (entityOrNull == null) return List.of();
        Iterable<byte[]> it = valueKeys.apply(entityOrNull);
        return it != null ? it : List.of();
    }

    /** One value (nullable -> “sparse index”). */
    public static <T, V> IndexDef<T> unique(String name, IndexKeyCodec<V> codec, Function<T, V> extractor) {
        return new IndexDef<>(name, t -> {
            V v = extractor.apply(t);
            if (v == null) return List.of();
            return List.of(codec.encode(v));
        });
    }

    /** Many values (nullable iterable -> empty). */
    public static <T, V> IndexDef<T> multi(String name, IndexKeyCodec<V> codec, Function<T, ? extends Iterable<V>> extractor) {
        return new IndexDef<>(name, t -> {
            Iterable<V> vs = extractor.apply(t);
            if (vs == null) return List.of();
            ArrayList<byte[]> out = new ArrayList<>();
            for (V v : vs) if (v != null) out.add(codec.encode(v));
            return out;
        });
    }
}
