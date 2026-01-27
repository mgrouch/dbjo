package org.github.dbjo.rdb;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/**
 * Defines a secondary index for a RocksDB-backed entity.
 *
 * Index keys are stored as:
 *    unique( escaped(valueBytes) + SEP + pkBytes )
 *
 * So both "unique" and "multi" indexes can be scanned by value prefix.
 *
 * For criteria pushdown we also optionally carry:
 *  - propertyName: Java bean property name (matching PropertyMeta#getPropertyName())
 *  - codec: how to encode that property's value into valueBytes (order-preserving for ranges)
 */
public final class IndexDef<T, V> {

    public enum Kind { UNIQUE, MULTI }

    private final String name;
    private final String propertyName; // nullable
    private final Kind kind;
    private final IndexKeyCodec<V> codec;

    // Produces encoded value-bytes (NOT including pk); maintainIndexes will append pk via IndexKeys.unique(...)
    private final Function<T, Iterable<byte[]>> valueKeys;

    private IndexDef(String name,
                     String propertyName,
                     Kind kind,
                     IndexKeyCodec<V> codec,
                     Function<T, Iterable<byte[]>> valueKeys) {
        this.name = Objects.requireNonNull(name, "name");
        this.propertyName = (propertyName == null || propertyName.isBlank()) ? null : propertyName;
        this.kind = Objects.requireNonNull(kind, "kind");
        this.codec = Objects.requireNonNull(codec, "codec");
        this.valueKeys = Objects.requireNonNull(valueKeys, "valueKeys");
    }

    public String name() { return name; }

    /** Nullable; when present enables criteria pushdown by property name. */
    public String propertyName() { return propertyName; }

    public Kind kind() { return kind; }

    public IndexKeyCodec<V> codec() { return codec; }

    /** Encodes a typed value to raw value-bytes (may return null if value is null). */
    public byte[] encodeValueOrNull(V value) {
        return value == null ? null : codec.encode(value);
    }

    /**
     * Encodes an arbitrary object as this index's value type.
     * Used by criteria pushdown where the AST value arrives as Object/Serializable.
     */
    @SuppressWarnings("unchecked")
    public byte[] encodeAnyOrNull(Object value) {
        if (value == null) return null;
        return codec.encode((V) value);
    }

    public Iterable<byte[]> valueKeysOrEmpty(T valueOrNull) {
        if (valueOrNull == null) return Collections.emptyList();
        Iterable<byte[]> it = valueKeys.apply(valueOrNull);
        return it == null ? Collections.emptyList() : it;
    }

    // ---------------- factories (backwards compatible) ----------------

    public static <T, V> IndexDef<T, V> unique(
            String name,
            IndexKeyCodec<V> codec,
            Function<T, V> extractor
    ) {
        return unique(name, null, codec, extractor);
    }

    public static <T, V> IndexDef<T, V> unique(
            String name,
            String propertyName,
            IndexKeyCodec<V> codec,
            Function<T, V> extractor
    ) {
        Objects.requireNonNull(extractor, "extractor");
        Function<T, Iterable<byte[]>> keys = t -> {
            V v = extractor.apply(t);
            if (v == null) return List.of();
            byte[] b = codec.encode(v);
            return b == null ? List.of() : List.of(b);
        };
        return new IndexDef<>(name, propertyName, Kind.UNIQUE, codec, keys);
    }

    public static <T, V> IndexDef<T, V> multi(
            String name,
            IndexKeyCodec<V> codec,
            Function<T, ? extends Iterable<V>> extractor
    ) {
        return multi(name, null, codec, extractor);
    }

    public static <T, V> IndexDef<T, V> multi(
            String name,
            String propertyName,
            IndexKeyCodec<V> codec,
            Function<T, ? extends Iterable<V>> extractor
    ) {
        Objects.requireNonNull(extractor, "extractor");
        Function<T, Iterable<byte[]>> keys = t -> {
            Iterable<V> vs = extractor.apply(t);
            if (vs == null) return List.of();
            // encode lazily by materializing to a simple list
            java.util.ArrayList<byte[]> out = new java.util.ArrayList<>();
            for (V v : vs) {
                if (v == null) continue;
                byte[] b = codec.encode(v);
                if (b != null) out.add(b);
            }
            return out;
        };
        return new IndexDef<>(name, propertyName, Kind.MULTI, codec, keys);
    }
}
