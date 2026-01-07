package org.github.dbjo.rdb;

import java.util.Optional;

public record KeyRange<K>(K from, boolean fromInclusive, K to, boolean toInclusive) {

    /** Convenience: Optional bounds, inclusive by default. */
    public KeyRange(Optional<? extends K> from, Optional<? extends K> to) {
        this(from.orElse(null), true, to.orElse(null), true);
    }

    /** Convenience: Optional bounds with explicit inclusivity. */
    public KeyRange(Optional<? extends K> from, boolean fromInclusive,
                    Optional<? extends K> to,   boolean toInclusive) {
        this(from.orElse(null), fromInclusive, to.orElse(null), toInclusive);
    }

    // Optional nice factories
    public static <K> KeyRange<K> betweenInclusive(K from, K to) {
        return new KeyRange<>(from, true, to, true);
    }

    public static <K> KeyRange<K> closedOpen(K fromInclusive, K toExclusive) {
        return new KeyRange<>(fromInclusive, true, toExclusive, false);
    }

    public static <K> KeyRange<K> all() {
        return new KeyRange<>(null, true, null, true);
    }
}
