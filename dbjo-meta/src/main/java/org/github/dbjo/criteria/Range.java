package org.github.dbjo.criteria;

import java.io.Serializable;
import java.util.Objects;

public record Range<V extends Serializable>(
        V lower,
        Bound lowerBound,
        V upper,
        Bound upperBound
) {
    public Range {
        Objects.requireNonNull(lowerBound, "lowerBound");
        Objects.requireNonNull(upperBound, "upperBound");
        if (lowerBound != Bound.UNBOUNDED && lower == null) {
            throw new IllegalArgumentException("lower cannot be null when lowerBound=" + lowerBound);
        }
        if (upperBound != Bound.UNBOUNDED && upper == null) {
            throw new IllegalArgumentException("upper cannot be null when upperBound=" + upperBound);
        }
    }

    public static <V extends Serializable> Range<V> closedOpen(V lo, V hi) {
        return new Range<>(lo, Bound.INCLUSIVE, hi, Bound.EXCLUSIVE);
    }

    public static <V extends Serializable> Range<V> atLeast(V lo) {
        return new Range<>(lo, Bound.INCLUSIVE, null, Bound.UNBOUNDED);
    }

    public static <V extends Serializable> Range<V> all() {
        return new Range<>(null, Bound.UNBOUNDED, null, Bound.UNBOUNDED);
    }
}
