package org.github.dbjo.criteria;

import java.io.Serializable;
import java.util.Objects;
import org.github.dbjo.meta.entity.PropertyMeta;

public record Scan<B extends Serializable, V extends Serializable>(
        PropertyMeta<B, V> prop,
        Range<V> range
) {
    public Scan {
        Objects.requireNonNull(prop, "prop");
        Objects.requireNonNull(range, "range");
    }
}
