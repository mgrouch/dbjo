package org.github.dbjo.criteria;

import java.io.Serializable;
import java.util.Objects;
import org.github.dbjo.meta.entity.PropertyMeta;

/** Inclusive between by definition here. */
public record Between<B extends Serializable, V extends Serializable>(
        PropertyMeta<B, V> prop,
        V lo,
        V hi
) implements Condition<B> {
    public Between {
        Objects.requireNonNull(prop, "prop");
        Objects.requireNonNull(lo, "lo");
        Objects.requireNonNull(hi, "hi");
    }
}
