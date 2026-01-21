package org.github.dbjo.criteria;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import org.github.dbjo.meta.entity.PropertyMeta;

public record In<B extends Serializable, V extends Serializable>(
        PropertyMeta<B, V> prop,
        List<V> values
) implements Condition<B> {
    public In {
        Objects.requireNonNull(prop, "prop");
        Objects.requireNonNull(values, "values");
    }
}
