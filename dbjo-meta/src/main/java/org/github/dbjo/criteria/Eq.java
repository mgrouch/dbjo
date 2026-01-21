package org.github.dbjo.criteria;

import java.io.Serializable;
import java.util.Objects;
import org.github.dbjo.meta.entity.PropertyMeta;

public record Eq<B extends Serializable, V extends Serializable>(
        PropertyMeta<B, V> prop,
        V value
) implements Condition<B> {
    public Eq { Objects.requireNonNull(prop, "prop"); }
}
