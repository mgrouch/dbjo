package org.github.dbjo.criteria;

import java.io.Serializable;
import java.util.Objects;
import org.github.dbjo.meta.entity.PropertyMeta;

public record Ne<B extends Serializable, V extends Serializable>(
        PropertyMeta<B, V> prop,
        V value
) implements Condition<B> {
    public Ne { Objects.requireNonNull(prop, "prop"); }
}
