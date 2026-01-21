package org.github.dbjo.criteria;

import java.io.Serializable;
import java.util.Objects;
import org.github.dbjo.meta.entity.PropertyMeta;

public record IsNotNull<B extends Serializable, V extends Serializable>(
        PropertyMeta<B, V> prop
) implements Condition<B> {
    public IsNotNull { Objects.requireNonNull(prop, "prop"); }
}
