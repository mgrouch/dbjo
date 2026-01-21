package org.github.dbjo.criteria;

import java.io.Serializable;
import java.util.Objects;
import org.github.dbjo.meta.entity.PropertyMeta;

public record Cmp<B extends Serializable, V extends Serializable>(
        PropertyMeta<B, V> prop,
        CmpOp op,
        V value
) implements Condition<B> {
    public Cmp {
        Objects.requireNonNull(prop, "prop");
        Objects.requireNonNull(op, "op");
        Objects.requireNonNull(value, "value");
    }
}
