package org.github.dbjo.criteria;

import java.io.Serializable;
import java.util.Objects;
import org.github.dbjo.meta.entity.PropertyMeta;

public record Like<B extends Serializable>(
        PropertyMeta<B, String> prop,
        String pattern
) implements Condition<B> {
    public Like {
        Objects.requireNonNull(prop, "prop");
        Objects.requireNonNull(pattern, "pattern");
    }
}
