package org.github.dbjo.criteria;

import java.io.Serializable;
import java.util.Objects;

public record Or<B extends Serializable>(
        Condition<B> left,
        Condition<B> right
) implements Condition<B> {
    public Or {
        Objects.requireNonNull(left, "left");
        Objects.requireNonNull(right, "right");
    }
}
