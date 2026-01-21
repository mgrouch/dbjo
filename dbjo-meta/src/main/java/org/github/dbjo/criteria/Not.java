package org.github.dbjo.criteria;

import java.io.Serializable;
import java.util.Objects;

public record Not<B extends Serializable>(
        Condition<B> inner
) implements Condition<B> {
    public Not { Objects.requireNonNull(inner, "inner"); }
}
