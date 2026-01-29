package org.github.dbjo.criteria;

import java.io.Serializable;
import java.util.Collection;
import java.util.Objects;
import org.github.dbjo.meta.entity.PropertyMeta;

public record PropertyTerm<B extends Serializable, V extends Serializable>(
        PropertyMeta<B, V> prop
) {
    public PropertyTerm { Objects.requireNonNull(prop, "prop"); }

    public Condition<B> eq(V value) { return Conditions.eq(prop, value); }
    public Condition<B> ne(V value) { return Conditions.ne(prop, value); }

    public Condition<B> isNull() { return Conditions.isNull(prop); }
    public Condition<B> isNotNull() { return Conditions.isNotNull(prop); }

    public Condition<B> in(Collection<? extends V> values) { return Conditions.in(prop, values); }

    @SafeVarargs
    public final Condition<B> in(V... values) { return Conditions.in(prop, values); }

    // ordered/range (runtime comparable checks)
    public Condition<B> between(V lo, V hi) { return Conditions.between(prop, lo, hi); }
    public Condition<B> lt(V v) { return Conditions.lt(prop, v); }
    public Condition<B> le(V v) { return Conditions.le(prop, v); }
    public Condition<B> gt(V v) { return Conditions.gt(prop, v); }
    public Condition<B> ge(V v) { return Conditions.ge(prop, v); }

    @SuppressWarnings("unchecked")
    public Condition<B> like(String pattern) { return Conditions.like((PropertyMeta<B, String>) prop, pattern); }
}
