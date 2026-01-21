package org.github.dbjo.criteria;

import java.io.Serializable;
import java.util.*;
import org.github.dbjo.meta.entity.PropertyMeta;

public final class Conditions {
    private Conditions() {}

    private static final TrueCond<?> TRUE = new TrueCond<>();
    private static final FalseCond<?> FALSE = new FalseCond<>();

    @SuppressWarnings("unchecked")
    public static <B extends Serializable> Condition<B> trueCondition() { return (Condition<B>) TRUE; }

    @SuppressWarnings("unchecked")
    public static <B extends Serializable> Condition<B> falseCondition() { return (Condition<B>) FALSE; }

    public static <B extends Serializable, V extends Serializable> Condition<B> eq(PropertyMeta<B, V> prop, V value) {
        Objects.requireNonNull(prop, "prop");
        if (value == null) return new IsNull<>(prop);
        return new Eq<>(prop, value);
    }

    public static <B extends Serializable, V extends Serializable> Condition<B> ne(PropertyMeta<B, V> prop, V value) {
        Objects.requireNonNull(prop, "prop");
        if (value == null) return new IsNotNull<>(prop);
        return new Ne<>(prop, value);
    }

    public static <B extends Serializable, V extends Serializable> Condition<B> isNull(PropertyMeta<B, V> prop) {
        return new IsNull<>(Objects.requireNonNull(prop, "prop"));
    }

    public static <B extends Serializable, V extends Serializable> Condition<B> isNotNull(PropertyMeta<B, V> prop) {
        return new IsNotNull<>(Objects.requireNonNull(prop, "prop"));
    }

    public static <B extends Serializable, V extends Serializable> Condition<B> in(PropertyMeta<B, V> prop, Collection<? extends V> values) {
        Objects.requireNonNull(prop, "prop");
        if (values == null || values.isEmpty()) return falseCondition();

        var list = new ArrayList<V>(values.size());
        for (V v : values) {
            if (v == null) {
                throw new IllegalArgumentException("IN(...) does not accept null values. prop=" + prop.getPropertyName());
            }
            list.add(v);
        }
        return new In<>(prop, List.copyOf(list));
    }

    @SafeVarargs
    public static <B extends Serializable, V extends Serializable> Condition<B> in(PropertyMeta<B, V> prop, V... values) {
        Objects.requireNonNull(prop, "prop");
        if (values == null || values.length == 0) return falseCondition();
        return in(prop, Arrays.asList(values));
    }

    public static <B extends Serializable, V extends Serializable> Condition<B> between(PropertyMeta<B, V> prop, V lo, V hi) {
        Objects.requireNonNull(prop, "prop");
        Objects.requireNonNull(lo, "lo");
        Objects.requireNonNull(hi, "hi");
        requireComparable(prop);
        return new Between<>(prop, lo, hi);
    }

    public static <B extends Serializable, V extends Serializable> Condition<B> lt(PropertyMeta<B, V> prop, V v) { return cmp(prop, CmpOp.LT, v); }
    public static <B extends Serializable, V extends Serializable> Condition<B> le(PropertyMeta<B, V> prop, V v) { return cmp(prop, CmpOp.LE, v); }
    public static <B extends Serializable, V extends Serializable> Condition<B> gt(PropertyMeta<B, V> prop, V v) { return cmp(prop, CmpOp.GT, v); }
    public static <B extends Serializable, V extends Serializable> Condition<B> ge(PropertyMeta<B, V> prop, V v) { return cmp(prop, CmpOp.GE, v); }

    public static <B extends Serializable, V extends Serializable> Condition<B> cmp(PropertyMeta<B, V> prop, CmpOp op, V value) {
        Objects.requireNonNull(prop, "prop");
        Objects.requireNonNull(op, "op");
        Objects.requireNonNull(value, "value");
        requireComparable(prop);
        return new Cmp<>(prop, op, value);
    }

    public static <B extends Serializable> Condition<B> and(Condition<B> a, Condition<B> b) {
        Objects.requireNonNull(a, "a");
        Objects.requireNonNull(b, "b");
        if (a instanceof FalseCond || b instanceof FalseCond) return falseCondition();
        if (a instanceof TrueCond) return b;
        if (b instanceof TrueCond) return a;
        return new And<>(a, b);
    }

    public static <B extends Serializable> Condition<B> or(Condition<B> a, Condition<B> b) {
        Objects.requireNonNull(a, "a");
        Objects.requireNonNull(b, "b");
        if (a instanceof TrueCond || b instanceof TrueCond) return trueCondition();
        if (a instanceof FalseCond) return b;
        if (b instanceof FalseCond) return a;
        return new Or<>(a, b);
    }

    private static <B extends Serializable, V extends Serializable> void requireComparable(PropertyMeta<B, V> prop) {
        var cls = prop.getPropertyClass();
        if (!Comparable.class.isAssignableFrom(cls)) {
            throw new IllegalArgumentException("Ordered operation requires Comparable property type: " +
                    prop.getPropertyName() + " type=" + cls.getName());
        }
    }
}
