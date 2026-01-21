package org.github.dbjo.criteria.eval;

import java.io.Serializable;
import java.util.Objects;
import org.github.dbjo.criteria.*;
import org.github.dbjo.meta.entity.PropertyMeta;

public final class ConditionEvaluator {
    private ConditionEvaluator() {}

    public static <B extends Serializable> boolean test(Condition<B> c, B bean) {
        Objects.requireNonNull(c, "condition");
        Objects.requireNonNull(bean, "bean");

        if (c instanceof TrueCond) return true;
        if (c instanceof FalseCond) return false;

        if (c instanceof And<B> a) return test(a.left(), bean) && test(a.right(), bean);
        if (c instanceof Or<B> o)  return test(o.left(), bean) || test(o.right(), bean);
        if (c instanceof Not<B> n) return !test(n.inner(), bean);

        if (c instanceof IsNull<B, ?> x) {
            return x.prop().get(bean) == null;
        }
        if (c instanceof IsNotNull<B, ?> x) {
            return x.prop().get(bean) != null;
        }
        if (c instanceof Eq<B, ?> x) {
            Object v = x.prop().get(bean);
            return Objects.equals(v, x.value());
        }
        if (c instanceof Ne<B, ?> x) {
            Object v = x.prop().get(bean);
            return !Objects.equals(v, x.value());
        }
        if (c instanceof In<B, ?> x) {
            Object v = x.prop().get(bean);
            if (v == null) return false;
            // values list is typically small; if large you can pre-hash
            return x.values().contains(v);
        }
        if (c instanceof Between<B, ?> x) {
            Object v = x.prop().get(bean);
            if (v == null) return false;
            return cmpBetween(x.prop(), v, x.lo(), x.hi());
        }
        if (c instanceof Cmp<B, ?> x) {
            Object v = x.prop().get(bean);
            if (v == null) return false;
            return cmpOp(x.prop(), v, x.op(), x.value());
        }

        throw new IllegalArgumentException("Unsupported condition node: " + c.getClass().getName());
    }

    @SuppressWarnings({"unchecked"})
    private static boolean cmpBetween(PropertyMeta<?, ?> prop, Object val, Object lo, Object hi) {
        if (!(val instanceof Comparable c)) {
            throw new IllegalArgumentException("Not comparable at runtime: " + prop.getPropertyName());
        }
        return c.compareTo(lo) >= 0 && c.compareTo(hi) <= 0;
    }

    @SuppressWarnings({"unchecked"})
    private static boolean cmpOp(PropertyMeta<?, ?> prop, Object val, CmpOp op, Object rhs) {
        if (!(val instanceof Comparable c)) {
            throw new IllegalArgumentException("Not comparable at runtime: " + prop.getPropertyName());
        }
        int d = c.compareTo(rhs);
        return switch (op) {
            case LT -> d < 0;
            case LE -> d <= 0;
            case GT -> d > 0;
            case GE -> d >= 0;
        };
    }
}
