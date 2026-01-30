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
            // values list is typically small
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
        if (c instanceof Like<B> x) {
            Object v = x.prop().get(bean);
            if (v == null) return false;
            return likeMatch(x.prop(), v, x.pattern());
        }

        throw new IllegalArgumentException("Unsupported condition node: " + c.getClass().getName());
    }

    @SuppressWarnings({"unchecked"})
    private static boolean cmpBetween(PropertyMeta<?, ?> prop, Object val, Object lo, Object hi) {
        return compare(prop, val, lo) >= 0 && compare(prop, val, hi) <= 0;
    }

    @SuppressWarnings({"unchecked"})
    private static boolean cmpOp(PropertyMeta<?, ?> prop, Object val, CmpOp op, Object rhs) {
        int d = compare(prop, val, rhs);
        return switch (op) {
            case LT -> d < 0;
            case LE -> d <= 0;
            case GT -> d > 0;
            case GE -> d >= 0;
        };
    }

    @SuppressWarnings({"unchecked"})
    private static int compare(PropertyMeta<?, ?> prop, Object left, Object right) {
        if (left instanceof Number ln && right instanceof Number rn) {
            return toBigDecimal(ln).compareTo(toBigDecimal(rn));
        }
        if (!(left instanceof Comparable c)) {
            throw new IllegalArgumentException("Not comparable at runtime: " + prop.getPropertyName());
        }
        try {
            return c.compareTo(right);
        } catch (ClassCastException ex) {
            throw new IllegalArgumentException(
                    "Not comparable at runtime: " + prop.getPropertyName()
                            + " (" + left.getClass().getSimpleName()
                            + " vs " + (right == null ? "null" : right.getClass().getSimpleName()) + ")",
                    ex);
        }
    }

    private static java.math.BigDecimal toBigDecimal(Number number) {
        return new java.math.BigDecimal(number.toString());
    }

    private static boolean likeMatch(PropertyMeta<?, ?> prop, Object val, String pattern) {
        if (!(val instanceof CharSequence cs)) {
            throw new IllegalArgumentException("LIKE requires CharSequence at runtime: " + prop.getPropertyName());
        }
        String regex = likeToRegex(pattern);
        return cs.toString().matches(regex);
    }

    private static String likeToRegex(String pattern) {
        StringBuilder sb = new StringBuilder("^");
        for (int i = 0; i < pattern.length(); i++) {
            char c = pattern.charAt(i);
            switch (c) {
                case '%' -> sb.append(".*");
                case '_' -> sb.append('.');
                case '\\' -> {
                    if (i + 1 < pattern.length()) {
                        i++;
                        sb.append(java.util.regex.Pattern.quote(String.valueOf(pattern.charAt(i))));
                    } else {
                        sb.append("\\\\");
                    }
                }
                default -> sb.append(java.util.regex.Pattern.quote(String.valueOf(c)));
            }
        }
        sb.append("$");
        return sb.toString();
    }
}
