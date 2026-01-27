package org.github.dbjo.rdb.criteria;

import org.github.dbjo.criteria.*;
import org.github.dbjo.criteria.eval.ConditionEvaluator;

import java.io.Serializable;

@SuppressWarnings({"rawtypes", "unchecked"})
public final class CriteriaSupport {
    private CriteriaSupport() {}

    /** Null limit -> MAX_VALUE. */
    public static int limitOrMax(Query<?> q) {
        if (q == null) return Integer.MAX_VALUE;
        Integer lim = q.limit();
        return (lim == null || lim <= 0) ? Integer.MAX_VALUE : lim;
    }

    /** Evaluate full criteria (where + scan) against a bean instance. */
    public static boolean test(Query<? extends Serializable> q, Object bean) {
        if (q == null) return true;
        if (bean == null) return false;
        if (!(bean instanceof Serializable s)) return false;

        // where
        if (q.where() != null) {
            if (!ConditionEvaluator.test((org.github.dbjo.criteria.Condition) q.where(), s)) return false;
        }

        // scan
        Scan scan = q.scan();
        if (scan != null) {
            Object v = scan.prop().get(s);
            return within(scan.range(), v);
        }

        return true;
    }

    /** Range check matching dbjo-meta Range<lower,Bound,upper,Bound>. */
    public static boolean within(Range range, Object value) {
        if (range == null) return true;
        if (range.lowerBound() == Bound.UNBOUNDED && range.upperBound() == Bound.UNBOUNDED) return true;

        if (value == null) return false;
        if (!(value instanceof Comparable cVal)) {
            throw new IllegalArgumentException("Scan value is not Comparable: " + value.getClass().getName());
        }

        if (range.lowerBound() != Bound.UNBOUNDED) {
            Object lo = range.lower(); // guaranteed non-null by Range ctor
            int cmp = cVal.compareTo(lo);
            if (cmp < 0) return false;
            if (cmp == 0 && range.lowerBound() == Bound.EXCLUSIVE) return false;
        }

        if (range.upperBound() != Bound.UNBOUNDED) {
            Object hi = range.upper(); // guaranteed non-null by Range ctor
            int cmp = cVal.compareTo(hi);
            if (cmp > 0) return false;
            return cmp != 0 || range.upperBound() != Bound.EXCLUSIVE;
        }

        return true;
    }
}
