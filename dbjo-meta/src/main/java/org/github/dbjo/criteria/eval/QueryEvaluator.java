package org.github.dbjo.criteria.eval;

import org.github.dbjo.criteria.Bound;
import org.github.dbjo.criteria.Query;
import org.github.dbjo.criteria.Range;
import org.github.dbjo.criteria.Scan;
import org.github.dbjo.meta.entity.PropertyMeta;

import java.io.Serializable;
import java.util.Objects;

/**
 * Evaluates a {@link Query} against an in-memory bean.
 *
 * <p>This is the “last mile” bridge for runtimes that do not (yet) compile criteria
 * into native storage queries. It applies {@code where} (via {@link ConditionEvaluator})
 * and also applies the {@code scan} bounds (if present) as an additional filter.
 */
public final class QueryEvaluator {
    private QueryEvaluator() {}

    /**
     * Returns {@code true} if {@code bean} matches the query.
     *
     * @throws NullPointerException if {@code q} is null
     * @throws IllegalArgumentException if {@code q.scan().range()} bounds are incompatible with the scanned value
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    public static boolean test(Query<? extends Serializable> q, Serializable bean) {
        Objects.requireNonNull(q, "q");
        if (!ConditionEvaluator.test((org.github.dbjo.criteria.Condition) q.where(), bean)) return false;

        Scan scan = (Scan) q.scan();
        if (scan == null) return true;

        Range range = (Range) scan.range();
        if (range == null) return true;
        if (range.lowerBound() == Bound.UNBOUNDED && range.upperBound() == Bound.UNBOUNDED) return true;

        PropertyMeta prop = (PropertyMeta) scan.prop();
        Object v = prop.get(bean);
        return inRange(v, range);
    }

    /**
     * Raw form: returns {@code true} if the bean matches the query.
     *
     * <p>Useful for DAO bridges where the entity type parameter is not bounded by {@link Serializable}.
     */
    public static boolean testRaw(Query<? extends Serializable> q, Object bean) {
        if (bean == null) return false;
        if (!(bean instanceof Serializable s)) {
            throw new IllegalArgumentException("Criteria evaluation requires entities to implement Serializable: " + bean.getClass().getName());
        }
        return test(q, s);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static boolean inRange(Object v, Range range) {
        if (range == null) return true;
        if (range.lowerBound() == Bound.UNBOUNDED && range.upperBound() == Bound.UNBOUNDED) return true;
        if (v == null) return false;
        if (!(v instanceof Comparable c)) {
            throw new IllegalArgumentException("Scan range requires Comparable values; got: " + v.getClass().getName());
        }

        if (range.lowerBound() != Bound.UNBOUNDED) {
            Object lo = range.lower();
            if (lo == null) return false;
            int cmp = c.compareTo(lo);
            if (cmp < 0) return false;
            if (cmp == 0 && range.lowerBound() == Bound.EXCLUSIVE) return false;
        }

        if (range.upperBound() != Bound.UNBOUNDED) {
            Object hi = range.upper();
            if (hi == null) return false;
            int cmp = c.compareTo(hi);
            if (cmp > 0) return false;
            if (cmp == 0 && range.upperBound() == Bound.EXCLUSIVE) return false;
        }

        return true;
    }
}
