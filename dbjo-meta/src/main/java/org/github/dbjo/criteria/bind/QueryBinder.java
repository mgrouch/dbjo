package org.github.dbjo.criteria.bind;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.github.dbjo.criteria.*;
import org.github.dbjo.criteria.spec.*;
import org.github.dbjo.meta.entity.EntityMeta;
import org.github.dbjo.meta.entity.PropertyMeta;

public final class QueryBinder {

    private final MetaRegistry registry;

    public QueryBinder(MetaRegistry registry) {
        this.registry = Objects.requireNonNull(registry, "registry");
    }

    public <B extends Serializable> Query<B> fromSpec(QuerySpec spec) {
        Objects.requireNonNull(spec, "spec");
        EntityMeta<B> meta = registry.entityMeta(spec.entityId());

        Condition<B> where = (spec.where() == null)
                ? Conditions.trueCondition()
                : bindCond(spec.entityId(), spec.where());

        Scan<B, ? extends Serializable> scan = null;
        if (spec.scan() != null) {
            var p = registry.<B>property(spec.entityId(), spec.scan().property());
            Range<Serializable> r = bindRange(p, spec.scan().range());
            scan = new Scan<>((PropertyMeta<B, Serializable>) p, r);
        }

        var qb = Query.<B>from(meta).where(where);
        if (scan != null) {
            // note: type erasure for scan is fine here; validation happens in bindRange
            qb.scan((PropertyMeta<B, Serializable>) scan.prop(), (Range<Serializable>) scan.range());
        }
        if (spec.limit() != null) qb.limit(spec.limit());
        return qb.build();
    }

    public <B extends Serializable> QuerySpec toSpec(String entityId, Query<B> query) {
        // This is optional. Often you'll build QuerySpec client-side.
        // Provided for completeness (cache keys etc.).
        CondSpec where = SpecMapper.toSpec(query.where());
        ScanSpec scan = null;
        if (query.scan() != null) {
            scan = new ScanSpec(
                    query.scan().prop().getPropertyName(),
                    new RangeSpec(
                            query.scan().range().lower(),
                            query.scan().range().lowerBound().name(),
                            query.scan().range().upper(),
                            query.scan().range().upperBound().name()
                    )
            );
        }
        return new QuerySpec(entityId, where, scan, query.limit());
    }

    // ---- binding helpers ----

    private <B extends Serializable> Condition<B> bindCond(String entityId, CondSpec c) {
        if (c instanceof TrueSpec) return Conditions.trueCondition();
        if (c instanceof FalseSpec) return Conditions.falseCondition();

        if (c instanceof EqSpec e) {
            var p = registry.<B>property(entityId, e.property());
            var v = castValue(p, e.value());
            return Conditions.eq((PropertyMeta<B, Serializable>) p, v);
        }
        if (c instanceof NeSpec e) {
            var p = registry.<B>property(entityId, e.property());
            var v = castValue(p, e.value());
            return Conditions.ne((PropertyMeta<B, Serializable>) p, v);
        }
        if (c instanceof InSpec e) {
            var p = registry.<B>property(entityId, e.property());
            var list = new ArrayList<Serializable>();
            if (e.values() != null) {
                for (Object o : e.values()) list.add(castValue(p, o));
            }
            return Conditions.in((PropertyMeta<B, Serializable>) p, list);
        }
        if (c instanceof IsNullSpec e) {
            var p = registry.<B>property(entityId, e.property());
            return Conditions.isNull((PropertyMeta<B, Serializable>) p);
        }
        if (c instanceof IsNotNullSpec e) {
            var p = registry.<B>property(entityId, e.property());
            return Conditions.isNotNull((PropertyMeta<B, Serializable>) p);
        }
        if (c instanceof BetweenSpec e) {
            var p = registry.<B>property(entityId, e.property());
            var lo = castValue(p, e.lo());
            var hi = castValue(p, e.hi());
            return Conditions.between((PropertyMeta<B, Serializable>) p, lo, hi);
        }
        if (c instanceof CmpSpec e) {
            var p = registry.<B>property(entityId, e.property());
            var v = castValue(p, e.value());
            var op = CmpOp.valueOf(e.op());
            return Conditions.cmp((PropertyMeta<B, Serializable>) p, op, v);
        }
        if (c instanceof NotSpec e) {
            return bindCond(entityId, e.inner()).not();
        }
        if (c instanceof AndSpec e) {
            List<CondSpec> items = (e.items() == null) ? List.of() : e.items();
            Condition<B> acc = Conditions.trueCondition();
            for (CondSpec it : items) acc = acc.and(bindCond(entityId, it));
            return acc;
        }
        if (c instanceof OrSpec e) {
            List<CondSpec> items = (e.items() == null) ? List.of() : e.items();
            Condition<B> acc = Conditions.falseCondition();
            for (CondSpec it : items) acc = acc.or(bindCond(entityId, it));
            return acc;
        }

        throw new IllegalArgumentException("Unsupported CondSpec type: " + c.getClass().getName());
    }

    private static <B extends Serializable> Range<Serializable> bindRange(PropertyMeta<B, Serializable> p, RangeSpec rs) {
        if (rs == null) throw new IllegalArgumentException("range is null for property " + p.getPropertyName());
        Bound lb = Bound.valueOf(rs.lowerBound());
        Bound ub = Bound.valueOf(rs.upperBound());

        Serializable lo = (lb == Bound.UNBOUNDED) ? null : castValue(p, rs.lower());
        Serializable hi = (ub == Bound.UNBOUNDED) ? null : castValue(p, rs.upper());

        return new Range<>(lo, lb, hi, ub);
    }

    private static <B extends Serializable> Serializable castValue(PropertyMeta<B, Serializable> p, Object o) {
        if (o == null) return null;
        Class<?> t = p.getPropertyClass();
        if (!t.isInstance(o)) {
            // Here is where you'd add converters (String->Timestamp, String->Enum, etc.).
            throw new IllegalArgumentException("Type mismatch for property " + p.getPropertyName() +
                    ": expected " + t.getName() + " got " + o.getClass().getName());
        }
        return (Serializable) o;
    }
}
