package org.github.dbjo.criteria.bind;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
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
            scan = new Scan<>(p, r);
        }

        var qb = Query.from(meta).where(where);
        if (scan != null) {
            qb.scan((PropertyMeta<B, Serializable>) scan.prop(), (Range<Serializable>) scan.range());
        }
        if (spec.limit() != null) qb.limit(spec.limit());
        return qb.build();
    }

    public <B extends Serializable> QuerySpec toSpec(String entityId, Query<B> query) {
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
            return Conditions.eq(p, v);
        }
        if (c instanceof NeSpec e) {
            var p = registry.<B>property(entityId, e.property());
            var v = castValue(p, e.value());
            return Conditions.ne(p, v);
        }
        if (c instanceof InSpec e) {
            var p = registry.<B>property(entityId, e.property());
            var list = new ArrayList<Serializable>();
            if (e.values() != null) {
                for (Object o : e.values()) list.add(castValue(p, o));
            }
            return Conditions.in(p, list);
        }
        if (c instanceof IsNullSpec e) {
            var p = registry.<B>property(entityId, e.property());
            return Conditions.isNull(p);
        }
        if (c instanceof IsNotNullSpec e) {
            var p = registry.<B>property(entityId, e.property());
            return Conditions.isNotNull(p);
        }
        if (c instanceof BetweenSpec e) {
            var p = registry.<B>property(entityId, e.property());
            var lo = castValue(p, e.lo());
            var hi = castValue(p, e.hi());
            return Conditions.between(p, lo, hi);
        }
        if (c instanceof CmpSpec e) {
            var p = registry.<B>property(entityId, e.property());
            var v = castValue(p, e.value());
            var op = CmpOp.valueOf(e.op());
            return Conditions.cmp(p, op, v);
        }
        if (c instanceof NotSpec e) {
            Condition<B> inner = bindCond(entityId, e.inner());
            return inner.not();
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
        if (t.isInstance(o)) return (Serializable) o;

        // numeric widening / parsing
        if (t == Integer.class) return (Serializable) coerceInt(o, p);
        if (t == Long.class) return (Serializable) coerceLong(o, p);
        if (t == Short.class) return (Serializable) coerceShort(o, p);
        if (t == Boolean.class) return (Serializable) coerceBool(o, p);
        if (t == String.class) return (Serializable) String.valueOf(o);

        // enum coercion (PK via of/ofNullable, or any unique field via by* / by*Nullable)
        if (t.isEnum()) {
            return (Serializable) coerceEnum(t, o, p);
        }

        throw new IllegalArgumentException("Type mismatch for property " + p.getPropertyName() +
                ": expected " + t.getName() + " got " + o.getClass().getName());
    }

    private static Integer coerceInt(Object o, PropertyMeta<?, ?> p) {
        if (o instanceof Integer i) return i;
        if (o instanceof Number n) return n.intValue();
        if (o instanceof String s) {
            try { return Integer.parseInt(s.trim()); } catch (Exception e) { /* fallthrough */ }
        }
        throw new IllegalArgumentException("Type mismatch for property " + p.getPropertyName() +
                ": expected Integer got " + o.getClass().getName());
    }

    private static Long coerceLong(Object o, PropertyMeta<?, ?> p) {
        if (o instanceof Long l) return l;
        if (o instanceof Number n) return n.longValue();
        if (o instanceof String s) {
            try { return Long.parseLong(s.trim()); } catch (Exception e) { /* fallthrough */ }
        }
        throw new IllegalArgumentException("Type mismatch for property " + p.getPropertyName() +
                ": expected Long got " + o.getClass().getName());
    }

    private static Short coerceShort(Object o, PropertyMeta<?, ?> p) {
        if (o instanceof Short s) return s;
        if (o instanceof Number n) return n.shortValue();
        if (o instanceof String s) {
            try { return Short.parseShort(s.trim()); } catch (Exception e) { /* fallthrough */ }
        }
        throw new IllegalArgumentException("Type mismatch for property " + p.getPropertyName() +
                ": expected Short got " + o.getClass().getName());
    }

    private static Boolean coerceBool(Object o, PropertyMeta<?, ?> p) {
        if (o instanceof Boolean b) return b;
        if (o instanceof String s) return Boolean.parseBoolean(s.trim());
        throw new IllegalArgumentException("Type mismatch for property " + p.getPropertyName() +
                ": expected Boolean got " + o.getClass().getName());
    }

    private static Object coerceEnum(Class<?> enumClass, Object o, PropertyMeta<?, ?> p) {
        // already handled isInstance above

        // 1) ofNullable(...)
        Object v = tryInvokeEnumStatic(enumClass, "ofNullable", o, true);
        if (v != null) return v;

        // 2) of(...)
        try {
            v = tryInvokeEnumStatic(enumClass, "of", o, false);
            if (v != null) return v;
        } catch (RuntimeException ex) {
            // continue to by* lookups; of(...) may throw for unknown key
        }

        // 3) by*Nullable(...) then by*(...)
        if (o instanceof String) {
            Object byV = tryInvokeAnyByNullable(enumClass, o);
            if (byV != null) return byV;

            try {
                Object byNonNull = tryInvokeAnyByNonNull(enumClass, o);
                if (byNonNull != null) return byNonNull;
            } catch (RuntimeException ex) {
                // ignore and try valueOf
            }

            // 4) last resort: Enum.valueOf (matches CONST names, not DB values)
            try {
                @SuppressWarnings({"unchecked","rawtypes"})
                Object ev = Enum.valueOf((Class<? extends Enum>) enumClass, ((String) o).trim());
                return ev;
            } catch (Exception ignored) { }
        }

        throw new IllegalArgumentException("Type mismatch for property " + p.getPropertyName() +
                ": expected " + enumClass.getName() + " got " + o.getClass().getName() +
                " (no suitable enum coercion via of/ofNullable/by*)");
    }

    private static Object tryInvokeEnumStatic(Class<?> enumClass, String methodName, Object raw, boolean nullableMethod) {
        Method[] ms = enumClass.getMethods();
        for (Method m : ms) {
            if (!m.getName().equals(methodName)) continue;
            if (!Modifier.isStatic(m.getModifiers())) continue;
            if (m.getParameterCount() != 1) continue;
            if (!enumClass.isAssignableFrom(m.getReturnType())) continue;

            Object arg = coerceToParamType(raw, m.getParameterTypes()[0]);
            if (arg == COERCE_FAIL) continue;

            try {
                Object r = m.invoke(null, arg);
                if (nullableMethod) return r; // may be null
                return r;
            } catch (ReflectiveOperationException e) {
                // treat invocation failures as "not usable"
                return null;
            } catch (RuntimeException ex) {
                // of(...) may throw IllegalArgumentException for unknown
                throw ex;
            }
        }
        return null;
    }

    private static Object tryInvokeAnyByNullable(Class<?> enumClass, Object raw) {
        Method[] ms = enumClass.getMethods();
        for (Method m : ms) {
            String n = m.getName();
            if (!n.startsWith("by") || !n.endsWith("Nullable")) continue;
            if (!Modifier.isStatic(m.getModifiers())) continue;
            if (m.getParameterCount() != 1) continue;
            if (!enumClass.isAssignableFrom(m.getReturnType())) continue;

            Object arg = coerceToParamType(raw, m.getParameterTypes()[0]);
            if (arg == COERCE_FAIL) continue;

            try {
                Object r = m.invoke(null, arg);
                if (r != null) return r;
            } catch (ReflectiveOperationException ignored) { }
        }
        return null;
    }

    private static Object tryInvokeAnyByNonNull(Class<?> enumClass, Object raw) {
        Method[] ms = enumClass.getMethods();
        for (Method m : ms) {
            String n = m.getName();
            if (!n.startsWith("by") || n.endsWith("Nullable")) continue;
            if (!Modifier.isStatic(m.getModifiers())) continue;
            if (m.getParameterCount() != 1) continue;
            if (!enumClass.isAssignableFrom(m.getReturnType())) continue;

            Object arg = coerceToParamType(raw, m.getParameterTypes()[0]);
            if (arg == COERCE_FAIL) continue;

            try {
                Object r = m.invoke(null, arg);
                if (r != null) return r;
            } catch (ReflectiveOperationException ignored) { }
        }
        return null;
    }

    private static final Object COERCE_FAIL = new Object();

    private static Object coerceToParamType(Object raw, Class<?> paramType) {
        if (raw == null) return null;

        if (paramType == String.class) return String.valueOf(raw);

        if (paramType == int.class || paramType == Integer.class) {
            if (raw instanceof Number n) return n.intValue();
            if (raw instanceof String s) {
                try { return Integer.parseInt(s.trim()); } catch (Exception e) { return COERCE_FAIL; }
            }
            return COERCE_FAIL;
        }

        if (paramType == long.class || paramType == Long.class) {
            if (raw instanceof Number n) return n.longValue();
            if (raw instanceof String s) {
                try { return Long.parseLong(s.trim()); } catch (Exception e) { return COERCE_FAIL; }
            }
            return COERCE_FAIL;
        }

        if (paramType == short.class || paramType == Short.class) {
            if (raw instanceof Number n) return n.shortValue();
            if (raw instanceof String s) {
                try { return Short.parseShort(s.trim()); } catch (Exception e) { return COERCE_FAIL; }
            }
            return COERCE_FAIL;
        }

        if (paramType.isInstance(raw)) return raw;

        return COERCE_FAIL;
    }
}
