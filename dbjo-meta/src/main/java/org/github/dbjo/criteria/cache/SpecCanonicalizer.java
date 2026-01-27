package org.github.dbjo.criteria.cache;

import java.util.*;
import org.github.dbjo.criteria.spec.*;

public final class SpecCanonicalizer {
    private SpecCanonicalizer() {}

    public static QuerySpec canonicalize(QuerySpec q) {
        if (q == null) return null;
        CondSpec where = (q.where() == null) ? new TrueSpec() : canonicalizeCond(q.where());
        ScanSpec scan = canonicalizeScan(q.scan());
        return new QuerySpec(q.entityId(), where, scan, q.limit());
    }

    private static ScanSpec canonicalizeScan(ScanSpec scan) {
        if (scan == null) return null;
        RangeSpec r = scan.range();
        if (r == null) return scan;
        // Normalize bound strings
        String lb = BoundTok.norm(r.lowerBound());
        String ub = BoundTok.norm(r.upperBound());
        return new ScanSpec(scan.property(), new RangeSpec(r.lower(), lb, r.upper(), ub));
    }

    private static CondSpec canonicalizeCond(CondSpec c) {
        if (c == null) return new TrueSpec();

        // Normalize Eq/Ne null into IS NULL / IS NOT NULL (cache key stability)
        if (c instanceof EqSpec e) {
            return (e.value() == null) ? new IsNullSpec(e.property()) : e;
        }
        if (c instanceof NeSpec e) {
            return (e.value() == null) ? new IsNotNullSpec(e.property()) : e;
        }

        if (c instanceof NotSpec n) {
            CondSpec inner = canonicalizeCond(n.inner());
            return new NotSpec(inner);
        }

        if (c instanceof AndSpec a) {
            List<CondSpec> flat = new ArrayList<>();
            if (a.items() != null) {
                for (CondSpec it : a.items()) addAnd(flat, canonicalizeCond(it));
            }

            // identities / short-circuits
            for (CondSpec it : flat) if (it instanceof FalseSpec) return new FalseSpec();
            flat.removeIf(it -> it instanceof TrueSpec);

            if (flat.isEmpty()) return new TrueSpec();
            if (flat.size() == 1) return flat.get(0);

            flat.sort(Comparator.comparing(SpecStringifier::stableKey));
            return new AndSpec(List.copyOf(flat));
        }

        if (c instanceof OrSpec o) {
            List<CondSpec> flat = new ArrayList<>();
            if (o.items() != null) {
                for (CondSpec it : o.items()) addOr(flat, canonicalizeCond(it));
            }

            for (CondSpec it : flat) if (it instanceof TrueSpec) return new TrueSpec();
            flat.removeIf(it -> it instanceof FalseSpec);

            if (flat.isEmpty()) return new FalseSpec();
            if (flat.size() == 1) return flat.get(0);

            flat.sort(Comparator.comparing(SpecStringifier::stableKey));
            return new OrSpec(List.copyOf(flat));
        }

        if (c instanceof InSpec i) {
            List<Object> vals = (i.values() == null) ? List.of() : i.values();
            // dedupe + sort by stable string key
            var set = new LinkedHashMap<String, Object>();
            for (Object v : vals) set.put(SpecStringifier.valueKey(v), v);
            var keys = new ArrayList<>(set.keySet());
            Collections.sort(keys);
            var out = new ArrayList<>(keys.size());
            for (String k : keys) out.add(set.get(k));
            return new InSpec(i.property(), List.copyOf(out));
        }

        // Normalize bounds string in CmpSpec
        if (c instanceof CmpSpec cmp) {
            return new CmpSpec(cmp.property(), cmp.op().toUpperCase(Locale.ROOT), cmp.value());
        }

        // pass-through for other atoms
        return c;
    }

    private static void addAnd(List<CondSpec> out, CondSpec c) {
        if (c instanceof AndSpec a && a.items() != null) out.addAll(a.items());
        else out.add(c);
    }

    private static void addOr(List<CondSpec> out, CondSpec c) {
        if (c instanceof OrSpec o && o.items() != null) out.addAll(o.items());
        else out.add(c);
    }

    private static final class BoundTok {
        static String norm(String s) {
            if (s == null) return "UNBOUNDED";
            String u = s.toUpperCase(Locale.ROOT);
            return switch (u) {
                case "INCLUSIVE", "EXCLUSIVE", "UNBOUNDED" -> u;
                default -> throw new IllegalArgumentException("Bad bound token: " + s);
            };
        }
    }
}
