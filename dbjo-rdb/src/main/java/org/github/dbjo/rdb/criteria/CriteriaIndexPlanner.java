package org.github.dbjo.rdb.criteria;

import org.github.dbjo.criteria.*;
import org.github.dbjo.meta.entity.PropertyMeta;
import org.github.dbjo.rdb.IndexPredicate;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

public final class CriteriaIndexPlanner {
    private CriteriaIndexPlanner() {}

    public static final int DEFAULT_MAX_IN_PUSHDOWN = 16;
    public static final int DEFAULT_MAX_OR_EQ_PUSHDOWN = 16;

    public record IndexInfo(String name, Function<Object, byte[]> encoder) {
        public IndexInfo {
            Objects.requireNonNull(name, "name");
            Objects.requireNonNull(encoder, "encoder");
        }
    }

    public record Candidate(IndexPredicate predicate, int score, List<IndexPredicate> unionPredicates) {
        public Candidate {
            unionPredicates = (unionPredicates == null) ? List.of() : List.copyOf(unionPredicates);
        }

        public static Candidate of(IndexPredicate predicate, int score) {
            return new Candidate(predicate, score, List.of());
        }

        public static Candidate union(List<IndexPredicate> predicates, int score) {
            return new Candidate(null, score, predicates);
        }
    }

    public static Candidate bestCandidate(Condition<?> where, Map<String, IndexInfo> indexByPropLower) {
        return bestCandidate(where, indexByPropLower, DEFAULT_MAX_IN_PUSHDOWN, DEFAULT_MAX_OR_EQ_PUSHDOWN);
    }

    public static Candidate bestCandidate(
            Condition<?> where,
            Map<String, IndexInfo> indexByPropLower,
            int maxInPushdown,
            int maxOrEqPushdown
    ) {
        if (where == null || indexByPropLower == null || indexByPropLower.isEmpty()) return null;
        return candidateFromCond(where, indexByPropLower, maxInPushdown, maxOrEqPushdown);
    }

    private static Candidate candidateFromCond(
            Condition<?> c,
            Map<String, IndexInfo> indexByPropLower,
            int maxInPushdown,
            int maxOrEqPushdown
    ) {
        if (c == null) return null;

        if (c instanceof Eq<?, ?> e) {
            return candEq(e.prop(), e.value(), indexByPropLower);
        }
        if (c instanceof Between<?, ?> b) {
            return candBetween(b.prop(), b.lo(), b.hi(), indexByPropLower);
        }
        if (c instanceof Cmp<?, ?> cmp) {
            return candCmp(cmp.prop(), cmp.op(), cmp.value(), indexByPropLower);
        }
        if (c instanceof In<?, ?> in) {
            return candIn(in.prop(), in.values(), indexByPropLower, maxInPushdown);
        }
        if (c instanceof And<?> a) {
            return better(
                    candidateFromCond(a.left(), indexByPropLower, maxInPushdown, maxOrEqPushdown),
                    candidateFromCond(a.right(), indexByPropLower, maxInPushdown, maxOrEqPushdown)
            );
        }
        if (c instanceof Or<?> o) {
            return candOrEqUnion(o, indexByPropLower, maxOrEqPushdown);
        }
        return null;
    }

    private static Candidate candEq(PropertyMeta<?, ?> prop, Object value, Map<String, IndexInfo> indexByPropLower) {
        if (prop == null || value == null) return null;
        IndexInfo idx = indexForProp(prop, indexByPropLower);
        if (idx == null) return null;

        byte[] vb = encode(idx, value);
        if (vb == null) return null;

        return Candidate.of(new IndexPredicate.Eq(idx.name(), vb), 1000);
    }

    private static Candidate candBetween(PropertyMeta<?, ?> prop, Object lo, Object hi, Map<String, IndexInfo> indexByPropLower) {
        if (prop == null) return null;
        IndexInfo idx = indexForProp(prop, indexByPropLower);
        if (idx == null) return null;

        byte[] from = (lo == null) ? null : encode(idx, lo);
        byte[] to = (hi == null) ? null : encode(idx, hi);
        if (from == null && to == null) return null;

        return Candidate.of(new IndexPredicate.Range(idx.name(), from, true, to, true), 650);
    }

    private static Candidate candCmp(PropertyMeta<?, ?> prop, CmpOp op, Object value, Map<String, IndexInfo> indexByPropLower) {
        if (prop == null || op == null || value == null) return null;
        IndexInfo idx = indexForProp(prop, indexByPropLower);
        if (idx == null) return null;

        byte[] b = encode(idx, value);
        if (b == null) return null;

        return switch (op) {
            case LT -> Candidate.of(new IndexPredicate.Range(idx.name(), null, false, b, false), 600);
            case LE -> Candidate.of(new IndexPredicate.Range(idx.name(), null, false, b, true), 600);
            case GT -> Candidate.of(new IndexPredicate.Range(idx.name(), b, false, null, false), 600);
            case GE -> Candidate.of(new IndexPredicate.Range(idx.name(), b, true, null, false), 600);
        };
    }

    private static Candidate candIn(
            PropertyMeta<?, ?> prop,
            List<?> values,
            Map<String, IndexInfo> indexByPropLower,
            int maxInPushdown
    ) {
        if (prop == null || values == null || values.isEmpty()) return null;
        if (values.size() > maxInPushdown) return null;
        IndexInfo idx = indexForProp(prop, indexByPropLower);
        if (idx == null) return null;

        ArrayList<IndexPredicate> ips = new ArrayList<>();
        for (Object v : values) {
            if (v == null) return null;
            byte[] b = encode(idx, v);
            if (b == null) return null;
            ips.add(new IndexPredicate.Eq(idx.name(), b));
        }
        return Candidate.union(ips, 900 - ips.size());
    }

    private static Candidate candOrEqUnion(
            Or<?> o,
            Map<String, IndexInfo> indexByPropLower,
            int maxOrEqPushdown
    ) {
        ArrayList<Eq<?, ?>> eqs = new ArrayList<>();
        if (!collectEqFromOr(o, eqs)) return null;
        if (eqs.isEmpty() || eqs.size() > maxOrEqPushdown) return null;

        PropertyMeta<?, ?> p0 = eqs.get(0).prop();
        if (p0 == null || p0.getPropertyName() == null) return null;
        String pn = p0.getPropertyName().trim().toLowerCase(Locale.ROOT);

        for (Eq<?, ?> e : eqs) {
            PropertyMeta<?, ?> p = e.prop();
            if (p == null || p.getPropertyName() == null) return null;
            if (!pn.equals(p.getPropertyName().trim().toLowerCase(Locale.ROOT))) return null;
        }

        IndexInfo idx = indexByPropLower.get(pn);
        if (idx == null) return null;

        ArrayList<IndexPredicate> ips = new ArrayList<>();
        for (Eq<?, ?> e : eqs) {
            Object v = e.value();
            if (v == null) return null;
            byte[] b = encode(idx, v);
            if (b == null) return null;
            ips.add(new IndexPredicate.Eq(idx.name(), b));
        }
        return Candidate.union(ips, 880 - ips.size());
    }

    private static boolean collectEqFromOr(Condition<?> c, List<Eq<?, ?>> out) {
        if (c instanceof Eq<?, ?> e) {
            out.add(e);
            return true;
        }
        if (c instanceof Or<?> o) {
            return collectEqFromOr(o.left(), out) && collectEqFromOr(o.right(), out);
        }
        return false;
    }

    private static Candidate better(Candidate a, Candidate b) {
        if (a == null) return b;
        if (b == null) return a;
        return (b.score() > a.score()) ? b : a;
    }

    private static IndexInfo indexForProp(PropertyMeta<?, ?> prop, Map<String, IndexInfo> indexByPropLower) {
        if (prop == null) return null;
        String pn = prop.getPropertyName();
        if (pn == null || pn.isBlank()) return null;
        return indexByPropLower.get(pn.trim().toLowerCase(Locale.ROOT));
    }

    private static byte[] encode(IndexInfo idx, Object value) {
        try {
            return idx.encoder().apply(value);
        } catch (ClassCastException ex) {
            return null;
        }
    }
}
