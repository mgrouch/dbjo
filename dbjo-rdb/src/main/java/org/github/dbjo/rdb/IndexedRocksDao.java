package org.github.dbjo.rdb;

import org.github.dbjo.criteria.*;
import org.github.dbjo.meta.entity.PropertyMeta;
import org.github.dbjo.rdb.criteria.CriteriaSupport;
import org.rocksdb.ColumnFamilyHandle;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Stream;

/**
 * RocksDAO with secondary indexes maintained in index column families.
 *
 * This version adds a criteria-based select() that can push down simple predicates
 * (Eq / Cmp / Between / Scan range / small In / simple Or-of-Eq) into RocksDB index scans.
 *
 * Any pushed down predicate is still validated by full criteria evaluation (safety first).
 */
public abstract class IndexedRocksDao<T, K> extends AbstractRocksDao<T, K> {

    private static final byte[] EMPTY = new byte[0];

    // heuristic limits for union pushdown
    private static final int MAX_IN_PUSHDOWN = 16;
    private static final int MAX_OR_EQ_PUSHDOWN = 16;

    // ✅ IndexDef has 2 type params
    private final List<IndexDef<T, ?>> indexes;

    // cached lookup: propertyName (lower) -> indexdef
    private volatile Map<String, IndexDef<T, ?>> indexByPropLower;

    protected IndexedRocksDao(
            RocksSessions sessions,
            ColumnFamilyHandle primaryCf,
            KeyCodec<K> keyCodec,
            Codec<T> valueCodec,
            Map<String, ColumnFamilyHandle> indexCfs,
            List<? extends IndexDef<T, ?>> indexes
    ) {
        super(sessions, primaryCf, keyCodec, valueCodec, indexCfs);
        this.indexes = List.copyOf(indexes);
    }

    /** Convenience: pull key/codec/indexes from EntityDef. */
    protected IndexedRocksDao(RocksSessions sessions, EntityDef<T, K> entity, Map<String, ColumnFamilyHandle> indexCfs) {
        this(sessions, entity.primaryCf(), entity.keyCodec(), entity.valueCodec(), indexCfs, entity.indexes());
    }

    protected IndexedRocksDao(RocksSessions sessions, ResolvedEntityDef<T, K> ent) {
        this(sessions,
                ent.def().primaryCf(),
                ent.def().keyCodec(),
                ent.def().valueCodec(),
                ent.indexCfs(),
                ent.def().indexes());
    }

    /**
     * Criteria select:
     * - if we can map a part of criteria to an index predicate, we scan the index CF
     * - otherwise fall back to full primary scan
     *
     * Always applies full criteria evaluation on candidates.
     */
    public List<T> select(org.github.dbjo.criteria.Query<?> criteria) {
        Objects.requireNonNull(criteria, "criteria");

        final int limit = (criteria.limit() == null) ? Integer.MAX_VALUE : Math.max(0, criteria.limit());
        if (limit == 0) return List.of();

        Plan<K> plan = plan(criteria);

        ArrayList<T> out = new ArrayList<>();
        HashSet<K> seen = (plan.queries.size() > 1) ? new HashSet<>() : null;

        for (Query<K> rq : plan.queries) {
            if (out.size() >= limit) break;

            try (Stream<Map.Entry<K, T>> s = stream(rq)) {
                Iterator<Map.Entry<K, T>> it = s.iterator();
                while (it.hasNext() && out.size() < limit) {
                    Map.Entry<K, T> e = it.next();
                    if (seen != null && !seen.add(e.getKey())) continue;

                    T bean = e.getValue();
                    if (CriteriaSupport.test(criteria, bean)) {
                        out.add(bean);
                    }
                }
            }
        }

        return out;
    }

    // index maintenance

    @Override
    protected final void maintainIndexes(RocksWriteBatch batch, K key, T oldValueOrNull, T newValue) {

        final byte[] pk = keyCodec.encodeKey(key);

        for (IndexDef<T, ?> idx : indexes) {
            ColumnFamilyHandle cf = indexCfs.get(idx.name());
            if (cf == null) throw new IllegalStateException("Missing index CF for " + idx.name());

            Set<ByteArrayKey> oldKeys = toSet(idx.valueKeysOrEmpty(oldValueOrNull));
            Set<ByteArrayKey> newKeys = toSet(idx.valueKeysOrEmpty(newValue));

            for (ByteArrayKey v : oldKeys) {
                if (!newKeys.contains(v)) {
                    batch.delete(cf, IndexKeys.unique(v.bytes(), pk));
                }
            }
            for (ByteArrayKey v : newKeys) {
                if (!oldKeys.contains(v)) {
                    batch.put(cf, IndexKeys.unique(v.bytes(), pk), EMPTY);
                }
            }
        }
    }

    @Override
    protected final void maintainIndexesOnDelete(RocksWriteBatch batch, K key, T oldValue) {

        final byte[] pk = keyCodec.encodeKey(key);

        for (IndexDef<T, ?> idx : indexes) {
            ColumnFamilyHandle cf = indexCfs.get(idx.name());
            if (cf == null) throw new IllegalStateException("Missing index CF for " + idx.name());

            for (byte[] v : idx.valueKeysOrEmpty(oldValue)) {
                if (v == null) continue;
                batch.delete(cf, IndexKeys.unique(v, pk));
            }
        }
    }

    private static Set<ByteArrayKey> toSet(Iterable<byte[]> keys) {
        HashSet<ByteArrayKey> s = new HashSet<>();
        for (byte[] k : keys) {
            if (k == null) continue;
            s.add(new ByteArrayKey(k));
        }
        return s;
    }

    // criteria -> rocks query planning

    private Map<String, IndexDef<T, ?>> indexByPropLower() {
        Map<String, IndexDef<T, ?>> m = indexByPropLower;
        if (m != null) return m;

        HashMap<String, IndexDef<T, ?>> tmp = new HashMap<>();
        for (IndexDef<T, ?> idx : indexes) {
            String pn = idx.propertyName();
            if (pn == null || pn.isBlank()) continue;
            tmp.put(pn.trim().toLowerCase(Locale.ROOT), idx);
        }
        indexByPropLower = tmp;
        return tmp;
    }

    private Plan<K> plan(org.github.dbjo.criteria.Query<?> cq) {
        Candidate scanCand  = candidateFromScan(cq.scan());
        Candidate whereCand = candidateFromWhere(cq.where());

        Candidate best = better(scanCand, whereCand);

        if (best == null) {
            Query.Builder<K> b = Query.builder();
            if (cq.limit() != null) b.limit(cq.limit());
            return new Plan<>(List.of(b.build()));
        }

        // union case: multiple Rocks queries (each with exactly one IndexPredicate)
        if (best.unionPredicates != null && !best.unionPredicates.isEmpty()) {
            ArrayList<Query<K>> qs = new ArrayList<>(best.unionPredicates.size());
            for (IndexPredicate ip : best.unionPredicates) {
                Query.Builder<K> b = Query.<K>builder().where(ip);
                if (cq.limit() != null) b.limit(cq.limit());
                qs.add(b.build());
            }
            return new Plan<>(qs);
        }

        Query.Builder<K> b = Query.<K>builder().where(best.predicate);
        if (cq.limit() != null) b.limit(cq.limit());
        return new Plan<>(List.of(b.build()));
    }

    private static Candidate better(Candidate a, Candidate b) {
        if (a == null) return b;
        if (b == null) return a;
        return (b.score > a.score) ? b : a;
    }

    private Candidate candidateFromScan(Scan<?, ? extends Serializable> scan) {
        if (scan == null) return null;

        PropertyMeta<?, ?> prop = scan.prop();
        if (prop == null) return null;

        String pn = prop.getPropertyName();
        if (pn == null || pn.isBlank()) return null;

        IndexDef<T, ?> idx = indexByPropLower().get(pn.trim().toLowerCase(Locale.ROOT));
        if (idx == null) return null;

        IndexPredicate ip = toIndexRange(idx, scan.range());
        if (ip == null) return null;

        return new Candidate(ip, 700);
    }

    private Candidate candidateFromWhere(Condition<?> where) {
        if (where == null) return null;
        return candidateFromCond(where);
    }

    private Candidate candidateFromCond(Condition<?> c) {
        if (c == null) return null;

        // Eq
        if (c instanceof Eq<?, ?> e) {
            return candEq(e.prop(), e.value());
        }

        // Between (inclusive)
        if (c instanceof Between<?, ?> b) {
            return candBetween(b.prop(), b.lo(), b.hi());
        }

        // Cmp
        if (c instanceof Cmp<?, ?> cmp) {
            return candCmp(cmp.prop(), cmp.op(), cmp.value());
        }

        // In
        if (c instanceof In<?, ?> in) {
            return candIn(in.prop(), in.values());
        }

        // And: choose best child (binary)
        if (c instanceof And<?> a) {
            return better(candidateFromCond(a.left()), candidateFromCond(a.right()));
        }

        // Or: support OR of Eq on same property (as a union pushdown)
        if (c instanceof Or<?> o) {
            return candOrEqUnion(o);
        }

        return null;
    }

    private Candidate candEq(PropertyMeta<?, ?> prop, Object value) {
        if (prop == null) return null;
        String pn = prop.getPropertyName();
        if (pn == null || pn.isBlank()) return null;

        IndexDef<T, ?> idx = indexByPropLower().get(pn.trim().toLowerCase(Locale.ROOT));
        if (idx == null) return null;

        if (value == null) return null;

        byte[] vb;
        try {
            vb = idx.encodeAnyOrNull(value);
        } catch (ClassCastException ex) {
            return null;
        }
        if (vb == null) return null;

        return new Candidate(new IndexPredicate.Eq(idx.name(), vb), 1000);
    }

    private Candidate candBetween(PropertyMeta<?, ?> prop, Object lo, Object hi) {
        if (prop == null) return null;
        String pn = prop.getPropertyName();
        if (pn == null || pn.isBlank()) return null;

        IndexDef<T, ?> idx = indexByPropLower().get(pn.trim().toLowerCase(Locale.ROOT));
        if (idx == null) return null;

        byte[] from, to;
        try {
            from = (lo == null) ? null : idx.encodeAnyOrNull(lo);
            to   = (hi == null) ? null : idx.encodeAnyOrNull(hi);
        } catch (ClassCastException ex) {
            return null;
        }
        if (from == null && to == null) return null;

        IndexPredicate ip = new IndexPredicate.Range(idx.name(), from, true, to, true);
        return new Candidate(ip, 650);
    }

    private Candidate candCmp(PropertyMeta<?, ?> prop, CmpOp op, Object v) {
        if (prop == null || op == null) return null;
        String pn = prop.getPropertyName();
        if (pn == null || pn.isBlank()) return null;

        IndexDef<T, ?> idx = indexByPropLower().get(pn.trim().toLowerCase(Locale.ROOT));
        if (idx == null) return null;

        if (v == null) return null;

        byte[] b;
        try {
            b = idx.encodeAnyOrNull(v);
        } catch (ClassCastException ex) {
            return null;
        }
        if (b == null) return null;

        return switch (op) {
            case LT -> new Candidate(new IndexPredicate.Range(idx.name(), null, false, b, false), 600);
            case LE -> new Candidate(new IndexPredicate.Range(idx.name(), null, false, b, true),  600);
            case GT -> new Candidate(new IndexPredicate.Range(idx.name(), b, false, null, false), 600);
            case GE -> new Candidate(new IndexPredicate.Range(idx.name(), b, true,  null, false), 600);
        };
    }

    private Candidate candIn(PropertyMeta<?, ?> prop, List<?> values) {
        if (prop == null || values == null || values.isEmpty()) return null;

        String pn = prop.getPropertyName();
        if (pn == null || pn.isBlank()) return null;

        IndexDef<T, ?> idx = indexByPropLower().get(pn.trim().toLowerCase(Locale.ROOT));
        if (idx == null) return null;

        if (values.size() > MAX_IN_PUSHDOWN) return null;

        ArrayList<IndexPredicate> ips = new ArrayList<>();
        for (Object v : values) {
            if (v == null) return null; // completeness: don't pushdown if null is present
            byte[] b;
            try {
                b = idx.encodeAnyOrNull(v);
            } catch (ClassCastException ex) {
                return null;
            }
            if (b == null) return null;
            ips.add(new IndexPredicate.Eq(idx.name(), b));
        }

        Candidate c = new Candidate(null, 900 - ips.size());
        c.unionPredicates = ips;
        return c;
    }

    private Candidate candOrEqUnion(Or<?> o) {
        ArrayList<Eq<?, ?>> eqs = new ArrayList<>();
        if (!collectEqFromOr(o, eqs)) return null;

        if (eqs.isEmpty() || eqs.size() > MAX_OR_EQ_PUSHDOWN) return null;

        PropertyMeta<?, ?> p0 = eqs.get(0).prop();
        if (p0 == null || p0.getPropertyName() == null) return null;
        String pn = p0.getPropertyName().trim().toLowerCase(Locale.ROOT);

        for (Eq<?, ?> e : eqs) {
            PropertyMeta<?, ?> p = e.prop();
            if (p == null || p.getPropertyName() == null) return null;
            if (!pn.equals(p.getPropertyName().trim().toLowerCase(Locale.ROOT))) return null;
        }

        IndexDef<T, ?> idx = indexByPropLower().get(pn);
        if (idx == null) return null;

        ArrayList<IndexPredicate> ips = new ArrayList<>();
        for (Eq<?, ?> e : eqs) {
            Object v = e.value();
            if (v == null) return null;
            byte[] b;
            try {
                b = idx.encodeAnyOrNull(v);
            } catch (ClassCastException ex) {
                return null;
            }
            if (b == null) return null;
            ips.add(new IndexPredicate.Eq(idx.name(), b));
        }

        Candidate c = new Candidate(null, 880 - ips.size());
        c.unionPredicates = ips;
        return c;
    }

    /** Returns false if any OR branch is not Eq (or nested Or-of-Eq). */
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

    private IndexPredicate toIndexRange(IndexDef<T, ?> idx, Range<? extends Serializable> r) {
        if (idx == null || r == null) return null;

        byte[] from = null, to = null;
        boolean fromIncl = false, toIncl = false;

        Bound loB = r.lowerBound();
        if (loB != Bound.UNBOUNDED) {
            Serializable lo = r.lower();
            try {
                from = idx.encodeAnyOrNull(lo);
            } catch (ClassCastException ex) {
                return null;
            }
            if (from == null) return null;
            fromIncl = (loB == Bound.INCLUSIVE);
        }

        Bound hiB = r.upperBound();
        if (hiB != Bound.UNBOUNDED) {
            Serializable hi = r.upper();
            try {
                to = idx.encodeAnyOrNull(hi);
            } catch (ClassCastException ex) {
                return null;
            }
            if (to == null) return null;
            toIncl = (hiB == Bound.INCLUSIVE);
        }

        if (from == null && to == null) return null;
        return new IndexPredicate.Range(idx.name(), from, fromIncl, to, toIncl);
    }

    private static final class Plan<K> {
        final List<Query<K>> queries;
        Plan(List<Query<K>> queries) {
            this.queries = Objects.requireNonNull(queries, "queries");
        }
    }

    private static final class Candidate {
        final IndexPredicate predicate; // may be null when unionPredicates used
        final int score;
        List<IndexPredicate> unionPredicates; // optional

        Candidate(IndexPredicate predicate, int score) {
            this.predicate = predicate;
            this.score = score;
        }
    }
}
