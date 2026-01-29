package org.github.dbjo.rdb;

import org.github.dbjo.criteria.*;
import org.github.dbjo.meta.entity.PropertyMeta;
import org.github.dbjo.rdb.criteria.CriteriaIndexPlanner;
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
    private static final int MAX_IN_PUSHDOWN = CriteriaIndexPlanner.DEFAULT_MAX_IN_PUSHDOWN;
    private static final int MAX_OR_EQ_PUSHDOWN = CriteriaIndexPlanner.DEFAULT_MAX_OR_EQ_PUSHDOWN;

    // ✅ IndexDef has 2 type params
    private final List<IndexDef<T, ?>> indexes;

    // cached lookup: propertyName (lower) -> indexdef
    private volatile Map<String, IndexDef<T, ?>> indexByPropLower;
    private volatile Map<String, CriteriaIndexPlanner.IndexInfo> indexInfoByPropLower;

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

    private Map<String, CriteriaIndexPlanner.IndexInfo> indexInfoByPropLower() {
        Map<String, CriteriaIndexPlanner.IndexInfo> m = indexInfoByPropLower;
        if (m != null) return m;

        Map<String, IndexDef<T, ?>> defs = indexByPropLower();
        HashMap<String, CriteriaIndexPlanner.IndexInfo> out = new HashMap<>();
        for (Map.Entry<String, IndexDef<T, ?>> e : defs.entrySet()) {
            IndexDef<T, ?> idx = e.getValue();
            if (idx == null) continue;
            out.put(e.getKey(), new CriteriaIndexPlanner.IndexInfo(idx.name(), idx::encodeAnyOrNull));
        }
        indexInfoByPropLower = out;
        return out;
    }

    private Plan<K> plan(org.github.dbjo.criteria.Query<?> cq) {
        CriteriaIndexPlanner.Candidate scanCand  = candidateFromScan(cq.scan());
        CriteriaIndexPlanner.Candidate whereCand = candidateFromWhere(cq.where());

        CriteriaIndexPlanner.Candidate best = better(scanCand, whereCand);

        if (best == null) {
            Query.Builder<K> b = Query.builder();
            if (cq.limit() != null) b.limit(cq.limit());
            return new Plan<>(List.of(b.build()));
        }

        // union case: multiple Rocks queries (each with exactly one IndexPredicate)
        if (!best.unionPredicates().isEmpty()) {
            ArrayList<Query<K>> qs = new ArrayList<>(best.unionPredicates().size());
            for (IndexPredicate ip : best.unionPredicates()) {
                Query.Builder<K> b = Query.<K>builder().where(ip);
                if (cq.limit() != null) b.limit(cq.limit());
                qs.add(b.build());
            }
            return new Plan<>(qs);
        }

        Query.Builder<K> b = Query.<K>builder().where(best.predicate());
        if (cq.limit() != null) b.limit(cq.limit());
        return new Plan<>(List.of(b.build()));
    }

    private static CriteriaIndexPlanner.Candidate better(CriteriaIndexPlanner.Candidate a, CriteriaIndexPlanner.Candidate b) {
        if (a == null) return b;
        if (b == null) return a;
        return (b.score() > a.score()) ? b : a;
    }

    private CriteriaIndexPlanner.Candidate candidateFromScan(Scan<?, ? extends Serializable> scan) {
        if (scan == null) return null;

        PropertyMeta<?, ?> prop = scan.prop();
        if (prop == null) return null;

        String pn = prop.getPropertyName();
        if (pn == null || pn.isBlank()) return null;

        IndexDef<T, ?> idx = indexByPropLower().get(pn.trim().toLowerCase(Locale.ROOT));
        if (idx == null) return null;

        IndexPredicate ip = toIndexRange(idx, scan.range());
        if (ip == null) return null;

        return CriteriaIndexPlanner.Candidate.of(ip, 700);
    }

    private CriteriaIndexPlanner.Candidate candidateFromWhere(Condition<?> where) {
        if (where == null) return null;
        return CriteriaIndexPlanner.bestCandidate(where, indexInfoByPropLower(), MAX_IN_PUSHDOWN, MAX_OR_EQ_PUSHDOWN);
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
}
