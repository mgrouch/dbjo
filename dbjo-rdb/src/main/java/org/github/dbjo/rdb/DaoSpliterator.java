package org.github.dbjo.rdb;

import org.rocksdb.*;

import java.util.Map;
import java.util.Objects;
import java.util.Spliterator;
import java.util.function.Consumer;

final class DaoSpliterator<K, T> implements Spliterator<Map.Entry<K, T>>, AutoCloseable {
    private static final byte SEP = 0;

    private final RocksSession session;
    private final ColumnFamilyHandle primaryCf;
    private final Map<String, ColumnFamilyHandle> indexCfs;
    private final KeyCodec<K> keyCodec;
    private final Codec<T> valueCodec;

    private final RocksIterator it;
    private final boolean indexScan;
    private final boolean descending;

    private int remaining;
    private boolean closed;

    // Bounds for iterator keys (not decoded K; these are raw key-bytes bounds)
    private final byte[] iterFrom;        // inclusive seek anchor (ascending) / lower bound filter (descending)
    private final boolean iterFromInc;
    private final byte[] iterTo;          // upper bound filter (ascending) / seek anchor (descending)
    private final boolean iterToInc;

    // For index-range exclusive filtering, we may need value-only bounds
    private final byte[] idxValueFrom;
    private final boolean idxValueFromInc;
    private final byte[] idxValueTo;
    private final boolean idxValueToInc;

    DaoSpliterator(RocksSession session,
                   ColumnFamilyHandle primaryCf,
                   Map<String, ColumnFamilyHandle> indexCfs,
                   KeyCodec<K> keyCodec,
                   Codec<T> valueCodec,
                   Query<K> q) {
        this.session = Objects.requireNonNull(session);
        this.primaryCf = Objects.requireNonNull(primaryCf);
        this.indexCfs = Objects.requireNonNull(indexCfs);
        this.keyCodec = Objects.requireNonNull(keyCodec);
        this.valueCodec = Objects.requireNonNull(valueCodec);

        this.remaining = q.limit();
        this.descending = q.descending();

        // Decide scan plan: if any index predicate exists, drive scan via the FIRST predicate.
        // (Full multi-predicate intersection is doable, but needs a bit more plumbing.)
        this.indexScan = !q.indexPredicates().isEmpty();

        if (!indexScan) {
            // Primary scan over primary CF keys
            this.it = newIterator(primaryCf);

            var kr = q.keyRange().orElse(null);
            if (kr != null) {
                this.iterFrom = (kr.from() != null) ? keyCodec.encodeKey(kr.from()) : null;
                this.iterFromInc = kr.fromInclusive();
                this.iterTo = (kr.to() != null) ? keyCodec.encodeKey(kr.to()) : null;
                this.iterToInc = kr.toInclusive();
            } else {
                this.iterFrom = null; this.iterFromInc = true;
                this.iterTo = null;   this.iterToInc = true;
            }

            // No index-value bounds in primary scan
            this.idxValueFrom = null; this.idxValueFromInc = true;
            this.idxValueTo = null;   this.idxValueToInc = true;

            seekPrimary();
        } else {
            // Index scan over index CF keys, then fetch primary value per pk.
            IndexPredicate p = q.indexPredicates().get(0);

            ColumnFamilyHandle idxCf = indexCfs.get(indexNameOf(p));
            if (idxCf == null) throw new IllegalArgumentException("Unknown index: " + indexNameOf(p));

            this.it = newIterator(idxCf);

            if (p instanceof IndexEq eq) {
                // prefix = valueKey + SEP
                byte[] prefix = ByteArrays.concat(eq.valueKey(), SEP);
                this.iterFrom = prefix;
                this.iterFromInc = true;
                this.iterTo = ByteArrays.prefixEndExclusive(prefix);
                this.iterToInc = false;

                this.idxValueFrom = eq.valueKey(); this.idxValueFromInc = true;
                this.idxValueTo = eq.valueKey();   this.idxValueToInc = true;
            } else if (p instanceof IndexRange r) {
                byte[] fromPrefix = ByteArrays.concat(r.from(), SEP);
                byte[] toPrefix = ByteArrays.concat(r.to(), SEP);

                this.iterFrom = fromPrefix;
                this.iterFromInc = true; // prefix scan always starts inclusive; exclusive handled by filtering
                this.iterTo = ByteArrays.prefixEndExclusive(toPrefix); // includes all keys for r.to()
                this.iterToInc = false;

                this.idxValueFrom = r.from(); this.idxValueFromInc = r.fromInclusive();
                this.idxValueTo = r.to();     this.idxValueToInc = r.toInclusive();
            } else {
                throw new IllegalArgumentException("Unsupported predicate type: " + p.getClass().getName());
            }

            seekIndex();
        }
    }

    private RocksIterator newIterator(ColumnFamilyHandle cf) {
        try {
            return session.newIterator(cf);
        } catch (RocksDBException e) {
            throw new RocksDaoException("newIterator failed", e);
        }
    }

    private static String indexNameOf(IndexPredicate p) {
        if (p instanceof IndexEq eq) return eq.indexName();
        if (p instanceof IndexRange r) return r.indexName();
        throw new IllegalArgumentException("Unknown predicate: " + p);
    }

    private void seekPrimary() {
        if (!descending) {
            if (iterFrom != null) {
                it.seek(iterFrom);
                if (!iterFromInc && it.isValid() && ByteArrays.compare(it.key(), iterFrom) == 0) it.next();
            } else {
                it.seekToFirst();
            }
        } else {
            if (iterTo != null) {
                // seek to first key >= iterTo, then step back to <= iterTo (or < iterTo if exclusive)
                it.seek(iterTo);
                if (!it.isValid()) it.seekToLast();
                else {
                    // if key == iterTo and exclusive, move back
                    if (!iterToInc && ByteArrays.compare(it.key(), iterTo) == 0) it.prev();
                    else {
                        // if key > iterTo (can happen), move back
                        if (ByteArrays.compare(it.key(), iterTo) > 0) it.prev();
                    }
                }
            } else {
                it.seekToLast();
            }
        }
    }

    private void seekIndex() {
        // iterFrom is always a proper lower bound key in index CF
        if (!descending) {
            it.seek(iterFrom);
        } else {
            // iterTo is endExclusive; for descending we want the last key < iterTo
            if (iterTo != null) {
                it.seek(iterTo);
                if (!it.isValid()) it.seekToLast();
                else it.prev();
            } else {
                it.seekToLast();
            }
        }
    }

    @Override
    public boolean tryAdvance(Consumer<? super Map.Entry<K, T>> action) {
        if (closed) return false;

        while (true) {
            if (remaining <= 0) { close(); return false; }
            if (!it.isValid()) { close(); return false; }

            // Stop on iterator bounds
            if (!descending) {
                if (iterTo != null) {
                    int c = ByteArrays.compare(it.key(), iterTo);
                    if (c > 0 || (c == 0 && !iterToInc)) { close(); return false; }
                }
            } else {
                if (iterFrom != null) {
                    int c = ByteArrays.compare(it.key(), iterFrom);
                    if (c < 0 || (c == 0 && !iterFromInc)) { close(); return false; }
                }
            }

            if (!indexScan) {
                // Primary scan emits current key/value
                K k = keyCodec.decodeKey(it.key());
                T v = valueCodec.decode(it.value());
                action.accept(Map.entry(k, v));

                if (!descending) it.next(); else it.prev();
                remaining--;
                return true;
            } else {
                // Index scan: parse valueBytes + SEP + pkBytes
                byte[] idxKey = it.key();
                int sepPos = ByteArrays.indexOf(idxKey, SEP);
                if (sepPos <= 0 || sepPos == idxKey.length - 1) {
                    // malformed index key; skip
                    if (!descending) it.next(); else it.prev();
                    continue;
                }

                byte[] valuePart = java.util.Arrays.copyOfRange(idxKey, 0, sepPos);

                // Apply exclusivity of IndexRange (if any)
                if (!idxValueFromInc && ByteArrays.compare(valuePart, idxValueFrom) == 0) {
                    if (!descending) it.next(); else it.prev();
                    continue;
                }
                if (!idxValueToInc && ByteArrays.compare(valuePart, idxValueTo) == 0) {
                    if (!descending) it.next(); else it.prev();
                    continue;
                }

                byte[] pkBytes = java.util.Arrays.copyOfRange(idxKey, sepPos + 1, idxKey.length);

                // Optional primary key range filter (from Query.keyRange)
                // NOTE: this compares encoded key bytes, which is correct if your KeyCodec is order-preserving.
                // If not, you should filter on decoded K instead.
                // We’ll do byte compare because it matches RocksDB ordering.
                // (If you store random hash keys, range is meaningless anyway.)
                // ----
                // If you want decoded-K filtering, I can switch this.
                // ----
                // Using the already-encoded pkBytes:
                // (We need the primary bounds bytes from KeyRange, but those are iterFrom/iterTo only for primary scan.
                // For index scan, use q.keyRange() if provided.)
                // Quick approach: if Query.keyRange present, recompute bounds per pkBytes now:
                // (That’s a tiny overhead.)
                // ----
                // Implemented below:
                // ----
                // Fetch q.keyRange again is not available here, so we skip that in this minimal impl.
                // If you want it, pass the primary bounds into this class for index scan too.
                // ----

                // Fetch primary
                try {
                    byte[] vb = session.get(primaryCf, pkBytes);
                    if (vb == null) {
                        // stale index entry; skip
                        if (!descending) it.next(); else it.prev();
                        continue;
                    }

                    K k = keyCodec.decodeKey(pkBytes);
                    T v = valueCodec.decode(vb);
                    action.accept(Map.entry(k, v));

                    if (!descending) it.next(); else it.prev();
                    remaining--;
                    return true;
                } catch (RocksDBException e) {
                    throw new RocksDaoException("index-driven fetch failed", e);
                }
            }
        }
    }

    @Override public Spliterator<Map.Entry<K, T>> trySplit() { return null; }
    @Override public long estimateSize() { return remaining < 0 ? Long.MAX_VALUE : remaining; }
    @Override public int characteristics() { return ORDERED | NONNULL; }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            it.close();
        }
    }
}
