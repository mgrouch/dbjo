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

    private final RocksSession.IteratorHandle ih;
    private final RocksIterator it;
    private final ReadOptions ro;

    private final boolean indexScan;
    private final boolean descending;

    private int remaining;
    private boolean closed;

    // Bounds for iterator keys (raw key bytes)
    private final byte[] iterFrom;
    private final boolean iterFromInc;
    private final byte[] iterTo;
    private final boolean iterToInc;

    // Extra: for IndexEq stop condition when iterTo == null
    private final byte[] eqPrefixOrNull;

    // For index-range filtering
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
        this.indexScan = !q.indexPredicates().isEmpty();

        ColumnFamilyHandle scanCf;
        byte[] tmpEqPrefix = null;

        if (!indexScan) {
            // Primary scan over primary CF
            scanCf = primaryCf;

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

            this.idxValueFrom = null; this.idxValueFromInc = true;
            this.idxValueTo = null;   this.idxValueToInc = true;
        } else {
            // Index scan
            IndexPredicate p = q.indexPredicates().get(0);
            ColumnFamilyHandle idxCf = indexCfs.get(indexNameOf(p));
            if (idxCf == null) throw new IllegalArgumentException("Unknown index: " + indexNameOf(p));
            scanCf = idxCf;

            if (p instanceof IndexPredicate.Eq eq) {
                byte[] prefix = ByteArrays.concat(eq.valueKey(), SEP);
                this.iterFrom = prefix;
                this.iterFromInc = true;
                this.iterTo = ByteArrays.prefixEndExclusive(prefix);
                this.iterToInc = false;

                this.idxValueFrom = eq.valueKey(); this.idxValueFromInc = true;
                this.idxValueTo = eq.valueKey();   this.idxValueToInc = true;

                tmpEqPrefix = prefix;
            } else if (p instanceof IndexPredicate.Range r) {
                byte[] fromPrefix = ByteArrays.concat(r.from(), SEP);
                byte[] toPrefix = ByteArrays.concat(r.to(), SEP);

                this.iterFrom = fromPrefix;
                this.iterFromInc = true;
                this.iterTo = ByteArrays.prefixEndExclusive(toPrefix);
                this.iterToInc = false;

                this.idxValueFrom = r.from(); this.idxValueFromInc = r.fromInclusive();
                this.idxValueTo = r.to();     this.idxValueToInc = r.toInclusive();
            } else {
                throw new IllegalArgumentException("Unsupported predicate type: " + p.getClass().getName());
            }
        }

        this.eqPrefixOrNull = tmpEqPrefix;

        // Allocate iterator + read options together
        this.ih = session.openIterator(scanCf);
        this.it = ih.it();
        this.ro = ih.ro();

        if (!indexScan) seekPrimary();
        else seekIndex();
    }

    private static String indexNameOf(IndexPredicate p) {
        if (p instanceof IndexPredicate.Eq eq) return eq.indexName();
        if (p instanceof IndexPredicate.Range r) return r.indexName();
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
                it.seek(iterTo);
                if (!it.isValid()) it.seekToLast();
                else {
                    if (!iterToInc && ByteArrays.compare(it.key(), iterTo) == 0) it.prev();
                    else if (ByteArrays.compare(it.key(), iterTo) > 0) it.prev();
                }
            } else {
                it.seekToLast();
            }
        }
    }

    private void seekIndex() {
        if (!descending) {
            it.seek(iterFrom);
        } else {
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

            // Stop on iterator bounds (generic)
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
                K k = keyCodec.decodeKey(it.key());
                T v = valueCodec.decode(it.value());
                action.accept(Map.entry(k, v));

                if (!descending) it.next(); else it.prev();
                remaining--;
                return true;
            } else {
                // IndexEq correctness: if iterTo == null we must stop when prefix no longer matches
                if (eqPrefixOrNull != null && !startsWith(it.key(), eqPrefixOrNull)) {
                    close();
                    return false;
                }

                byte[] idxKey = it.key();
                int sepPos = ByteArrays.indexOf(idxKey, SEP);
                if (sepPos <= 0 || sepPos == idxKey.length - 1) {
                    if (!descending) it.next(); else it.prev();
                    continue;
                }

                byte[] valuePart = java.util.Arrays.copyOfRange(idxKey, 0, sepPos);

                // Range fence (also protects against iterTo == null cases)
                if (idxValueFrom != null) {
                    int cFrom = ByteArrays.compare(valuePart, idxValueFrom);
                    if (cFrom < 0 || (cFrom == 0 && !idxValueFromInc)) {
                        if (!descending) it.next(); else it.prev();
                        continue;
                    }
                }
                if (idxValueTo != null) {
                    int cTo = ByteArrays.compare(valuePart, idxValueTo);
                    if (cTo > 0 || (cTo == 0 && !idxValueToInc)) {
                        // In ascending order, once we pass upper bound we can stop.
                        if (!descending) { close(); return false; }
                        // In descending, keep stepping until we’re back in range.
                        it.prev();
                        continue;
                    }
                }

                byte[] pkBytes = java.util.Arrays.copyOfRange(idxKey, sepPos + 1, idxKey.length);

                try {
                    // Reuse same ReadOptions (snapshot-consistent and avoids per-row allocation)
                    byte[] vb = session.get(primaryCf, ro, pkBytes);
                    if (vb == null) {
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

    private static boolean startsWith(byte[] a, byte[] prefix) {
        if (a.length < prefix.length) return false;
        for (int i = 0; i < prefix.length; i++) {
            if (a[i] != prefix[i]) return false;
        }
        return true;
    }

    @Override public Spliterator<Map.Entry<K, T>> trySplit() { return null; }
    @Override public long estimateSize() { return remaining < 0 ? Long.MAX_VALUE : remaining; }
    @Override public int characteristics() { return ORDERED | NONNULL; }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            ih.close(); // closes iterator + ReadOptions
        }
    }
}
