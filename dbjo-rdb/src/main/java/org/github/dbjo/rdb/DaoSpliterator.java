package org.github.dbjo.rdb;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.util.Map;
import java.util.Spliterator;
import java.util.function.Consumer;

final class DaoSpliterator<K, T> implements Spliterator<Map.Entry<K, T>>, AutoCloseable {
    private final RocksIterator it;
    private final KeyCodec<K> keyCodec;
    private final Codec<T> valueCodec;

    private final byte[] endExclusive; // null => no end bound
    private int remaining;
    private final boolean descending;
    private boolean closed;

    DaoSpliterator(RocksSession session,
                   ColumnFamilyHandle cf,
                   KeyCodec<K> keyCodec,
                   Codec<T> valueCodec,
                   Query<K> q) throws RocksDBException {
        this.it = session.newIterator(cf);
        this.keyCodec = keyCodec;
        this.valueCodec = valueCodec;
        this.remaining = q.limit();
        this.descending = q.descending();

        byte[] start = null;
        byte[] end = null;

        if (q.range() != null) {
            if (q.range().from() != null) start = keyCodec.encodeKey(q.range().from());
            if (q.range().to() != null) end = keyCodec.encodeKey(q.range().to());
        }

        this.endExclusive = end;

        if (!descending) {
            if (start != null) it.seek(start);
            else it.seekToFirst();
        } else {
            if (end != null) {
                // seekForPrev positions at <= end; we treat end as exclusive, so we’ll step back if equal
                it.seekForPrev(end);
                if (it.isValid() && ByteArrays.compare(it.key(), end) >= 0) it.prev();
            } else {
                it.seekToLast();
            }
        }
    }

    @Override
    public boolean tryAdvance(Consumer<? super Map.Entry<K, T>> action) {
        if (closed) return false;
        if (remaining <= 0) { close(); return false; }
        if (!it.isValid()) { close(); return false; }

        if (!descending && endExclusive != null && ByteArrays.compare(it.key(), endExclusive) >= 0) {
            close();
            return false;
        }

        K k = keyCodec.decodeKey(it.key());
        T v = valueCodec.decode(it.value());
        action.accept(Map.entry(k, v));

        if (!descending) it.next();
        else it.prev();

        remaining--;
        return true;
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
