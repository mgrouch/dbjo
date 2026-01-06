package org.github.dbjo.rdb;

final class DaoSpliterator<K, T> implements java.util.Spliterator<java.util.Map.Entry<K, T>>, AutoCloseable {
    private final org.rocksdb.RocksIterator it;
    private final int limit;
    private int produced = 0;
    private boolean closed = false;

    DaoSpliterator(org.rocksdb.RocksIterator it, int limit) {
        this.it = it;
        this.limit = limit;
        this.it.seekToFirst(); // or seek(startKeyBytes)
    }

    @Override
    public boolean tryAdvance(java.util.function.Consumer<? super java.util.Map.Entry<K, T>> action) {
        if (closed) return false;
        if (produced >= limit) { close(); return false; }
        if (!it.isValid()) { close(); return false; }

        // decode key/value here:
        // K k = keyCodec.decodeKey(it.key());
        // T v = valueCodec.decode(it.value());

        // action.accept(java.util.Map.entry(k, v));

        it.next();
        produced++;
        return true;
    }

    @Override public java.util.Spliterator<java.util.Map.Entry<K, T>> trySplit() { return null; } // sequential scan
    @Override public long estimateSize() { return Long.MAX_VALUE; }
    @Override public int characteristics() {
        return ORDERED | NONNULL; // usually
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            it.close();
        }
    }
}
