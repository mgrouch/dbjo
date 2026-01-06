package org.github.dbjo.rdb;

import org.rocksdb.*;

import java.util.*;
import java.util.function.Consumer;

public abstract class AbstractRocksDao<T, K> implements Dao<T, K> {
    protected final RocksDB db;
    protected final ColumnFamilyHandle cf; // can be default CF too
    protected final KeyCodec<K> keyCodec;
    protected final Codec<T> valueCodec;

    protected final ReadOptions readOptions;
    protected final WriteOptions writeOptions;

    protected AbstractRocksDao(
            RocksDB db,
            ColumnFamilyHandle cf,
            KeyCodec<K> keyCodec,
            Codec<T> valueCodec,
            ReadOptions readOptions,
            WriteOptions writeOptions
    ) {
        this.db = Objects.requireNonNull(db);
        this.cf = Objects.requireNonNull(cf);
        this.keyCodec = Objects.requireNonNull(keyCodec);
        this.valueCodec = Objects.requireNonNull(valueCodec);
        this.readOptions = (readOptions != null) ? readOptions : new ReadOptions();
        this.writeOptions = (writeOptions != null) ? writeOptions : new WriteOptions();
    }

    @Override
    public Optional<T> get(K key) {
        Objects.requireNonNull(key);
        try {
            byte[] k = keyCodec.encodeKey(key);
            byte[] v = db.get(cf, readOptions, k);
            if (v == null) return Optional.empty();
            return Optional.ofNullable(valueCodec.decode(v));
        } catch (RocksDBException e) {
            throw new RocksDaoException("get failed", e);
        }
    }

    @Override
    public boolean containsKey(K key) {
        return get(key).isPresent();
    }

    @Override
    public void put(K key, T value) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(value);

        // Optional hook: maintain secondary indexes
        // We do: read old value (if any), compute index deltas.
        Optional<T> old = get(key);

        try (WriteBatch batch = new WriteBatch()) {
            byte[] k = keyCodec.encodeKey(key);
            byte[] v = valueCodec.encode(value);
            batch.put(cf, k, v);

            if (old.isPresent()) {
                onUpdateIndexes(batch, key, old.get(), value);
            } else {
                onInsertIndexes(batch, key, value);
            }

            db.write(writeOptions, batch);
        } catch (RocksDBException e) {
            throw new RocksDaoException("put failed", e);
        }
    }

    @Override
    public boolean delete(K key) {
        Objects.requireNonNull(key);

        Optional<T> old = get(key);
        if (old.isEmpty()) return false;

        try (WriteBatch batch = new WriteBatch()) {
            byte[] k = keyCodec.encodeKey(key);
            batch.delete(cf, k);

            onDeleteIndexes(batch, key, old.get());

            db.write(writeOptions, batch);
            return true;
        } catch (RocksDBException e) {
            throw new RocksDaoException("delete failed", e);
        }
    }

    @Override
    public Map<K, T> getAll(Collection<K> keys) {
        Objects.requireNonNull(keys);
        if (keys.isEmpty()) return Map.of();

        try {
            // RocksJava multiGet requires list of byte[] keys
            List<byte[]> keyBytes = new ArrayList<>(keys.size());
            List<K> keyList = new ArrayList<>(keys.size());
            for (K k : keys) {
                keyList.add(k);
                keyBytes.add(keyCodec.encodeKey(k));
            }

            Map<byte[], byte[]> raw = db.multiGetAsList(
                    Collections.nCopies(keyBytes.size(), cf),
                    keyBytes
            ).stream().collect(HashMap::new, (m, v) -> {
                // multiGetAsList returns values list, so we handle below; easiest is manual loop.
            }, HashMap::putAll);

            // The above stream approach is awkward; do manual:
            List<byte[]> values = db.multiGetAsList(Collections.nCopies(keyBytes.size(), cf), keyBytes);
            Map<K, T> out = new LinkedHashMap<>();
            for (int i = 0; i < keyList.size(); i++) {
                byte[] v = values.get(i);
                if (v != null) out.put(keyList.get(i), valueCodec.decode(v));
            }
            return out;
        } catch (RocksDBException e) {
            throw new RocksDaoException("getAll failed", e);
        }
    }

    @Override
    public void putAll(Map<K, T> entries) {
        Objects.requireNonNull(entries);
        if (entries.isEmpty()) return;

        try (WriteBatch batch = new WriteBatch()) {
            // If you need index maintenance in bulk, you can:
            // 1) multiGet old values
            // 2) apply deltas
            // For simplicity, this bulk put does NOT diff old vs new; override if needed.
            for (var e : entries.entrySet()) {
                byte[] k = keyCodec.encodeKey(e.getKey());
                byte[] v = valueCodec.encode(e.getValue());
                batch.put(cf, k, v);
            }
            db.write(writeOptions, batch);
        } catch (RocksDBException e) {
            throw new RocksDaoException("putAll failed", e);
        }
    }

    @Override
    public void forEach(Consumer<Map.Entry<K, T>> consumer) {
        Objects.requireNonNull(consumer);
        try (RocksIterator it = db.newIterator(cf, readOptions)) {
            for (it.seekToFirst(); it.isValid(); it.next()) {
                K k = keyCodec.decodeKey(it.key());
                T v = valueCodec.decode(it.value());
                consumer.accept(Map.entry(k, v));
            }
        }
    }

    @Override
    public long approximateSize() {
        // Could be improved with RocksDB properties / estimate
        // If you need accurate counts, store a counter key and update it transactionally.
        return -1;
    }

    // ---- Secondary index hooks (optional) ----
    // Override these in concrete DAOs if you want indexes.
    protected void onInsertIndexes(WriteBatch batch, K key, T value) throws RocksDBException {}
    protected void onUpdateIndexes(WriteBatch batch, K key, T oldValue, T newValue) throws RocksDBException {}
    protected void onDeleteIndexes(WriteBatch batch, K key, T oldValue) throws RocksDBException {}

    @Override
    public void close() {
        // DO NOT close RocksDB here if it's shared across DAOs.
        // Close per-DAO options if you own them:
        // readOptions.close(); writeOptions.close();
    }
}
