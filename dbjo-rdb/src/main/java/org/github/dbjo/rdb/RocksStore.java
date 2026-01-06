package org.github.dbjo.rdb;

import org.rocksdb.*;

import java.nio.file.Path;
import java.util.*;

public final class RocksStore implements AutoCloseable {
    static { RocksDB.loadLibrary(); }

    private final RocksDB db;
    private final DBOptions dbOptions;
    private final List<ColumnFamilyHandle> handles = new ArrayList<>();
    private final Map<String, ColumnFamilyHandle> cfByName = new HashMap<>();

    public RocksStore(Path dir, Set<String> columnFamilies) {
        try {
            dbOptions = new DBOptions()
                    .setCreateIfMissing(true)
                    .setCreateMissingColumnFamilies(true);

            List<ColumnFamilyDescriptor> cfs = new ArrayList<>();
            // default CF is required
            cfs.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, new ColumnFamilyOptions()));
            for (String name : columnFamilies) {
                cfs.add(new ColumnFamilyDescriptor(name.getBytes(), new ColumnFamilyOptions()));
            }

            db = RocksDB.open(dbOptions, dir.toString(), cfs, handles);

            // map names
            cfByName.put("default", handles.get(0));
            for (int i = 1; i < cfs.size(); i++) {
                cfByName.put(new String(cfs.get(i).getName()), handles.get(i));
            }
        } catch (RocksDBException e) {
            throw new RocksDaoException("Failed to open RocksDB", e);
        }
    }

    public RocksDB db() { return db; }

    public ColumnFamilyHandle cf(String name) {
        ColumnFamilyHandle h = cfByName.get(name);
        if (h == null) throw new IllegalArgumentException("Unknown column family: " + name);
        return h;
    }

    @Override
    public void close() {
        // close handles first, then DB, then options
        for (ColumnFamilyHandle h : handles) h.close();
        db.close();
        dbOptions.close();
    }
}
