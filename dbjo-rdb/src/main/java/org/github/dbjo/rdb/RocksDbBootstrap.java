package org.github.dbjo.rdb;

import org.rocksdb.*;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.*;

public final class RocksDbBootstrap {
    private RocksDbBootstrap() {}

    public static RocksDbHandle open(RocksProps props, List<RocksSchema> schemas) throws RocksDBException {
        RocksDB.loadLibrary();

        if (props.wipeOnStart()) {
            destroyIfExists(Path.of(props.path()));
        }

        // collect CF names (dedupe + stable order)
        LinkedHashSet<String> names = new LinkedHashSet<>();
        names.add("default"); // always
        for (RocksSchema s : schemas) {
            for (String cf : s.columnFamilies()) {
                if (cf != null && !cf.isBlank() && !"default".equals(cf)) names.add(cf);
            }
        }

        // keep these alive for DB lifetime (RocksDbHandle closes them)
        ColumnFamilyOptions cfOpts = new ColumnFamilyOptions();
        DBOptions dbOpts = new DBOptions()
                .setCreateIfMissing(true)
                .setCreateMissingColumnFamilies(true);

        TransactionDBOptions txOpts = new TransactionDBOptions();

        List<ColumnFamilyDescriptor> desc = new ArrayList<>();
        desc.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOpts));
        for (String n : names) {
            if ("default".equals(n)) continue;
            desc.add(new ColumnFamilyDescriptor(n.getBytes(StandardCharsets.UTF_8), cfOpts));
        }

        List<ColumnFamilyHandle> handles = new ArrayList<>(desc.size());
        TransactionDB db = TransactionDB.open(dbOpts, txOpts, props.path(), desc, handles);

        Map<String, ColumnFamilyHandle> cfByName = new HashMap<>();
        for (int i = 0; i < desc.size(); i++) {
            byte[] raw = desc.get(i).getName();
            String n = Arrays.equals(raw, RocksDB.DEFAULT_COLUMN_FAMILY)
                    ? "default"
                    : new String(raw, StandardCharsets.UTF_8);
            cfByName.put(n, handles.get(i));
        }

        return new RocksDbHandle(db, dbOpts, txOpts, cfOpts, handles, Map.copyOf(cfByName));
    }

    private static void destroyIfExists(Path dir) {
        try (Options opt = new Options()) {
            RocksDB.destroyDB(dir.toString(), opt);
        } catch (RocksDBException ignore) {
        }
    }
}
