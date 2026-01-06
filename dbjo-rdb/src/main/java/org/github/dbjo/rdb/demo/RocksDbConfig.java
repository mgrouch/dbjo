package org.github.dbjo.rdb.demo;

import org.github.dbjo.rdb.RocksDbHandle;
import org.github.dbjo.rdb.RocksProps;
import org.rocksdb.*;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.*;

import java.nio.charset.StandardCharsets;
import java.util.*;

@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(RocksProps.class)
public class RocksDbConfig {

    @Bean(destroyMethod = "close")
    public RocksDbHandle rocksDbHandle(RocksProps props) throws RocksDBException {
        RocksDB.loadLibrary();

        String path = props.path();

        // Keep these ALIVE for the lifetime of the DB (don’t use try-with-resources here)
        ColumnFamilyOptions cfOpts = new ColumnFamilyOptions();
        DBOptions dbOpts = new DBOptions()
                .setCreateIfMissing(true)
                .setCreateMissingColumnFamilies(true);

        TransactionDBOptions txOpts = new TransactionDBOptions();

        // IMPORTANT: first descriptor MUST be DEFAULT column family
        List<ColumnFamilyDescriptor> cfDescriptors = List.of(
                new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOpts),
                new ColumnFamilyDescriptor("users".getBytes(StandardCharsets.UTF_8), cfOpts),
                new ColumnFamilyDescriptor("users_email_idx".getBytes(StandardCharsets.UTF_8), cfOpts)
        );

        List<ColumnFamilyHandle> cfHandles = new ArrayList<>(cfDescriptors.size());

        TransactionDB db = TransactionDB.open(dbOpts, txOpts, path, cfDescriptors, cfHandles);

        // Map name -> handle (ensure "default" exists)
        Map<String, ColumnFamilyHandle> cfByName = new HashMap<>();
        for (int i = 0; i < cfDescriptors.size(); i++) {
            byte[] n = cfDescriptors.get(i).getName();
            String name = Arrays.equals(n, RocksDB.DEFAULT_COLUMN_FAMILY)
                    ? "default"
                    : new String(n, StandardCharsets.UTF_8);
            cfByName.put(name, cfHandles.get(i));
        }

        return new RocksDbHandle(db, dbOpts, txOpts, cfOpts, cfHandles, Map.copyOf(cfByName));
    }
}
