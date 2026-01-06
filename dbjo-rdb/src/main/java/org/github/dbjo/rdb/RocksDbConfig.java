package org.github.dbjo.rdb;

import org.rocksdb.*;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.*;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import java.nio.charset.StandardCharsets;
import java.util.*;

@Configuration
@EnableTransactionManagement
@EnableConfigurationProperties(RocksProps.class)
public class RocksDbConfig {

    static {
        RocksDB.loadLibrary();
    }

    @Bean(destroyMethod = "close")
    public RocksDbHandle rocksDbHandle(RocksProps props) throws RocksDBException {
        // Define CFs you want. For demo we hardcode: users + email index.
        List<ColumnFamilyDescriptor> cfs = List.of(
                new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, new ColumnFamilyOptions()),
                new ColumnFamilyDescriptor("users".getBytes(StandardCharsets.UTF_8), new ColumnFamilyOptions()),
                new ColumnFamilyDescriptor("users_email_idx".getBytes(StandardCharsets.UTF_8), new ColumnFamilyOptions())
        );

        var handles = new ArrayList<ColumnFamilyHandle>();

        DBOptions dbOpts = new DBOptions()
                .setCreateIfMissing(true)
                .setCreateMissingColumnFamilies(true);

        TransactionDBOptions txOpts = new TransactionDBOptions();

        TransactionDB db = TransactionDB.open(dbOpts, txOpts, props.path(), cfs, handles);

        // Map CF name -> handle (must keep handles for the lifetime of the DB)
        Map<String, ColumnFamilyHandle> byName = new HashMap<>();
        for (ColumnFamilyHandle h : handles) {
            byName.put(new String(h.getName(), StandardCharsets.UTF_8), h);
        }

        return new RocksDbHandle(db, dbOpts, txOpts, handles, Map.copyOf(byName));
    }

    @Bean
    public RocksDbTransactionManager rocksTxManager(RocksDbHandle h) {
        return new RocksDbTransactionManager(h.db());
    }

    @Bean
    public RocksSessions rocksSessions(RocksDbHandle h) {
        return new SpringRocksSessions(h.db());
    }

    @Bean
    public DaoRegistry daoRegistry() {
        return new DaoRegistry();
    }
}
