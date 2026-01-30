package org.github.dbjo.app.jdbc;

import org.github.dbjo.generated.model.rdb.jdbc.GeneratedRocksJdbcCatalog;
import org.github.dbjo.rdb.RocksDbHandle;
import org.github.dbjo.rdb.jdbc.RocksJdbcEngine;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RocksJdbcRestConfiguration {

    @Bean(destroyMethod = "close")
    public RocksJdbcEngine rocksJdbcEngine(RocksDbHandle rocksDbHandle) {
        return new RocksJdbcEngine(new GeneratedRocksJdbcCatalog(), rocksDbHandle.db(), rocksDbHandle.cfByName());
    }
}
