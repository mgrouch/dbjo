package org.github.dbjo.app.jdbc;

import org.github.dbjo.generated.model.rdb.jdbc.GeneratedRocksJdbcCatalog;
import org.github.dbjo.rdb.RocksProps;
import org.github.dbjo.rdb.jdbc.RocksJdbcEngine;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.sql.SQLException;

@Configuration
public class RocksJdbcRestConfiguration {

    @Bean(destroyMethod = "close")
    public RocksJdbcEngine rocksJdbcEngine(RocksProps rocksProps) throws SQLException {
        return new RocksJdbcEngine(new GeneratedRocksJdbcCatalog(), rocksProps.path(), false, true);
    }
}
