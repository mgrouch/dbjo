package org.github.dbjo.rdb.demo;

import org.github.dbjo.rdb.*;
import org.rocksdb.RocksDBException;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.*;

import java.util.List;

@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(RocksProps.class)
public class RocksDbConfig {

    @Bean(destroyMethod = "close")
    public RocksDbHandle rocksDbHandle(RocksProps props, List<RocksSchema> schemas) throws RocksDBException {
        return RocksDbBootstrap.open(props, schemas);
    }

    // Demo contributes its CF list in one line:
    @Bean
    public RocksSchema userCfSchema() {
        return RocksSchema.of(UserSchema.USERS_CF, UserSchema.IDX_EMAIL);
    }
}
