package org.github.dbjo.rdb.demo;

import org.github.dbjo.rdb.*;
import org.springframework.context.annotation.*;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;

@Configuration(proxyBeanMethods = false)
@EnableTransactionManagement
public class RocksDbSpringConfig {

    @Bean
    public PlatformTransactionManager transactionManager(RocksDbHandle h) {
        return new RocksDbTransactionManager(h.db());
    }

    @Bean
    public RocksSessions rocksSessions(RocksDbHandle h) {
        return new SpringRocksSessions(h.db());
    }

    @Bean
    public DaoRegistry daoRegistry(RocksDbHandle h) {
        return new DaoRegistry(h.db(), h.cfByName(), true);
    }
}
