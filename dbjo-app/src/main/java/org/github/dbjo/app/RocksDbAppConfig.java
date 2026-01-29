package org.github.dbjo.app;

import org.github.dbjo.generated.model.dao.rdb.ClientDao;
import org.github.dbjo.generated.model.dao.rdb.ProductDao;
import org.github.dbjo.generated.model.dao.rdb.PurchaseDao;
import org.github.dbjo.generated.model.rdb.schema.ClientSchema;
import org.github.dbjo.generated.model.rdb.schema.ProductSchema;
import org.github.dbjo.generated.model.rdb.schema.PurchaseSchema;
import org.github.dbjo.rdb.DaoRegistry;
import org.github.dbjo.rdb.RocksDbBootstrap;
import org.github.dbjo.rdb.RocksDbHandle;
import org.github.dbjo.rdb.RocksDbTransactionManager;
import org.github.dbjo.rdb.RocksProps;
import org.github.dbjo.rdb.RocksSchema;
import org.github.dbjo.rdb.RocksSessions;
import org.github.dbjo.rdb.SpringRocksSessions;
import org.rocksdb.RocksDBException;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.List;

@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(RocksProps.class)
public class RocksDbAppConfig {

    @Bean
    public List<RocksSchema<?>> rocksSchemas() {
        return List.of(
                ClientSchema.INSTANCE,
                ProductSchema.INSTANCE,
                PurchaseSchema.INSTANCE
        );
    }

    @Bean
    public RocksDbHandle rocksDbHandle(RocksProps props, List<RocksSchema<?>> schemas) throws RocksDBException {
        return RocksDbBootstrap.open(props, schemas);
    }

    @Bean(name = "rocksTransactionManager")
    public PlatformTransactionManager rocksTransactionManager(RocksDbHandle h) {
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

    @Bean
    public ClientDao clientDao(RocksSessions sessions, DaoRegistry registry) {
        return new ClientDao(sessions, registry);
    }

    @Bean
    public ProductDao productDao(RocksSessions sessions, DaoRegistry registry) {
        return new ProductDao(sessions, registry);
    }

    @Bean
    public PurchaseDao purchaseDao(RocksSessions sessions, DaoRegistry registry) {
        return new PurchaseDao(sessions, registry);
    }

}
