package org.github.dbjo.app;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.sql.init.dependency.DependsOnDatabaseInitialization;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.DependsOn;
import org.springframework.core.annotation.Order;
import org.springframework.core.Ordered;
import org.github.dbjo.generated.model.dao.jdbc.ClientJdbcDao;
import org.github.dbjo.generated.model.dao.jdbc.ProductJdbcDao;
import org.github.dbjo.generated.model.dao.jdbc.PurchaseJdbcDao;
import org.github.dbjo.generated.model.dao.rdb.ClientDao;
import org.github.dbjo.generated.model.dao.rdb.ProductDao;
import org.github.dbjo.generated.model.dao.rdb.PurchaseDao;
import org.github.dbjo.generated.model.rdb.schema.ClientSchema;
import org.github.dbjo.generated.model.rdb.schema.ProductSchema;
import org.github.dbjo.generated.model.rdb.schema.PurchaseSchema;
import org.github.dbjo.meta.jdbc.DbDialect;
import org.github.dbjo.rdb.DaoRegistry;
import org.github.dbjo.rdb.RocksDbHandle;
import org.github.dbjo.rdb.RocksDbTransactionManager;
import org.github.dbjo.rdb.RocksProps;
import org.github.dbjo.rdb.RocksSchema;
import org.github.dbjo.rdb.RocksSessions;
import org.github.dbjo.rdb.SpringRocksSessions;
import org.github.dbjo.rdb.RocksDbBootstrap;
import org.github.dbjo.rdb.jdbc.RocksJdbcEngine;
import org.rocksdb.RocksDBException;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;
import java.util.List;

@SpringBootApplication
@ConfigurationPropertiesScan(basePackageClasses = RocksProps.class)
public class DbjoAppApplication {

    public static void main(String[] args) {
        SpringApplication.run(DbjoAppApplication.class, args);
    }

    @Bean
    public List<RocksSchema<?>> rocksSchemas() {
        return List.of(
                ClientSchema.INSTANCE,
                ProductSchema.INSTANCE,
                PurchaseSchema.INSTANCE
        );
    }

    @Bean(destroyMethod = "close")
    public RocksDbHandle rocksDbHandle(RocksProps rocksProps, List<RocksSchema<?>> rocksSchemas)
            throws RocksDBException {
        return RocksDbBootstrap.open(rocksProps, rocksSchemas);
    }

    @Bean
    @Order(Ordered.HIGHEST_PRECEDENCE)
    CommandLineRunner hsqlScriptInitializer(DataSource dataSource) {
        return args -> HsqlScriptRunner.runScripts(
                dataSource,
                List.of("classpath:schema.sql", "classpath:data.sql")
        );
    }

    @Bean
    @DependsOnDatabaseInitialization
    @DependsOn("hsqlScriptInitializer")
    @Order(Ordered.LOWEST_PRECEDENCE)
    CommandLineRunner loadRocksDb(DataSource dataSource, RocksDbHandle rocksDbHandle, RocksJdbcEngine rocksJdbcEngine) {
        return args -> {
            PlatformTransactionManager transactionManager = new RocksDbTransactionManager(rocksDbHandle.db());
            TransactionTemplate rocksTransactionTemplate = new TransactionTemplate(transactionManager);
            RocksSessions sessions = new SpringRocksSessions(rocksDbHandle.db());
            DaoRegistry registry = new DaoRegistry(rocksDbHandle.db(), rocksDbHandle.cfByName(), true);

            ClientJdbcDao clientJdbcDao = new ClientJdbcDao(dataSource, DbDialect.HSQL);
            ProductJdbcDao productJdbcDao = new ProductJdbcDao(dataSource, DbDialect.HSQL);
            PurchaseJdbcDao purchaseJdbcDao = new PurchaseJdbcDao(dataSource, DbDialect.HSQL);

            ClientDao clientDao = new ClientDao(sessions, registry);
            ProductDao productDao = new ProductDao(sessions, registry);
            PurchaseDao purchaseDao = new PurchaseDao(sessions, registry);

            HsqlToRocksLoader loader = new HsqlToRocksLoader(
                    clientJdbcDao,
                    productJdbcDao,
                    purchaseJdbcDao,
                    clientDao,
                    productDao,
                    purchaseDao,
                    rocksTransactionTemplate
            );
            RocksJdbcReporter reporter = new RocksJdbcReporter(rocksJdbcEngine);
            loader.load();
            reporter.reportTables();
        };
    }
}
