package org.github.dbjo.app;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.ApplicationArguments;
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
    public PlatformTransactionManager rocksTransactionManager(RocksDbHandle rocksDbHandle) {
        return new RocksDbTransactionManager(rocksDbHandle.db());
    }

    @Bean
    public TransactionTemplate rocksTransactionTemplate(PlatformTransactionManager rocksTransactionManager) {
        return new TransactionTemplate(rocksTransactionManager);
    }

    @Bean
    public RocksSessions rocksSessions(RocksDbHandle rocksDbHandle) {
        return new SpringRocksSessions(rocksDbHandle.db());
    }

    @Bean
    public DaoRegistry daoRegistry(RocksDbHandle rocksDbHandle) {
        return new DaoRegistry(rocksDbHandle.db(), rocksDbHandle.cfByName(), true);
    }

    @Bean
    public ClientJdbcDao clientJdbcDao(DataSource dataSource) {
        return new ClientJdbcDao(dataSource, DbDialect.HSQL);
    }

    @Bean
    public ProductJdbcDao productJdbcDao(DataSource dataSource) {
        return new ProductJdbcDao(dataSource, DbDialect.HSQL);
    }

    @Bean
    public PurchaseJdbcDao purchaseJdbcDao(DataSource dataSource) {
        return new PurchaseJdbcDao(dataSource, DbDialect.HSQL);
    }

    @Bean
    public ClientDao clientDao(RocksSessions rocksSessions, DaoRegistry daoRegistry) {
        return new ClientDao(rocksSessions, daoRegistry);
    }

    @Bean
    public ProductDao productDao(RocksSessions rocksSessions, DaoRegistry daoRegistry) {
        return new ProductDao(rocksSessions, daoRegistry);
    }

    @Bean
    public PurchaseDao purchaseDao(RocksSessions rocksSessions, DaoRegistry daoRegistry) {
        return new PurchaseDao(rocksSessions, daoRegistry);
    }

    @Bean
    public HsqlToRocksLoader hsqlToRocksLoader(ClientJdbcDao clientJdbcDao, ProductJdbcDao productJdbcDao,
                                               PurchaseJdbcDao purchaseJdbcDao, ClientDao clientDao,
                                               ProductDao productDao, PurchaseDao purchaseDao,
                                               TransactionTemplate rocksTransactionTemplate,
                                               ApplicationArguments applicationArguments) {
        PartitionArgs partitionArgs = PartitionArgs.from(applicationArguments);
        return new HsqlToRocksLoader(
                clientJdbcDao,
                productJdbcDao,
                purchaseJdbcDao,
                clientDao,
                productDao,
                purchaseDao,
                rocksTransactionTemplate,
                partitionArgs.partitionNum(),
                partitionArgs.totalPartitions()
        );
    }

    @Bean
    public RocksJdbcReporter rocksJdbcReporter(RocksJdbcEngine rocksJdbcEngine) {
        return new RocksJdbcReporter(rocksJdbcEngine);
    }

    @Bean
    @Order(Ordered.HIGHEST_PRECEDENCE)
    CommandLineRunner hsqlScriptInitializer(DataSource dataSource) {
        return args -> HsqlScriptRunner.runScripts(
                dataSource,
                List.of("classpath:functions.sql", "classpath:schema.sql", "classpath:data.sql")
        );
    }

    @Bean
    @DependsOnDatabaseInitialization
    @DependsOn("hsqlScriptInitializer")
    @Order(Ordered.LOWEST_PRECEDENCE)
    CommandLineRunner loadRocksDb(HsqlToRocksLoader loader, RocksJdbcReporter reporter) {
        return args -> {
            loader.load();
            reporter.reportTables();
        };
    }
}
