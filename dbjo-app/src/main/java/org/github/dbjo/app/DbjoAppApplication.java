package org.github.dbjo.app;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.ConfigurableApplicationContext;
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
import org.github.dbjo.rdb.RocksDbBootstrap;
import org.github.dbjo.rdb.RocksDbHandle;
import org.github.dbjo.rdb.RocksDbTransactionManager;
import org.github.dbjo.rdb.RocksProps;
import org.github.dbjo.rdb.RocksSchema;
import org.github.dbjo.rdb.RocksSessions;
import org.github.dbjo.rdb.SpringRocksSessions;
import org.rocksdb.RocksDBException;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.List;

@SpringBootApplication
@ConfigurationPropertiesScan(basePackageClasses = RocksProps.class)
public class DbjoAppApplication {

    public static void main(String[] args) {
        try (ConfigurableApplicationContext app = SpringApplication.run(DbjoAppApplication.class, args)) {
            DataSource dataSource = app.getBean(DataSource.class);
            RocksProps rocksProps = app.getBean(RocksProps.class);
            List<RocksSchema<?>> schemas = List.of(
                    ClientSchema.INSTANCE,
                    ProductSchema.INSTANCE,
                    PurchaseSchema.INSTANCE
            );
            try (RocksDbHandle rocksDbHandle = RocksDbBootstrap.open(rocksProps, schemas)) {
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
                RocksJdbcReporter reporter = new RocksJdbcReporter(rocksProps);
                loader.load();
                reporter.reportTables();
            } catch (RocksDBException | SQLException e) {
                throw new IllegalStateException("Failed to initialize RocksDB wiring", e);
            }
        }
    }
}
