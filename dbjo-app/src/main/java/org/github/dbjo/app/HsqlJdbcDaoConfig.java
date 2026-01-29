package org.github.dbjo.app;

import org.github.dbjo.generated.model.dao.jdbc.ClientJdbcDao;
import org.github.dbjo.generated.model.dao.jdbc.ProductJdbcDao;
import org.github.dbjo.generated.model.dao.jdbc.PurchaseJdbcDao;
import org.github.dbjo.meta.jdbc.DbDialect;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;

@Configuration(proxyBeanMethods = false)
public class HsqlJdbcDaoConfig {

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
}
