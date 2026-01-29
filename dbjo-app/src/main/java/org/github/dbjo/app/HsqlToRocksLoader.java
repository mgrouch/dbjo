package org.github.dbjo.app;

import org.github.dbjo.generated.model.dao.jdbc.ClientJdbcDao;
import org.github.dbjo.generated.model.dao.jdbc.ProductJdbcDao;
import org.github.dbjo.generated.model.dao.jdbc.PurchaseJdbcDao;
import org.github.dbjo.generated.model.dao.rdb.ClientDao;
import org.github.dbjo.generated.model.dao.rdb.ProductDao;
import org.github.dbjo.generated.model.dao.rdb.PurchaseDao;
import org.github.dbjo.generated.model.entity.Client;
import org.github.dbjo.generated.model.entity.Product;
import org.github.dbjo.generated.model.entity.Purchase;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

import java.sql.SQLException;
import java.util.function.BiConsumer;
import java.util.function.Function;

@Service
public record HsqlToRocksLoader(ClientJdbcDao clientJdbcDao, ProductJdbcDao productJdbcDao,
                                PurchaseJdbcDao purchaseJdbcDao, ClientDao clientDao, ProductDao productDao,
                                PurchaseDao purchaseDao, TransactionTemplate rocksTransactionTemplate) {

    public void load() {
        rocksTransactionTemplate.executeWithoutResult(status -> {
            loadClients();
            loadProducts();
            loadPurchases();
        });
    }

    private void loadClients() {
        loadEntities("clients", clientJdbcDao::selectAll, clientDao::upsert, Client::getId);
    }

    private void loadProducts() {
        loadEntities("products", productJdbcDao::selectAll, productDao::upsert, Product::getId);
    }

    private void loadPurchases() {
        loadEntities("purchases", purchaseJdbcDao::selectAll, purchaseDao::upsert, Purchase::getId);
    }

    private <T> void loadEntities(String label, SqlSupplier<? extends Iterable<T>> loader,
                                  BiConsumer<Long, T> upsert, Function<T, Long> idProvider) {
        try {
            for (T entity : loader.get()) {
                upsert.accept(idProvider.apply(entity), entity);
            }
        } catch (SQLException ex) {
            throw new IllegalStateException("Failed to load " + label + " from HSQL", ex);
        }
    }

    @FunctionalInterface
    private interface SqlSupplier<T> {
        T get() throws SQLException;
    }
}
