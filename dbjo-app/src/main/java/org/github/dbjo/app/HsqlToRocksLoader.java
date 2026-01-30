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
import org.github.dbjo.meta.jdbc.LoaderUtil;
import org.springframework.transaction.support.TransactionTemplate;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

public record HsqlToRocksLoader(ClientJdbcDao clientJdbcDao, ProductJdbcDao productJdbcDao,
                                PurchaseJdbcDao purchaseJdbcDao, ClientDao clientDao, ProductDao productDao,
                                PurchaseDao purchaseDao, TransactionTemplate rocksTransactionTemplate) {

    public void load() {
        rocksTransactionTemplate.executeWithoutResult(status -> {
            LoaderUtil.loadEntities("clients", clientJdbcDao::selectAll, clientDao::upsert, Client::getId);
            LoaderUtil.loadEntities("products", productJdbcDao::selectAll, productDao::upsert, Product::getId);
            List<Purchase> purchases = loadEntities("purchases", purchaseJdbcDao::selectAll);
            for (Purchase purchase : purchases) {
                purchaseDao.upsert(purchase.getId(), purchase);
            }
            if (purchases.isEmpty()) {
                seedFallbackPurchase();
            }
        });
    }

    private void seedFallbackPurchase() {
        List<Client> clients = loadEntities("clients", clientJdbcDao::selectAll);
        List<Product> products = loadEntities("products", productJdbcDao::selectAll);
        if (clients.isEmpty() || products.isEmpty()) {
            return;
        }
        Client client = clients.get(0);
        Product product = products.get(0);
        Purchase purchase = new Purchase();
        purchase.setId(1L);
        purchase.setClientId(client.getId());
        purchase.setProductId(product.getId());
        purchase.setQty(1);
        purchase.setOrderedAt(new Timestamp(System.currentTimeMillis()));
        purchaseDao.upsert(purchase.getId(), purchase);
    }

    private static <T> List<T> loadEntities(String label, LoaderUtil.SqlSupplier<? extends Iterable<T>> loader) {
        try {
            List<T> items = new ArrayList<>();
            for (T item : loader.get()) {
                items.add(item);
            }
            return items;
        } catch (Exception ex) {
            throw new IllegalStateException("Failed to load " + label + " from HSQL", ex);
        }
    }
}
