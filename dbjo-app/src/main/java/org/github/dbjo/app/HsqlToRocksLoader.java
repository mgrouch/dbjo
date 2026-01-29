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
import org.springframework.transaction.annotation.Transactional;

import java.sql.SQLException;

@Service
public class HsqlToRocksLoader {
    private final ClientJdbcDao clientJdbcDao;
    private final ProductJdbcDao productJdbcDao;
    private final PurchaseJdbcDao purchaseJdbcDao;
    private final ClientDao clientDao;
    private final ProductDao productDao;
    private final PurchaseDao purchaseDao;

    public HsqlToRocksLoader(
            ClientJdbcDao clientJdbcDao,
            ProductJdbcDao productJdbcDao,
            PurchaseJdbcDao purchaseJdbcDao,
            ClientDao clientDao,
            ProductDao productDao,
            PurchaseDao purchaseDao
    ) {
        this.clientJdbcDao = clientJdbcDao;
        this.productJdbcDao = productJdbcDao;
        this.purchaseJdbcDao = purchaseJdbcDao;
        this.clientDao = clientDao;
        this.productDao = productDao;
        this.purchaseDao = purchaseDao;
    }

    @Transactional(transactionManager = "rocksTransactionManager")
    public void load() {
        loadClients();
        loadProducts();
        loadPurchases();
    }

    private void loadClients() {
        try {
            for (Client client : clientJdbcDao.selectAll()) {
                clientDao.upsert(client.getId(), client);
            }
        } catch (SQLException ex) {
            throw new IllegalStateException("Failed to load clients from HSQL", ex);
        }
    }

    private void loadProducts() {
        try {
            for (Product product : productJdbcDao.selectAll()) {
                productDao.upsert(product.getId(), product);
            }
        } catch (SQLException ex) {
            throw new IllegalStateException("Failed to load products from HSQL", ex);
        }
    }

    private void loadPurchases() {
        try {
            for (Purchase purchase : purchaseJdbcDao.selectAll()) {
                purchaseDao.upsert(purchase.getId(), purchase);
            }
        } catch (SQLException ex) {
            throw new IllegalStateException("Failed to load purchases from HSQL", ex);
        }
    }
}
