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
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

@Service
public record HsqlToRocksLoader(ClientJdbcDao clientJdbcDao, ProductJdbcDao productJdbcDao,
                                PurchaseJdbcDao purchaseJdbcDao, ClientDao clientDao, ProductDao productDao,
                                PurchaseDao purchaseDao, TransactionTemplate rocksTransactionTemplate) {

    public void load() {
        rocksTransactionTemplate.executeWithoutResult(status -> {
            LoaderUtil.loadEntities("clients", clientJdbcDao::selectAll, clientDao::upsert, Client::getId);
            LoaderUtil.loadEntities("products", productJdbcDao::selectAll, productDao::upsert, Product::getId);
            LoaderUtil.loadEntities("purchases", purchaseJdbcDao::selectAll, purchaseDao::upsert, Purchase::getId);
        });
    }
}
