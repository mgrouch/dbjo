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
import org.github.dbjo.meta.features.Partitioned;
import org.github.dbjo.dao.jdbc.BaseJdbcDAO;
import org.github.dbjo.meta.jdbc.LoaderUtil;
import org.springframework.transaction.support.TransactionTemplate;

public record HsqlToRocksLoader(ClientJdbcDao clientJdbcDao, ProductJdbcDao productJdbcDao,
                                PurchaseJdbcDao purchaseJdbcDao, ClientDao clientDao, ProductDao productDao,
                                PurchaseDao purchaseDao, TransactionTemplate rocksTransactionTemplate,
                                int partitionNum, int totalPartitions, String additionalCriteria) {

    public static final String DEFAULT_ADDITIONAL_CRITERIA = "active = 'Y'";

    public void load() {
        rocksTransactionTemplate.executeWithoutResult(status -> {
            LoaderUtil.loadEntities("clients", loaderFor(clientJdbcDao, Client.class), clientDao::upsert, Client::getId);
            LoaderUtil.loadEntities("products", loaderFor(productJdbcDao, Product.class), productDao::upsert, Product::getId);
            LoaderUtil.loadEntities("purchases", loaderFor(purchaseJdbcDao, Purchase.class), purchaseDao::upsert, Purchase::getId);
        });
    }

    private <T> LoaderUtil.SqlSupplier<? extends Iterable<T>> loaderFor(BaseJdbcDAO<T, ?> dao, Class<T> entityClass) {
        if (Partitioned.class.isAssignableFrom(entityClass)) {
            String criteria = additionalCriteria == null || additionalCriteria.isBlank()
                    ? DEFAULT_ADDITIONAL_CRITERIA
                    : additionalCriteria.trim();
            return () -> dao.selectAllByPartition(partitionNum, totalPartitions, criteria);
        }
        return dao::selectAll;
    }
}
