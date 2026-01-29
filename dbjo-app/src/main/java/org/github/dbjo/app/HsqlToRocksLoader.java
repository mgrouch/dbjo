package org.github.dbjo.app;

import org.github.dbjo.generated.model.dao.rdb.ClientDao;
import org.github.dbjo.generated.model.dao.rdb.ProductDao;
import org.github.dbjo.generated.model.dao.rdb.PurchaseDao;
import org.github.dbjo.generated.model.dbmeta.ClientDbMeta;
import org.github.dbjo.generated.model.dbmeta.ProductDbMeta;
import org.github.dbjo.generated.model.dbmeta.PurchaseDbMeta;
import org.github.dbjo.generated.model.entity.Client;
import org.github.dbjo.generated.model.entity.Product;
import org.github.dbjo.generated.model.entity.Purchase;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.jdbc.core.ResultSetExtractor;

@Service
public class HsqlToRocksLoader {
    private final JdbcTemplate jdbcTemplate;
    private final ClientDao clientDao;
    private final ProductDao productDao;
    private final PurchaseDao purchaseDao;

    public HsqlToRocksLoader(
            JdbcTemplate jdbcTemplate,
            ClientDao clientDao,
            ProductDao productDao,
            PurchaseDao purchaseDao
    ) {
        this.jdbcTemplate = jdbcTemplate;
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
        jdbcTemplate.query(
                ClientDbMeta.INSTANCE.selectAllSql(),
                (ResultSetExtractor<Void>) rs -> {
                    while (rs.next()) {
                        Client client = ClientDbMeta.INSTANCE.fromRow(rs);
                        clientDao.upsert(client.getId(), client);
                    }
                    return null;
                }
        );
    }

    private void loadProducts() {
        jdbcTemplate.query(
                ProductDbMeta.INSTANCE.selectAllSql(),
                (ResultSetExtractor<Void>) rs -> {
                    while (rs.next()) {
                        Product product = ProductDbMeta.INSTANCE.fromRow(rs);
                        productDao.upsert(product.getId(), product);
                    }
                    return null;
                }
        );
    }

    private void loadPurchases() {
        jdbcTemplate.query(
                PurchaseDbMeta.INSTANCE.selectAllSql(),
                (ResultSetExtractor<Void>) rs -> {
                    while (rs.next()) {
                        Purchase purchase = PurchaseDbMeta.INSTANCE.fromRow(rs);
                        purchaseDao.upsert(purchase.getId(), purchase);
                    }
                    return null;
                }
        );
    }
}
