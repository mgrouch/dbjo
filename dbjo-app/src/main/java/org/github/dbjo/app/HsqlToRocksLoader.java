package org.github.dbjo.app;

import org.github.dbjo.generated.model.dao.rdb.ClientDao;
import org.github.dbjo.generated.model.dao.rdb.ProductDao;
import org.github.dbjo.generated.model.dao.rdb.PurchaseDao;
import org.github.dbjo.generated.model.entity.Client;
import org.github.dbjo.generated.model.entity.Product;
import org.github.dbjo.generated.model.entity.Purchase;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.sql.ResultSet;
import java.sql.SQLException;

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
                "select id, email, name, created_at from client",
                (rs) -> clientDao.upsert(rs.getLong("id"), toClient(rs))
        );
    }

    private void loadProducts() {
        jdbcTemplate.query(
                "select id, sku, title, price_cents from product",
                (rs) -> productDao.upsert(rs.getLong("id"), toProduct(rs))
        );
    }

    private void loadPurchases() {
        jdbcTemplate.query(
                "select id, client_id, product_id, qty, ordered_at from purchase",
                (rs) -> purchaseDao.upsert(rs.getLong("id"), toPurchase(rs))
        );
    }

    private static Client toClient(ResultSet rs) throws SQLException {
        Client client = new Client();
        client.setId(rs.getLong("id"));
        client.setEmail(rs.getString("email"));
        client.setName(rs.getString("name"));
        client.setCreatedAt(rs.getTimestamp("created_at"));
        return client;
    }

    private static Product toProduct(ResultSet rs) throws SQLException {
        Product product = new Product();
        product.setId(rs.getLong("id"));
        product.setSku(rs.getString("sku"));
        product.setTitle(rs.getString("title"));
        product.setPriceCents(rs.getInt("price_cents"));
        return product;
    }

    private static Purchase toPurchase(ResultSet rs) throws SQLException {
        Purchase purchase = new Purchase();
        purchase.setId(rs.getLong("id"));
        purchase.setClientId(rs.getLong("client_id"));
        purchase.setProductId(rs.getLong("product_id"));
        purchase.setQty(rs.getInt("qty"));
        purchase.setOrderedAt(rs.getTimestamp("ordered_at"));
        return purchase;
    }
}
