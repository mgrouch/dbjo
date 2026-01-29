package org.github.dbjo.app.client;

import org.github.dbjo.meta.jdbc.JdbcUtil;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public final class RemoteJdbcClientApp {
    private static final String DEFAULT_URL = "jdbc:rocksdb+rest:https://localhost:8433/api/rocks-jdbc";

    private RemoteJdbcClientApp() {}

    public static void main(String[] args) {
        String url = (args.length > 0 && !args[0].isBlank()) ? args[0] : DEFAULT_URL;
        List<String> queries = List.of(
                "select * from tables",
                "select * from client",
                "select count(*) from client",
                "select id, name, email from client where id >= 1 order by id limit 5",
                "select id from client where id in (1, 2, 3)",
                "select * from client where email is not null",
                "select id, email from client order by id",
                "select min(qty), max(qty), sum(qty) from purchase",
                "select product_id, sum(qty) from purchase group by product_id order by product_id",
                "select product_id, count(*) from purchase group by product_id having count(*) > 0"
        );

        try (Connection connection = DriverManager.getConnection(url);
             Statement statement = connection.createStatement()) {
            for (String query : queries) {
                System.out.println("==> " + query);
                try (ResultSet rs = statement.executeQuery(query)) {
                    JdbcUtil.printResultSet(System.out, rs);
                }
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Remote JDBC queries failed for URL: " + url, e);
        }
    }
}
