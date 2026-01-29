package org.github.dbjo.app.client;

import org.github.dbjo.meta.jdbc.JdbcUtil;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public final class RemoteJdbcClientApp {
    private static final String DEFAULT_URL = "jdbc:rocksdb+rest:http://localhost:8080/api/rocks-jdbc";

    private RemoteJdbcClientApp() {}

    public static void main(String[] args) {
        String url = (args.length > 0 && !args[0].isBlank()) ? args[0] : DEFAULT_URL;
        List<String> queries = List.of(
                "select * from tables",
                "select * from client",
                "select count(*) from client",
                "select id, email from client order by id",
                "select product_id, sum(qty) from purchase group by product_id order by product_id",
                "select c.name, count(*) from purchase p join client c on p.client_id = c.id group by c.name"
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
