package org.github.dbjo.app;

import org.github.dbjo.generated.model.rdb.jdbc.GeneratedRocksJdbcCatalog;
import org.github.dbjo.meta.jdbc.JdbcUtil;
import org.github.dbjo.rdb.RocksProps;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

@Component
public class RocksJdbcReporter {
    private final RocksProps rocksProps;

    public RocksJdbcReporter(RocksProps rocksProps) {
        this.rocksProps = rocksProps;
    }

    public void reportTables() throws SQLException {
        String url = "jdbc:rocksdb:" + rocksProps.path();
        Properties props = new Properties();
        props.setProperty("catalogClass", GeneratedRocksJdbcCatalog.class.getName());

        try (Connection connection = DriverManager.getConnection(url, props);
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("select * from tables")) {
            JdbcUtil.printResultSet(System.out, resultSet);
        }
    }
}
