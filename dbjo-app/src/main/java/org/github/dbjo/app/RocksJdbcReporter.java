package org.github.dbjo.app;

import org.github.dbjo.generated.model.rdb.jdbc.GeneratedRocksJdbcCatalog;
import org.github.dbjo.rdb.RocksProps;
import org.github.dbjo.rdb.jdbc.RocksJdbcDriver;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

@Component
public class RocksJdbcReporter {
    private final RocksProps rocksProps;

    public RocksJdbcReporter(RocksProps rocksProps) {
        this.rocksProps = rocksProps;
    }

    public void reportTables() throws SQLException {
        DriverManager.registerDriver(new RocksJdbcDriver());
        String url = "jdbc:rocksdb:" + rocksProps.path() + "?catalog=" + GeneratedRocksJdbcCatalog.class.getName();

        try (Connection connection = DriverManager.getConnection(url);
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("select * from tables")) {
            printResultSet(resultSet);
        }
    }

    private static void printResultSet(ResultSet resultSet) throws SQLException {
        ResultSetMetaData meta = resultSet.getMetaData();
        int columns = meta.getColumnCount();
        StringBuilder header = new StringBuilder();
        for (int i = 1; i <= columns; i++) {
            if (i > 1) {
                header.append(" | ");
            }
            header.append(meta.getColumnLabel(i));
        }
        System.out.println(header);
        while (resultSet.next()) {
            StringBuilder row = new StringBuilder();
            for (int i = 1; i <= columns; i++) {
                if (i > 1) {
                    row.append(" | ");
                }
                row.append(resultSet.getString(i));
            }
            System.out.println(row);
        }
    }
}
