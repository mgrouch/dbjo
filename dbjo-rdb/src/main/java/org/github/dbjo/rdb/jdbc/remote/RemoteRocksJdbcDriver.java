package org.github.dbjo.rdb.jdbc.remote;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.github.dbjo.rdb.jdbc.catalog.RocksJdbcCatalog;
import org.github.dbjo.rdb.jdbc.remote.dto.RemoteRocksJdbcCatalogDto;

import java.net.http.HttpClient;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;

/**
 * URL forms:
 *   jdbc:rocksdb+rest:http://host:port/api/rocks-jdbc
 *   jdbc:dbjo-rocks+rest:http://host:port/api/rocks-jdbc
 */
public final class RemoteRocksJdbcDriver implements Driver {
    public static final String URL1 = "jdbc:rocksdb+rest:";
    public static final String URL2 = "jdbc:dbjo-rocks+rest:";

    static {
        try {
            DriverManager.registerDriver(new RemoteRocksJdbcDriver());
        } catch (SQLException ignore) {}
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) return null;
        String baseUrl = url.substring(url.startsWith(URL1) ? URL1.length() : URL2.length());
        if (baseUrl.isBlank()) {
            throw new SQLException("Missing base URL in JDBC URL");
        }
        HttpClient.Builder clientBuilder = HttpClient.newBuilder();
        if (baseUrl.startsWith("https://")) {
            javax.net.ssl.SSLContext sslContext = RemoteRocksJdbcSsl.createSslContext(info);
            if (sslContext != null) {
                clientBuilder.sslContext(sslContext);
            }
        }
        HttpClient client = clientBuilder.build();
        ObjectMapper mapper = new ObjectMapper();
        RemoteRocksJdbcClient remoteClient = new RemoteRocksJdbcClient(client, mapper, baseUrl);
        RemoteRocksJdbcCatalogDto dto = remoteClient.fetchCatalog();
        RocksJdbcCatalog catalog = RemoteRocksJdbcCatalogMapper.toCatalog(dto);
        return new RemoteRocksJdbcConnection(url, info, remoteClient, catalog);
    }

    @Override
    public boolean acceptsURL(String url) {
        return url != null && (url.startsWith(URL1) || url.startsWith(URL2));
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
        return new DriverPropertyInfo[0];
    }

    @Override public int getMajorVersion() { return 1; }
    @Override public int getMinorVersion() { return 0; }
    @Override public boolean jdbcCompliant() { return false; }
    @Override public java.util.logging.Logger getParentLogger() { return java.util.logging.Logger.getGlobal(); }
}
