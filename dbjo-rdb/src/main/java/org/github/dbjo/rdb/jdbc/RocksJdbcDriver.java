package org.github.dbjo.rdb.jdbc;

import org.github.dbjo.rdb.jdbc.catalog.RocksJdbcCatalog;

import java.sql.*;
import java.util.Objects;
import java.util.Properties;

/**
 * URL forms:
 *   jdbc:dbjo-rocks:/absolute/or/relative/path
 *   jdbc:rocksdb:/absolute/or/relative/path
 *
 * Properties:
 *   rebuildIndexes = true|false   (default true)
 *   catalogClass   = fqcn of generated catalog (optional if driver registered programmatically)
 *   readOnly       = true|false   (default false)
 */
public final class RocksJdbcDriver implements Driver {
    public static final String URL1 = "jdbc:dbjo-rocks:";
    public static final String URL2 = "jdbc:rocksdb:";

    private final RocksJdbcCatalog fixedCatalogOrNull;

    static {
        try {
            DriverManager.registerDriver(new RocksJdbcDriver(null));
        } catch (SQLException ignore) {}
    }

    public RocksJdbcDriver() { this(null); }

    public RocksJdbcDriver(RocksJdbcCatalog catalog) {
        this.fixedCatalogOrNull = catalog;
    }

    public static void register(RocksJdbcCatalog catalog) throws SQLException {
        DriverManager.registerDriver(new RocksJdbcDriver(Objects.requireNonNull(catalog)));
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) return null;

        Properties props = (info == null) ? new Properties() : info;

        RocksJdbcCatalog catalog = this.fixedCatalogOrNull;
        if (catalog == null) {
            String cls = props.getProperty("catalogClass", System.getProperty("dbjo.jdbc.catalog"));
            if (cls == null || cls.isBlank()) {
                throw new SQLException("Missing catalog: set Properties catalogClass=... or call RocksJdbcDriver.register(new GeneratedRocksJdbcCatalog())");
            }
            try {
                Class<?> c = Class.forName(cls);
                Object o = c.getDeclaredConstructor().newInstance();
                catalog = (RocksJdbcCatalog) o;
            } catch (Exception e) {
                throw new SQLException("Failed to create catalogClass: " + cls, e);
            }
        }

        String path = url.substring(url.startsWith(URL1) ? URL1.length() : URL2.length());
        if (path.isBlank()) throw new SQLException("Missing RocksDB path in URL");

        boolean rebuild = Boolean.parseBoolean(props.getProperty("rebuildIndexes", "true"));
        boolean readOnly = Boolean.parseBoolean(props.getProperty("readOnly", "false"));

        RocksJdbcEngine engine = new RocksJdbcEngine(catalog, path, rebuild, readOnly);
        return new RocksJdbcConnection(url, props, engine);
    }

    @Override
    public boolean acceptsURL(String url) {
        return url != null && (url.startsWith(URL1) || url.startsWith(URL2));
    }

    @Override public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
        return new DriverPropertyInfo[0];
    }

    @Override public int getMajorVersion() { return 1; }
    @Override public int getMinorVersion() { return 0; }
    @Override public boolean jdbcCompliant() { return false; }
    @Override public java.util.logging.Logger getParentLogger() { return java.util.logging.Logger.getGlobal(); }
}
