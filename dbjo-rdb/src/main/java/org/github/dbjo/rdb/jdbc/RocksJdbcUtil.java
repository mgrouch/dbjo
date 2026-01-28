package org.github.dbjo.rdb.jdbc;

import org.github.dbjo.rdb.jdbc.catalog.RocksJdbcCatalog;

import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

final class RocksJdbcUtil {
    private RocksJdbcUtil() {}

    static final String URL_PREFIX = "jdbc:rocksdb:";

    record ParsedUrl(String dbPath, String catalogClassName) {}

    static ParsedUrl parseUrl(String url, Properties info) throws SQLException {
        if (url == null || !url.startsWith(URL_PREFIX)) {
            throw new SQLException("Bad URL (expected prefix " + URL_PREFIX + "): " + url);
        }

        String rest = url.substring(URL_PREFIX.length());

        String pathPart = rest;
        String queryPart = null;

        int q = rest.indexOf('?');
        if (q >= 0) {
            pathPart = rest.substring(0, q);
            queryPart = rest.substring(q + 1);
        }

        String dbPath = decodePath(pathPart);
        if (dbPath == null || dbPath.trim().isEmpty()) {
            throw new SQLException("Missing RocksDB path in URL: " + url);
        }

        Map<String, String> qp = parseQuery(queryPart);

        String catalog = null;
        if (info != null) catalog = trimToNull(info.getProperty("catalog"));
        if (catalog == null) catalog = trimToNull(qp.get("catalog"));
        if (catalog == null) catalog = trimToNull(System.getProperty("dbjo.rocksJdbcCatalog"));

        return new ParsedUrl(dbPath, catalog);
    }

    static RocksJdbcCatalog loadCatalog(String catalogClassName) throws SQLException {
        if (catalogClassName == null || catalogClassName.isBlank()) {
            throw new SQLException(
                    """
                            Rocks JDBC requires a catalog class.
                            Provide it as connection property 'catalog' or URL ?catalog=... or system property dbjo.rocksJdbcCatalog.
                            Example: jdbc:rocksdb:/path/to/db?catalog=org.github.dbjo.generated.rdb.jdbc.GeneratedRocksJdbcCatalog"""
            );
        }

        try {
            Class<?> c = Class.forName(catalogClassName);
            if (!RocksJdbcCatalog.class.isAssignableFrom(c)) {
                throw new SQLException("Catalog class does not implement RocksJdbcCatalog: " + catalogClassName);
            }

            // Try static create()
            try {
                var m = c.getMethod("create");
                Object o = m.invoke(null);
                return (RocksJdbcCatalog) o;
            } catch (NoSuchMethodException ignored) {
                // fall through
            }

            return (RocksJdbcCatalog) c.getDeclaredConstructor().newInstance();

        } catch (SQLException e) {
            throw e;
        } catch (Throwable t) {
            throw new SQLException("Failed to load catalog: " + catalogClassName, t);
        }
    }

    private static String decodePath(String s) throws SQLException {
        if (s == null) return null;
        String p = s.trim();
        if (p.isEmpty()) return p;

        if (p.startsWith("file:")) {
            try {
                return java.nio.file.Paths.get(URI.create(p)).toString();
            } catch (Throwable t) {
                throw new SQLException("Bad file: URI in JDBC URL: " + p, t);
            }
        }

        return URLDecoder.decode(p, StandardCharsets.UTF_8);
    }

    private static Map<String, String> parseQuery(String q) {
        Map<String, String> m = new HashMap<>();
        if (q == null || q.isBlank()) return m;

        String[] parts = q.split("&");
        for (String part : parts) {
            if (part.isBlank()) continue;
            int i = part.indexOf('=');
            if (i < 0) m.put(urlDecode(part), "");
            else m.put(urlDecode(part.substring(0, i)), urlDecode(part.substring(i + 1)));
        }
        return m;
    }

    private static String urlDecode(String s) {
        if (s == null) return null;
        return URLDecoder.decode(s, StandardCharsets.UTF_8);
    }

    private static String trimToNull(String s) {
        if (s == null) return null;
        String t = s.trim();
        return t.isEmpty() ? null : t;
    }
}
