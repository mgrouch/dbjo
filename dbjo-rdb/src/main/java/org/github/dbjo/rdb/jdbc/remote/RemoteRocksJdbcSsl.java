package org.github.dbjo.rdb.jdbc.remote;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Properties;

final class RemoteRocksJdbcSsl {
    static final String TRUST_ALL_PROPERTY = "ssl.trustAll";
    static final String TRUST_STORE_PROPERTY = "ssl.trustStore";
    static final String TRUST_STORE_PASSWORD_PROPERTY = "ssl.trustStorePassword";
    static final String TRUST_STORE_TYPE_PROPERTY = "ssl.trustStoreType";

    private RemoteRocksJdbcSsl() {}

    static SSLContext createSslContext(Properties info) throws SQLException {
        Objects.requireNonNull(info, "info");
        boolean trustAll = Boolean.parseBoolean(info.getProperty(TRUST_ALL_PROPERTY, "false"));
        if (trustAll) {
            return buildTrustAllContext();
        }
        String trustStore = info.getProperty(TRUST_STORE_PROPERTY);
        if (trustStore == null || trustStore.isBlank()) {
            return null;
        }
        String trustStorePassword = info.getProperty(TRUST_STORE_PASSWORD_PROPERTY, "");
        String trustStoreType = info.getProperty(TRUST_STORE_TYPE_PROPERTY, KeyStore.getDefaultType());
        return buildTrustStoreContext(Path.of(trustStore), trustStorePassword.toCharArray(), trustStoreType);
    }

    private static SSLContext buildTrustAllContext() throws SQLException {
        TrustManager[] trustManagers = new TrustManager[] { new TrustAllManager() };
        try {
            SSLContext context = SSLContext.getInstance("TLS");
            context.init(null, trustManagers, new SecureRandom());
            return context;
        } catch (GeneralSecurityException e) {
            throw new SQLException("Unable to initialize SSL context", e);
        }
    }

    private static SSLContext buildTrustStoreContext(Path trustStorePath,
                                                     char[] password,
                                                     String trustStoreType) throws SQLException {
        try (InputStream inputStream = Files.newInputStream(trustStorePath)) {
            KeyStore keyStore = KeyStore.getInstance(trustStoreType);
            keyStore.load(inputStream, password);
            TrustManagerFactory factory =
                    TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            factory.init(keyStore);
            SSLContext context = SSLContext.getInstance("TLS");
            context.init(null, factory.getTrustManagers(), new SecureRandom());
            return context;
        } catch (IOException | GeneralSecurityException e) {
            throw new SQLException("Unable to load SSL trust store", e);
        }
    }

    private static final class TrustAllManager implements X509TrustManager {
        @Override
        public void checkClientTrusted(X509Certificate[] chain, String authType) {
            // Trusted.
        }

        @Override
        public void checkServerTrusted(X509Certificate[] chain, String authType) {
            // Trusted.
        }

        @Override
        public X509Certificate[] getAcceptedIssuers() {
            return new X509Certificate[0];
        }
    }
}
