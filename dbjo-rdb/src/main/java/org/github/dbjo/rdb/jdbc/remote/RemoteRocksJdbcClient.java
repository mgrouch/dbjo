package org.github.dbjo.rdb.jdbc.remote;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.github.dbjo.rdb.jdbc.remote.dto.RemoteRocksJdbcCatalogDto;
import org.github.dbjo.rdb.jdbc.remote.dto.RemoteRocksJdbcQueryRequest;
import org.github.dbjo.rdb.jdbc.remote.dto.RemoteRocksJdbcQueryResponse;

import javax.sql.rowset.RowSetProvider;
import javax.sql.rowset.WebRowSet;
import java.io.IOException;
import java.io.StringReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;

final class RemoteRocksJdbcClient {
    private static final Logger LOGGER = Logger.getLogger(RemoteRocksJdbcClient.class.getName());
    private final HttpClient client;
    private final ObjectMapper mapper;
    private final String baseUrl;

    RemoteRocksJdbcClient(HttpClient client, ObjectMapper mapper, String baseUrl) {
        this.client = Objects.requireNonNull(client, "client");
        this.mapper = Objects.requireNonNull(mapper, "mapper");
        this.baseUrl = Objects.requireNonNull(baseUrl, "baseUrl").replaceAll("/+$", "");
    }

    RemoteRocksJdbcCatalogDto fetchCatalog() throws SQLException {
        LOGGER.info(() -> "Fetching remote catalog from " + baseUrl);
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/catalog"))
                .timeout(Duration.ofSeconds(30))
                .GET()
                .build();
        try {
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            LOGGER.info(() -> "Remote catalog response status=" + response.statusCode());
            if (response.statusCode() >= 300) {
                throw new SQLException("Remote catalog request failed: HTTP " + response.statusCode());
            }
            RemoteRocksJdbcCatalogDto dto = mapper.readValue(response.body(), RemoteRocksJdbcCatalogDto.class);
            LOGGER.fine(() -> "Remote catalog parsed tables=" + dto.tables().size());
            return dto;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SQLException("Remote catalog request failed", e);
        } catch (IOException e) {
            throw new SQLException("Remote catalog request failed", e);
        }
    }

    ResultSet query(String sql, int maxRows) throws SQLException {
        LOGGER.info(() -> "Executing remote query maxRows=" + maxRows + " sql=" + sql);
        RemoteRocksJdbcQueryRequest body = new RemoteRocksJdbcQueryRequest(sql, maxRows);
        try {
            String payload = mapper.writeValueAsString(body);
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(baseUrl + "/query"))
                    .timeout(Duration.ofSeconds(30))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(payload))
                    .build();
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            LOGGER.info(() -> "Remote query response status=" + response.statusCode());
            if (response.statusCode() >= 300) {
                throw new SQLException("Remote query failed: HTTP " + response.statusCode());
            }
            RemoteRocksJdbcQueryResponse queryResponse =
                    mapper.readValue(response.body(), RemoteRocksJdbcQueryResponse.class);
            WebRowSet rowSet = RowSetProvider.newFactory().createWebRowSet();
            rowSet.setType(ResultSet.TYPE_SCROLL_INSENSITIVE);
            rowSet.readXml(new StringReader(queryResponse.rowsetXml()));
            normalizeTemporalValues(rowSet);
            rowSet.beforeFirst();
            LOGGER.fine(() -> "Remote query returned rows=" + rowSet.size());
            return RocksJdbcResultSet.wrap(rowSet);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SQLException("Remote query failed", e);
        } catch (IOException e) {
            throw new SQLException("Remote query failed", e);
        } catch (SQLException e) {
            LOGGER.log(Level.WARNING, "Remote query rowset conversion failed", e);
            throw e;
        }
    }

    private static void normalizeTemporalValues(WebRowSet rowSet) throws SQLException {
        ResultSetMetaData meta = rowSet.getMetaData();
        int columnCount = meta.getColumnCount();
        rowSet.beforeFirst();
        while (rowSet.next()) {
            boolean updated = false;
            for (int i = 1; i <= columnCount; i++) {
                int type = meta.getColumnType(i);
                Object value = rowSet.getObject(i);
                Object normalized = normalizeTemporalValue(value, type);
                if (normalized != value) {
                    rowSet.updateObject(i, normalized);
                    updated = true;
                }
            }
            if (updated) {
                rowSet.updateRow();
            }
        }
        rowSet.beforeFirst();
    }

    private static Object normalizeTemporalValue(Object value, int sqlType) {
        if (value == null) return null;
        return switch (sqlType) {
            case Types.DATE -> normalizeDateValue(value);
            case Types.TIME, Types.TIME_WITH_TIMEZONE -> normalizeTimeValue(value);
            case Types.TIMESTAMP, Types.TIMESTAMP_WITH_TIMEZONE -> normalizeTimestampValue(value);
            default -> value;
        };
    }

    private static Object normalizeDateValue(Object value) {
        if (value instanceof Date) return value;
        if (value instanceof LocalDate date) return Date.valueOf(date);
        if (value instanceof LocalDateTime dateTime) return Date.valueOf(dateTime.toLocalDate());
        if (value instanceof OffsetDateTime dateTime) return Date.valueOf(dateTime.toLocalDate());
        if (value instanceof String s) {
            try {
                return Date.valueOf(s.trim());
            } catch (IllegalArgumentException ignored) {
                try {
                    return Date.valueOf(LocalDate.parse(s.trim()));
                } catch (RuntimeException ignoredAgain) {
                    return value;
                }
            }
        }
        return value;
    }

    private static Object normalizeTimeValue(Object value) {
        if (value instanceof Time) return value;
        if (value instanceof LocalTime time) return Time.valueOf(time);
        if (value instanceof OffsetTime time) return Time.valueOf(time.toLocalTime());
        if (value instanceof String s) {
            String trimmed = s.trim();
            try {
                return Time.valueOf(trimmed);
            } catch (IllegalArgumentException ignored) {
                try {
                    return Time.valueOf(LocalTime.parse(trimmed));
                } catch (RuntimeException ignoredAgain) {
                    try {
                        return Time.valueOf(OffsetTime.parse(trimmed).toLocalTime());
                    } catch (RuntimeException ignoredOnceMore) {
                        return value;
                    }
                }
            }
        }
        return value;
    }

    private static Object normalizeTimestampValue(Object value) {
        if (value instanceof Timestamp) return value;
        if (value instanceof LocalDateTime dateTime) return Timestamp.valueOf(dateTime);
        if (value instanceof OffsetDateTime dateTime) return Timestamp.from(dateTime.toInstant());
        if (value instanceof Instant instant) return Timestamp.from(instant);
        if (value instanceof String s) {
            String trimmed = s.trim();
            try {
                return Timestamp.valueOf(trimmed);
            } catch (IllegalArgumentException ignored) {
                try {
                    return Timestamp.valueOf(LocalDateTime.parse(trimmed));
                } catch (RuntimeException ignoredAgain) {
                    try {
                        return Timestamp.from(OffsetDateTime.parse(trimmed).toInstant());
                    } catch (RuntimeException ignoredOnceMore) {
                        return value;
                    }
                }
            }
        }
        return value;
    }
}
