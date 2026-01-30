package org.github.dbjo.rdb.jdbc.remote;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.github.dbjo.rdb.jdbc.remote.dto.RemoteRocksJdbcCatalogDto;
import org.github.dbjo.rdb.jdbc.remote.dto.RemoteRocksJdbcQueryRequest;
import org.github.dbjo.rdb.jdbc.remote.dto.RemoteRocksJdbcQueryResponse;

import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetProvider;
import javax.sql.rowset.WebRowSet;
import java.io.IOException;
import java.io.StringReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Objects;

final class RemoteRocksJdbcClient {
    private final HttpClient client;
    private final ObjectMapper mapper;
    private final String baseUrl;

    RemoteRocksJdbcClient(HttpClient client, ObjectMapper mapper, String baseUrl) {
        this.client = Objects.requireNonNull(client, "client");
        this.mapper = Objects.requireNonNull(mapper, "mapper");
        this.baseUrl = Objects.requireNonNull(baseUrl, "baseUrl").replaceAll("/+$", "");
    }

    RemoteRocksJdbcCatalogDto fetchCatalog() throws SQLException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/catalog"))
                .timeout(Duration.ofSeconds(30))
                .GET()
                .build();
        try {
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() >= 300) {
                throw new SQLException("Remote catalog request failed: HTTP " + response.statusCode());
            }
            return mapper.readValue(response.body(), RemoteRocksJdbcCatalogDto.class);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SQLException("Remote catalog request failed", e);
        } catch (IOException e) {
            throw new SQLException("Remote catalog request failed", e);
        }
    }

    CachedRowSet query(String sql, int maxRows) throws SQLException {
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
            if (response.statusCode() >= 300) {
                throw new SQLException("Remote query failed: HTTP " + response.statusCode());
            }
            RemoteRocksJdbcQueryResponse queryResponse =
                    mapper.readValue(response.body(), RemoteRocksJdbcQueryResponse.class);
            WebRowSet rowSet = RowSetProvider.newFactory().createWebRowSet();
            rowSet.readXml(new StringReader(queryResponse.rowsetXml()));
            rowSet.beforeFirst();
            return rowSet;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SQLException("Remote query failed", e);
        } catch (IOException e) {
            throw new SQLException("Remote query failed", e);
        }
    }
}
