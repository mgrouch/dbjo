package org.github.dbjo.rdb.jdbc.remote;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.github.dbjo.rdb.jdbc.remote.dto.RemoteRocksJdbcCatalogDto;
import org.github.dbjo.rdb.jdbc.remote.dto.RemoteRocksJdbcColumnDto;
import org.github.dbjo.rdb.jdbc.remote.dto.RemoteRocksJdbcIndexDto;
import org.github.dbjo.rdb.jdbc.remote.dto.RemoteRocksJdbcQueryRequest;
import org.github.dbjo.rdb.jdbc.remote.dto.RemoteRocksJdbcQueryResponse;
import org.github.dbjo.rdb.jdbc.remote.dto.RemoteRocksJdbcTableDto;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import javax.sql.rowset.RowSetMetaDataImpl;
import javax.sql.rowset.RowSetProvider;
import javax.sql.rowset.WebRowSet;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StringWriter;
import java.net.InetSocketAddress;
import java.net.http.HttpClient;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class RemoteRocksJdbcClientTest {
    private HttpServer server;

    @AfterEach
    void stopServer() {
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    void fetchesCatalogAndQueriesRowset() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        RemoteRocksJdbcCatalogDto catalogDto = new RemoteRocksJdbcCatalogDto(List.of(
                new RemoteRocksJdbcTableDto(
                        "PUBLIC",
                        "client",
                        "client_cf",
                        List.of(new RemoteRocksJdbcColumnDto(
                                1, "id", Types.INTEGER, "INTEGER", 0, 0, false, "NO", null, "getId"
                        )),
                        List.of("id"),
                        List.of(new RemoteRocksJdbcIndexDto("pk_client", true, List.of("id"))),
                        List.of("client")
                )
        ));
        String rowsetXml = buildRowSetXml();

        server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/api/rocks-jdbc/catalog", exchange -> writeJson(exchange, mapper, catalogDto));
        server.createContext("/api/rocks-jdbc/query", exchange -> {
            RemoteRocksJdbcQueryRequest request = mapper.readValue(exchange.getRequestBody(), RemoteRocksJdbcQueryRequest.class);
            assertThat(request.sql()).isEqualTo("select * from client");
            RemoteRocksJdbcQueryResponse response = new RemoteRocksJdbcQueryResponse(rowsetXml);
            writeJson(exchange, mapper, response);
        });
        server.start();

        String baseUrl = "http://localhost:" + server.getAddress().getPort() + "/api/rocks-jdbc";
        RemoteRocksJdbcClient client = new RemoteRocksJdbcClient(HttpClient.newHttpClient(), mapper, baseUrl);

        RemoteRocksJdbcCatalogDto fetched = client.fetchCatalog();
        assertThat(fetched.tables()).hasSize(1);
        try (ResultSet rowSet = client.query("select * from client", 0)) {
            rowSet.next();
            assertThat(rowSet.getInt("id")).isEqualTo(42);
            assertThat(rowSet.getDate("created_on")).isEqualTo(Date.valueOf(LocalDate.of(2025, 1, 2)));
            assertThat(rowSet.getTimestamp("created_at"))
                    .isEqualTo(Timestamp.valueOf(LocalDateTime.of(2025, 1, 2, 3, 4, 5)));
            assertThat(rowSet.getString("created_on")).isEqualTo("2025-01-02");
            assertThat(rowSet.getObject("created_on")).isInstanceOf(Date.class);
            assertThat(rowSet.getObject("created_at")).isInstanceOf(Timestamp.class);
            assertThat(rowSet.getString("created_at")).isEqualTo("2025-01-02 03:04:05.0");
            assertThat(rowSet.next()).isFalse();
        }
    }


    @Test
    void normalizesIsoTemporalStringsFromRemoteRowset() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        String rowsetXml = buildIsoTemporalStringRowSetXml();

        server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/api/rocks-jdbc/catalog", exchange -> writeJson(exchange, mapper, new RemoteRocksJdbcCatalogDto(List.of())));
        server.createContext("/api/rocks-jdbc/query", exchange ->
                writeJson(exchange, mapper, new RemoteRocksJdbcQueryResponse(rowsetXml)));
        server.start();

        String baseUrl = "http://localhost:" + server.getAddress().getPort() + "/api/rocks-jdbc";
        RemoteRocksJdbcClient client = new RemoteRocksJdbcClient(HttpClient.newHttpClient(), mapper, baseUrl);

        try (ResultSet rowSet = client.query("select now()", 0)) {
            assertThat(rowSet.next()).isTrue();
            assertThat(rowSet.getDate("created_on")).isEqualTo(Date.valueOf(LocalDate.of(2025, 1, 2)));
            assertThat(rowSet.getTimestamp("created_at"))
                    .isEqualTo(Timestamp.valueOf(LocalDateTime.of(2025, 1, 2, 3, 4, 5)));
            assertThat(rowSet.getString("created_on")).isEqualTo("2025-01-02");
            assertThat(rowSet.getObject("created_on")).isInstanceOf(Date.class);
            assertThat(rowSet.getObject("created_at")).isInstanceOf(Timestamp.class);
        }
    }

    private static String buildRowSetXml() throws SQLException {
        try (WebRowSet rowSet = RowSetProvider.newFactory().createWebRowSet()) {
            rowSet.setType(ResultSet.TYPE_SCROLL_INSENSITIVE);
            RowSetMetaDataImpl meta = new RowSetMetaDataImpl();
            meta.setColumnCount(3);
            meta.setColumnName(1, "id");
            meta.setColumnType(1, Types.INTEGER);
            meta.setColumnName(2, "created_on");
            meta.setColumnType(2, Types.DATE);
            meta.setColumnName(3, "created_at");
            meta.setColumnType(3, Types.TIMESTAMP);
            rowSet.setMetaData(meta);
            rowSet.moveToInsertRow();
            rowSet.updateInt(1, 42);
            rowSet.updateDate(2, Date.valueOf(LocalDate.of(2025, 1, 2)));
            rowSet.updateTimestamp(3, Timestamp.valueOf(LocalDateTime.of(2025, 1, 2, 3, 4, 5)));
            rowSet.insertRow();
            rowSet.moveToCurrentRow();
            rowSet.beforeFirst();
            StringWriter writer = new StringWriter();
            rowSet.writeXml(writer);
            return writer.toString();
        }
    }


    private static String buildIsoTemporalStringRowSetXml() throws SQLException {
        String xml = buildRowSetXml();
        xml = xml.replace("<date>2025-01-02</date>", "<date>2025-01-02T00:00:00Z</date>");
        xml = xml.replace("<timestamp>2025-01-02 03:04:05.0</timestamp>", "<timestamp>2025-01-02T03:04:05Z</timestamp>");
        return xml;
    }

    private static void writeJson(HttpExchange exchange, ObjectMapper mapper, Object body) throws IOException {
        byte[] payload = mapper.writeValueAsBytes(body);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(200, payload.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(payload);
        }
        exchange.close();
    }
}
