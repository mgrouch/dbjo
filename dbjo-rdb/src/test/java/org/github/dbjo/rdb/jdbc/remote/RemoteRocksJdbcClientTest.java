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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
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
            assertThat(rowSet.next()).isFalse();
        }
    }

    private static String buildRowSetXml() throws SQLException {
        try (WebRowSet rowSet = RowSetProvider.newFactory().createWebRowSet()) {
            rowSet.setType(ResultSet.TYPE_SCROLL_INSENSITIVE);
            RowSetMetaDataImpl meta = new RowSetMetaDataImpl();
            meta.setColumnCount(1);
            meta.setColumnName(1, "id");
            meta.setColumnType(1, Types.INTEGER);
            rowSet.setMetaData(meta);
            rowSet.moveToInsertRow();
            rowSet.updateInt(1, 42);
            rowSet.insertRow();
            rowSet.moveToCurrentRow();
            rowSet.beforeFirst();
            StringWriter writer = new StringWriter();
            rowSet.writeXml(writer);
            return writer.toString();
        }
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
