package org.github.dbjo.app.jdbc;

import org.github.dbjo.rdb.jdbc.RocksJdbcEngine;
import org.github.dbjo.rdb.jdbc.remote.RemoteRocksJdbcCatalogMapper;
import org.github.dbjo.rdb.jdbc.remote.dto.RemoteRocksJdbcCatalogDto;
import org.github.dbjo.rdb.jdbc.remote.dto.RemoteRocksJdbcQueryRequest;
import org.github.dbjo.rdb.jdbc.remote.dto.RemoteRocksJdbcQueryResponse;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetProvider;
import javax.sql.rowset.WebRowSet;
import java.io.StringWriter;
import java.sql.ResultSet;
import java.sql.SQLException;

@RestController
@RequestMapping("/api/rocks-jdbc")
public class RocksJdbcRestController {
    private final RocksJdbcEngine engine;

    public RocksJdbcRestController(RocksJdbcEngine engine) {
        this.engine = engine;
    }

    @GetMapping("/catalog")
    public RemoteRocksJdbcCatalogDto catalog() {
        return RemoteRocksJdbcCatalogMapper.fromCatalog(engine.catalog());
    }

    @PostMapping("/query")
    public RemoteRocksJdbcQueryResponse query(@RequestBody RemoteRocksJdbcQueryRequest request) {
        try (WebRowSet webRowSet = RowSetProvider.newFactory().createWebRowSet();
             CachedRowSet rowSet = engine.query(request.sql(), request.maxRows())) {
            webRowSet.setType(ResultSet.TYPE_SCROLL_INSENSITIVE);
            rowSet.beforeFirst();
            webRowSet.populate(rowSet);
            webRowSet.beforeFirst();
            StringWriter writer = new StringWriter();
            webRowSet.writeXml(writer);
            return new RemoteRocksJdbcQueryResponse(writer.toString());
        } catch (SQLException e) {
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Query failed", e);
        }
    }
}
