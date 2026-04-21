package org.github.dbjo.kafka.listener.app;

import javax.sql.DataSource;
import org.github.dbjo.generated.model.dao.jdbc.ListenerConsumedOffsetsJdbcDao;
import org.github.dbjo.generated.model.dao.jdbc.ListenerOutboxJdbcDao;
import org.github.dbjo.meta.jdbc.DbDialect;

public class JdbcDatasource {
    private final DataSource dataSource;
    private final ListenerOutboxJdbcDao listenerOutboxDao;
    private final ListenerConsumedOffsetsJdbcDao listenerConsumedOffsetsDao;

    public JdbcDatasource(DataSource dataSource) {
        this.dataSource = dataSource;
        this.listenerOutboxDao = new ListenerOutboxJdbcDao(dataSource, DbDialect.HSQL);
        this.listenerConsumedOffsetsDao = new ListenerConsumedOffsetsJdbcDao(dataSource, DbDialect.HSQL);
    }

    public DataSource dataSource() {
        return dataSource;
    }

    public ListenerOutboxJdbcDao listenerOutboxDao() {
        return listenerOutboxDao;
    }

    public ListenerConsumedOffsetsJdbcDao listenerConsumedOffsetsDao() {
        return listenerConsumedOffsetsDao;
    }
}
