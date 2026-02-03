package org.github.dbjo.rdb.jdbc.remote;

import javax.sql.rowset.WebRowSet;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;

final class RocksJdbcResultSet {
    private RocksJdbcResultSet() {}

    static ResultSet wrap(WebRowSet delegate, Logger logger) {
        Objects.requireNonNull(delegate, "delegate");
        Objects.requireNonNull(logger, "logger");
        InvocationHandler handler = new LoggingInvocationHandler(delegate, logger);
        return (ResultSet) Proxy.newProxyInstance(
                RocksJdbcResultSet.class.getClassLoader(),
                new Class<?>[]{ResultSet.class},
                handler);
    }

    private static final class LoggingInvocationHandler implements InvocationHandler {
        private final WebRowSet delegate;
        private final Logger logger;

        private LoggingInvocationHandler(WebRowSet delegate, Logger logger) {
            this.delegate = delegate;
            this.logger = logger;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            String arguments = args == null ? "" : Arrays.toString(args);
            logger.info(() -> "ResultSet call: " + method.getName() + " args=" + arguments);
            try {
                Object result = method.invoke(delegate, args);
                logger.fine(() -> "ResultSet call complete: " + method.getName());
                return result;
            } catch (Throwable ex) {
                logger.log(Level.WARNING, "ResultSet call failed: " + method.getName(), ex);
                throw ex.getCause() == null ? ex : ex.getCause();
            }
        }
    }
}
