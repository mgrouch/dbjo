package org.github.dbjo.rdb.jdbc.remote;

import javax.sql.rowset.CachedRowSet;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;

final class RawSetWrapper {
    private RawSetWrapper() {}

    // CachedRowSet is not a functional interface, so a lambda cannot wrap all methods.
    // Use a dynamic proxy to log every call without hand-writing a full delegating class.
    static CachedRowSet wrap(CachedRowSet delegate, Logger logger) {
        Objects.requireNonNull(delegate, "delegate");
        Objects.requireNonNull(logger, "logger");
        InvocationHandler handler = new LoggingInvocationHandler(delegate, logger);
        return (CachedRowSet) Proxy.newProxyInstance(
                RawSetWrapper.class.getClassLoader(),
                new Class<?>[]{CachedRowSet.class},
                handler);
    }

    private static final class LoggingInvocationHandler implements InvocationHandler {
        private final CachedRowSet delegate;
        private final Logger logger;

        private LoggingInvocationHandler(CachedRowSet delegate, Logger logger) {
            this.delegate = delegate;
            this.logger = logger;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            String arguments = args == null ? "" : Arrays.toString(args);
            logger.info(() -> "RowSet call: " + method.getName() + " args=" + arguments);
            try {
                Object result = method.invoke(delegate, args);
                logger.fine(() -> "RowSet call complete: " + method.getName());
                return result;
            } catch (Throwable ex) {
                logger.log(Level.WARNING, "RowSet call failed: " + method.getName(), ex);
                throw ex.getCause() == null ? ex : ex.getCause();
            }
        }
    }
}
