package org.github.dbjo.meta.jdbc;

import java.sql.SQLException;
import java.util.function.BiConsumer;
import java.util.function.Function;

public class LoaderUtil {
    @FunctionalInterface
    public interface SqlSupplier<T> {
        T get() throws SQLException;
    }

    public static <T> void loadEntities(String label, SqlSupplier<? extends Iterable<T>> loader,
                                        BiConsumer<Long, T> upsert, Function<T, Long> idProvider) {
        try {
            for (T entity : loader.get()) {
                upsert.accept(idProvider.apply(entity), entity);
            }
        } catch (SQLException ex) {
            throw new IllegalStateException("Failed to load " + label + " from HSQL", ex);
        }
    }
}
