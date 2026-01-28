package org.github.dbjo.rdb.jdbc.catalog;

import org.github.dbjo.criteria.PropertyTerm;
import org.github.dbjo.meta.entity.EntityMeta;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Map;
import java.util.Objects;

public final class RocksJdbcCriteriaBinding<B extends Serializable> {
    private final String tableName;
    private final Class<B> rowClass;
    private final Class<?> metaClass;
    private final Map<String, PropertyTerm<B, ? extends Serializable>> termsByColumnLower;
    private final Class<?> daoClass; // generated IndexedRocksDao class, optional

    private volatile EntityMeta<B> cachedMeta;

    public RocksJdbcCriteriaBinding(
            String tableName,
            Class<B> rowClass,
            Class<?> metaClass,
            Map<String, PropertyTerm<B, ? extends Serializable>> termsByColumnLower,
            Class<?> daoClass
    ) {
        this.tableName = Objects.requireNonNull(tableName, "tableName");
        this.rowClass = Objects.requireNonNull(rowClass, "rowClass");
        this.metaClass = Objects.requireNonNull(metaClass, "metaClass");
        this.termsByColumnLower = Objects.requireNonNull(termsByColumnLower, "termsByColumnLower");
        this.daoClass = daoClass;
    }

    public String tableName() { return tableName; }
    public Class<B> rowClass() { return rowClass; }
    public Class<?> metaClass() { return metaClass; }
    public Map<String, PropertyTerm<B, ? extends Serializable>> termsByColumnLower() { return termsByColumnLower; }
    public Class<?> daoClass() { return daoClass; }

    public EntityMeta<B> meta() {
        EntityMeta<B> m = cachedMeta;
        if (m != null) return m;
        m = resolveEntityMeta(metaClass);
        cachedMeta = m;
        return m;
    }

    @SuppressWarnings("unchecked")
    private static <B extends Serializable> EntityMeta<B> resolveEntityMeta(Class<?> metaClass) {
        // Prefer public static fields
        for (Field f : metaClass.getFields()) {
            if (!Modifier.isStatic(f.getModifiers())) continue;
            if (!EntityMeta.class.isAssignableFrom(f.getType())) continue;
            try {
                Object v = f.get(null);
                if (v != null) return (EntityMeta<B>) v;
            } catch (Throwable ignored) {}
        }
        // Then public static no-arg methods
        for (Method m : metaClass.getMethods()) {
            if (!Modifier.isStatic(m.getModifiers())) continue;
            if (m.getParameterCount() != 0) continue;
            if (!EntityMeta.class.isAssignableFrom(m.getReturnType())) continue;
            try {
                Object v = m.invoke(null);
                if (v != null) return (EntityMeta<B>) v;
            } catch (Throwable ignored) {}
        }
        throw new IllegalStateException("Could not resolve EntityMeta from meta class: " + metaClass.getName());
    }
}
