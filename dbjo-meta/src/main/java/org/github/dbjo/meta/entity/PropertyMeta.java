package org.github.dbjo.meta.entity;

import java.io.Serializable;

public record PropertyMeta<B extends Serializable, V extends Serializable>(
        String propertyName,
        Class<V> propertyClass,
        PropGetter<B, V> getter,
        PropSetter<B, V> setter
) implements PropAccessor<B, V> {

    @Override
    public String getPropertyName() {
        return propertyName;
    }

    @Override
    public Class<V> getPropertyClass() {
        return propertyClass;
    }

    @Override
    public V get(B bean) {
        return getter.get(bean);
    }

    @Override
    public void set(B bean, V value) {
        setter.set(bean, value);
    }
}
