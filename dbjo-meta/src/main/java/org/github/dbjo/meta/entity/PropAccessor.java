package org.github.dbjo.meta.entity;

public interface PropAccessor<B, V> extends PropGetter<B, V>, PropSetter<B, V>  {

    String getPropertyName();

    Class<V> getPropertyClass();
}
