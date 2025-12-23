package org.github.dbjo.meta.entity;

public interface PropSetter<B, V> {
    void set(B bean, V value);
}
