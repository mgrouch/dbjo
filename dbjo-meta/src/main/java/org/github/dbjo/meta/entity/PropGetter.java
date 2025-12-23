package org.github.dbjo.meta.entity;

public interface PropGetter<B, V> {
    V get(B bean);
}
