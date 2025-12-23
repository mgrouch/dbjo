package org.github.dbjo.meta.entity;

import java.io.Serializable;

public record PropertyMeta<B extends Serializable, V extends Serializable>(
        String propId,
        String propDesc,
        Class<V> propClass,
        PropAccessor<B, V> propAccessor
) implements Serializable {

}
