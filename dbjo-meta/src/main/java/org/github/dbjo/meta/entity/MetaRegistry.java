package org.github.dbjo.meta.entity;

import java.io.Serializable;

public interface MetaRegistry {
    <B extends Serializable> EntityMeta<B> entityMeta(String entityId);
    <B extends Serializable> PropertyMeta<B, Serializable> property(String entityId, String propertyName);
}

