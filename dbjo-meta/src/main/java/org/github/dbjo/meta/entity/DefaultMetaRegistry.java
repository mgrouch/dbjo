package org.github.dbjo.meta.entity;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public final class DefaultMetaRegistry implements MetaRegistry {

    private final Map<String, EntityMeta<? extends Serializable>> entities = new HashMap<>();
    private final Map<String, Map<String, PropertyMeta<?, Serializable>>> props = new HashMap<>();

    public <B extends Serializable> DefaultMetaRegistry register(String entityId, EntityMeta<B> meta) {
        Objects.requireNonNull(entityId, "entityId");
        Objects.requireNonNull(meta, "meta");

        entities.put(entityId, meta);

        Map<String, PropertyMeta<?, Serializable>> m = new HashMap<>();
        for (PropertyMeta<B, Serializable> pm : meta.allPropertyMetas()) {
            m.put(pm.getPropertyName(), pm);
        }
        props.put(entityId, m);
        return this;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <B extends Serializable> EntityMeta<B> entityMeta(String entityId) {
        var meta = entities.get(entityId);
        if (meta == null) throw new IllegalArgumentException("Unknown entityId: " + entityId);
        return (EntityMeta<B>) meta;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <B extends Serializable> PropertyMeta<B, Serializable> property(String entityId, String propertyName) {
        var m = props.get(entityId);
        if (m == null) throw new IllegalArgumentException("Unknown entityId: " + entityId);
        var p = m.get(propertyName);
        if (p == null) throw new IllegalArgumentException("Unknown property '" + propertyName + "' for entityId=" + entityId);
        return (PropertyMeta<B, Serializable>) p;
    }
}
