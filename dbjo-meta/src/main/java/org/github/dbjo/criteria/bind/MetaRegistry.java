package org.github.dbjo.criteria.bind;

import java.io.Serializable;
import org.github.dbjo.meta.entity.EntityMeta;
import org.github.dbjo.meta.entity.PropertyMeta;

public interface MetaRegistry {
    <B extends Serializable> EntityMeta<B> entityMeta(String entityId);

    /** Lookup property by name. The binder will validate types/ops separately. */
    <B extends Serializable> PropertyMeta<B, Serializable> property(String entityId, String propertyName);
}
