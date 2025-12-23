package org.github.dbjo.meta.entity;

import java.util.List;

public abstract class EntityPropAccessor<B> {

    public abstract List<PropAccessor<B, ?>> getAllProps();

    public abstract List<String> getAllPropsNames();

    public abstract List<Class<?>> getAllPropsTypes();
}
