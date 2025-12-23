package org.github.dbjo.meta.entity;

import java.io.Serializable;
import java.util.List;

public record EntityMeta<B extends Serializable>(
        List<PropertyMeta<B, Serializable>> allPropertyMetas,
        List<String> allPropsNames,
        List<Class<?>> allPropsTypes

) implements EntityPropAccessor<B> {
    @Override
    public List<? extends PropAccessor<B, Serializable>> getAllProps() {
        return allPropertyMetas;
    }

    @Override
    public List<String> getAllPropsNames() {
        return allPropsNames;
    }

    @Override
    public List<Class<?>> getAllPropsTypes() {
        return allPropsTypes;
    }
}
