package org.github.dbjo.meta.entity;

import java.io.Serializable;
import java.util.List;

public interface EntityPropAccessor<B> {

    List<? extends PropAccessor<B, Serializable>> getAllProps();

    List<String> getAllPropsNames();

    List<Class<?>> getAllPropsTypes();
}
