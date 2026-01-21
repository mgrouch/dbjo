package org.github.dbjo.criteria;

import java.io.Serializable;
import org.github.dbjo.meta.entity.PropertyMeta;

public final class Terms {
    private Terms() {}
    public static <B extends Serializable, V extends Serializable> PropertyTerm<B, V> prop(PropertyMeta<B, V> meta) {
        return new PropertyTerm<>(meta);
    }
}
