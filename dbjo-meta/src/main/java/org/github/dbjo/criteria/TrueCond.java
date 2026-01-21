package org.github.dbjo.criteria;

import java.io.Serializable;

public record TrueCond<B extends Serializable>() implements Condition<B> {}
