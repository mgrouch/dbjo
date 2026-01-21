// File: src/main/java/org/github/dbjo/criteria/FalseCond.java
package org.github.dbjo.criteria;

import java.io.Serializable;

public record FalseCond<B extends Serializable>() implements Condition<B> {}
