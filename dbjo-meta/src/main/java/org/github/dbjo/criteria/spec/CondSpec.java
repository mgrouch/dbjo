package org.github.dbjo.criteria.spec;

import java.io.Serializable;

public sealed interface CondSpec extends Serializable
        permits TrueSpec, FalseSpec,
        EqSpec, NeSpec, InSpec, IsNullSpec, IsNotNullSpec,
        BetweenSpec, CmpSpec,
        AndSpec, OrSpec, NotSpec {}

