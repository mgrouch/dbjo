package org.github.dbjo.rdb;

import java.util.OptionalLong;

public interface TtlPolicy<T> {
    /** Return expiry epoch seconds, or empty for “no expiry”. */
    OptionalLong expiresAtEpochSeconds(T value);
}

