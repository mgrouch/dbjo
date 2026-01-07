package org.github.dbjo.rdb;

import java.nio.charset.StandardCharsets;

public final class IndexValueCodecs {
    private IndexValueCodecs() {}

    public static final IndexValueCodec<String> STRING_UTF8 =
            s -> s.getBytes(StandardCharsets.UTF_8);
}
