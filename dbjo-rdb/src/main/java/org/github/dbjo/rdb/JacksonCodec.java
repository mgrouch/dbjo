package org.github.dbjo.rdb;

import com.fasterxml.jackson.databind.ObjectMapper;

public final class JacksonCodec<T> implements Codec<T> {
    private final ObjectMapper om;
    private final Class<T> type;

    public JacksonCodec(ObjectMapper om, Class<T> type) {
        this.om = om;
        this.type = type;
    }

    @Override public byte[] encode(T value) {
        try { return om.writeValueAsBytes(value); }
        catch (Exception e) { throw new RuntimeException(e); }
    }

    @Override public T decode(byte[] bytes) {
        try { return om.readValue(bytes, type); }
        catch (Exception e) { throw new RuntimeException(e); }
    }
}
