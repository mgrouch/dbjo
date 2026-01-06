package org.github.dbjo.rdb;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "rocks")
public record RocksProps(String path) {
    public RocksProps {
        if (path == null || path.isBlank()) {
            // default for demo
            path = "/tmp/rocksdb";
        }
    }
}