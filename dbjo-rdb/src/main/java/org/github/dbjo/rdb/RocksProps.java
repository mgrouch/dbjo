package org.github.dbjo.rdb;

import org.rocksdb.*;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "dbjo.rocksdb")
public record RocksProps(String path) {}
