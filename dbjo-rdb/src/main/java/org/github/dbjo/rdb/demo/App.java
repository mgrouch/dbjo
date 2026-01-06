package org.github.dbjo.rdb.demo;

import org.rocksdb.RocksDB;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = "org.github.dbjo")
public class App {
    public static void main(String[] args) {
        RocksDB.loadLibrary();
        SpringApplication.run(App.class, args);
    }
}