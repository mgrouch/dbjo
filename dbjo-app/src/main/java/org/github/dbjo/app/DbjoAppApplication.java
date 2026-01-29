package org.github.dbjo.app;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.github.dbjo.rdb.RocksDbHandle;

@SpringBootApplication
public class DbjoAppApplication {

    public static void main(String[] args) {
        SpringApplication.run(DbjoAppApplication.class, args);
    }

    @Bean
    CommandLineRunner run(HsqlToRocksLoader loader, RocksJdbcReporter reporter, RocksDbHandle rocksDbHandle) {
        return args -> {
            loader.load();
            rocksDbHandle.close();
            reporter.reportTables();
        };
    }
}
