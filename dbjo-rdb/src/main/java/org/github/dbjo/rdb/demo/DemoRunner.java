package org.github.dbjo.rdb.demo;

import org.github.dbjo.rdb.*;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Optional;

@Configuration
public class DemoRunner {

    @Bean
    CommandLineRunner run(UserService svc, UserDao userDao) {
        return args -> {
            svc.insertDemoUsers();

            System.out.println("get(u2) = " + userDao.findByKey("u2").orElse(null));

            // Range scan over primary keys [u1, u9)
            var qRange = Query.<String>builder()
                    .range(new KeyRange<>(Optional.of("u1"), Optional.of("u9")))
                    .limit(1000)
                    .build();

            try (var st = userDao.stream(qRange)) {
                System.out.println("range scan:");
                st.forEach(e -> System.out.println("  " + e.getKey() + " => " + e.getValue()));
            }

            System.out.println("findByEmail(alice@example.com) = " + svc.findByEmail("alice@example.com"));
        };
    }
}
