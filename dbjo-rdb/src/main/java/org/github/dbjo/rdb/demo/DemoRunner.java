package org.github.dbjo.rdb.demo;

import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class DemoRunner {

    @Bean
    CommandLineRunner run(UserService svc) {
        return args -> {
            svc.insertDemoUsers();

            System.out.println("get(u2) = " + svc.get("u2").orElse(null));

            System.out.println("range scan:");
            svc.scanIdRange("u1", "u9", 1000)
                    .forEach(u -> System.out.println("  " + u.getId() + " => " + u));

            System.out.println("findByEmail(alice@example.com) = " + svc.findByEmail("alice@example.com"));
        };
    }
}
