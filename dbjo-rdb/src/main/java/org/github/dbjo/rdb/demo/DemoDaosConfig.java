package org.github.dbjo.rdb.demo;

import org.github.dbjo.rdb.*;
import org.springframework.context.annotation.*;

@Configuration(proxyBeanMethods = false)
public class DemoDaosConfig {

    @Bean
    public UserDao userDao(RocksSessions sessions, DaoRegistry registry) {
        return new UserDao(sessions, registry);
    }
}
