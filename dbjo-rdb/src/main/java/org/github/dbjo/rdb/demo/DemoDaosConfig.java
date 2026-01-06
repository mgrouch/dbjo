package org.github.dbjo.rdb.demo;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.github.dbjo.rdb.*;
import org.springframework.context.annotation.*;

@Configuration
public class DemoDaosConfig {

    @Bean
    public UserDao userDao(DaoRegistry registry, ObjectMapper om) {
        return registry.register(UserDao.definition(om));
    }
}

