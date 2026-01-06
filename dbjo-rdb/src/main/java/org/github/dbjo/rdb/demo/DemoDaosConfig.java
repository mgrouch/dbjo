package org.github.dbjo.rdb.demo;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.github.dbjo.rdb.*;
import org.springframework.context.annotation.*;

@Configuration
public class DemoDaosConfig {

    @Bean
    public RocksDao<User, String> userDao(RocksSessions sessions, DaoRegistry registry, ObjectMapper om) {
        var def = UserSchema.def(om, registry.cf("users"), registry.cf(UserSchema.IDX_EMAIL));
        return new RocksDao<>(sessions, def);
    }
}

