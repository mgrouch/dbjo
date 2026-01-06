package org.github.dbjo.rdb.demo;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.github.dbjo.rdb.*;
import org.springframework.context.annotation.*;

import java.util.Map;

@Configuration
public class DemoDaosConfig {

    @Bean
    public UserDao userDao(RocksSessions sessions, RocksDbHandle h, ObjectMapper om, DaoRegistry registry) {
        var usersCf = h.cf("users");
        var indexCfs = Map.of(UserDao.IDX_EMAIL, h.cf(UserDao.IDX_EMAIL));
        var dao = new UserDao(sessions, usersCf, indexCfs, new JacksonCodec<>(om, User.class));
        registry.register(UserDao.class, dao);
        return dao;
    }
}
