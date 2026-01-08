package org.github.dbjo.rdb.demo;

import org.github.dbjo.rdb.*;
import org.github.dbjo.rdb.demo.generated.dao.UserDao;
import org.github.dbjo.rdb.demo.generated.protomap.UserProtoMapper;
import org.springframework.context.annotation.*;

@Configuration(proxyBeanMethods = false)
public class DemoDaosConfig {

    @Bean
    public UserProtoMapper userProtoMapper() {
        return new UserProtoMapper();
    }

    @Bean
    public UserDao userDao(RocksSessions sessions, DaoRegistry registry) {
        return new UserDao(sessions, registry);
    }
}
