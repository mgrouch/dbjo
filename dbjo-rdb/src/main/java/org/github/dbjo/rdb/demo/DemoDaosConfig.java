package org.github.dbjo.rdb.demo;

import org.github.dbjo.rdb.*;
import org.github.dbjo.rdb.demo.generated.proto.User;
import org.springframework.context.annotation.*;

import java.util.Map;

@Configuration(proxyBeanMethods = false)
public class DemoDaosConfig {

    @Bean
    public UserDao userDao(RocksSessions sessions, DaoRegistry registry) {
        var usersCf = registry.cf(UserSchema.USERS_CF);
        var indexCfs = Map.of(UserSchema.IDX_EMAIL, registry.cf(UserSchema.IDX_EMAIL));

        return new UserDao(
                sessions,
                usersCf,
                indexCfs,
                ProtobufCodec.ofDefault(User.getDefaultInstance())
        );
    }
}
