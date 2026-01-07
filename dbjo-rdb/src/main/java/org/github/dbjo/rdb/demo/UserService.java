package org.github.dbjo.rdb.demo;

import org.github.dbjo.rdb.IndexPredicate;
import org.github.dbjo.rdb.Query;
import org.github.dbjo.rdb.demo.generated.proto.User;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;
import java.util.List;

@Service
public class UserService {

    private final UserDao userDao;

    public UserService(UserDao userDao) {
        this.userDao = userDao;
    }

    @Transactional
    public void insertDemoUsers() {
        userDao.upsert("u1", User.newBuilder()
                .setId("u1")
                .setEmail("alice@example.com")
                .setName("Alice")
                .build());

        userDao.upsert("u2", User.newBuilder()
                .setId("u2")
                .setEmail("bob@example.com")
                .setName("Bob")
                .build());

        userDao.upsert("u3", User.newBuilder()
                .setId("u3")
                .setEmail("alice@example.com")
                .setName("Alice Clone")
                .build());

        // all three either commit or roll back together
    }

    @Transactional(readOnly = true)
    public List<User> findByEmail(String email) {
        var q = Query.<String>builder()
                .where(new IndexPredicate.Eq(
                        UserSchema.IDX_EMAIL,   // safer than literal
                        email.getBytes(StandardCharsets.UTF_8)
                ))
                .limit(100)
                .build();

        try (var s = userDao.stream(q)) {
            return s.map(e -> e.getValue()).toList();
        }
    }
}
