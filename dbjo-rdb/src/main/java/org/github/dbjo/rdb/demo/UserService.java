package org.github.dbjo.rdb.demo;

import org.github.dbjo.rdb.*;
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
        userDao.upsert("u1", new User("u1", "alice@example.com", "Alice"));
        userDao.upsert("u2", new User("u2", "bob@example.com", "Bob"));
        userDao.upsert("u3", new User("u3", "alice@example.com", "Alice Clone"));
        // all three either commit or roll back together
    }

    @Transactional(readOnly = true)
    public List<User> findByEmail(String email) {
        var q = Query.<String>builder()
                .where(new IndexPredicate.Eq("users_email_idx", email.getBytes(StandardCharsets.UTF_8)))
                .limit(100)
                .build();

        try (var s = userDao.stream(q)) {
            return s.map(e -> e.getValue()).toList();
        }
    }
}
