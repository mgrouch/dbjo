package org.github.dbjo.rdb.demo;

import org.github.dbjo.rdb.IndexKeyCodec;
import org.github.dbjo.rdb.IndexPredicate;
import org.github.dbjo.rdb.KeyRange;
import org.github.dbjo.rdb.Query;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Map;

@Service
public class UserService {

    private static final IndexKeyCodec<String> EMAIL_CODEC = IndexKeyCodec.stringUtf8();

    private final UserDao userDao;

    public UserService(UserDao userDao) {
        this.userDao = userDao;
    }

    @Transactional
    public void insertDemoUsers() {
        User u1 = new User(); u1.setId("u1"); u1.setEmail("alice@example.com"); u1.setName("Alice");
        userDao.upsert("u1", u1);

        User u2 = new User(); u2.setId("u2"); u2.setEmail("bob@example.com"); u2.setName("Bob");
        userDao.upsert("u2", u2);

        User u3 = new User(); u3.setId("u3"); u3.setEmail("alice@example.com"); u3.setName("Alice Clone");
        userDao.upsert("u3", u3);
    }

    @Transactional(readOnly = true)
    public java.util.Optional<User> get(String id) {
        return userDao.findByKey(id);
    }

    @Transactional(readOnly = true)
    public List<User> scanIdRange(String fromIncl, String toExcl, int limit) {
        var q = Query.<String>builder()
                .range(KeyRange.closedOpen(fromIncl, toExcl))
                .limit(limit)
                .build();

        try (var s = userDao.stream(q)) {
            return s.map(Map.Entry::getValue).toList();
        }
    }

    @Transactional(readOnly = true)
    public List<User> findByEmail(String email) {
        var q = Query.<String>builder()
                .where(new IndexPredicate.Eq(UserSchema.IDX_EMAIL, EMAIL_CODEC.encode(email)))
                .limit(100)
                .build();

        try (var s = userDao.stream(q)) {
            return s.map(Map.Entry::getValue).toList();
        }
    }
}

