package org.github.dbjo.criteria.demo;

import java.io.Serializable;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;

import org.github.dbjo.criteria.*;
import org.github.dbjo.criteria.bind.QueryBinder;
import org.github.dbjo.criteria.cache.*;
import org.github.dbjo.criteria.eval.ConditionEvaluator;
import org.github.dbjo.criteria.spec.*;
import org.github.dbjo.meta.entity.DefaultMetaRegistry;
import org.github.dbjo.meta.entity.EntityMeta;
import org.github.dbjo.meta.entity.PropertyMeta;

public final class InMemoryCriteriaDemo {

    // Minimal POJO
    public static final class Client implements Serializable {
        private Long id;
        private String email;
        private String name;
        private Timestamp createdAt;

        public Client(Long id, String email, String name, Timestamp createdAt) {
            this.id = id; this.email = email; this.name = name; this.createdAt = createdAt;
        }
        public Long getId() { return id; }
        public void setId(Long id) { this.id = id; }
        public String getEmail() { return email; }
        public void setEmail(String email) { this.email = email; }
        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        public Timestamp getCreatedAt() { return createdAt; }
        public void setCreatedAt(Timestamp createdAt) { this.createdAt = createdAt; }

        @Override public String toString() {
            return "Client{id=" + id + ", email=" + email + ", name=" + name + ", createdAt=" + createdAt + "}";
        }
    }

    // Minimal meta (matches generator shape)
    public static final class ClientMeta {
        private ClientMeta() {}

        public static final PropertyMeta<Client, Long> ID =
                new PropertyMeta<>("id", Long.class, Client::getId, Client::setId);

        public static final PropertyMeta<Client, String> EMAIL =
                new PropertyMeta<>("email", String.class, Client::getEmail, Client::setEmail);

        public static final PropertyMeta<Client, String> NAME =
                new PropertyMeta<>("name", String.class, Client::getName, Client::setName);

        public static final PropertyMeta<Client, Timestamp> CREATED_AT =
                new PropertyMeta<>("createdAt", Timestamp.class, Client::getCreatedAt, Client::setCreatedAt);

        @SuppressWarnings({"rawtypes", "unchecked"})
        public static final EntityMeta<Client> _META = new EntityMeta<>(
                (List) List.of(ID, EMAIL, NAME, CREATED_AT),
                List.of("id", "email", "name", "createdAt"),
                List.of(Long.class, String.class, String.class, Timestamp.class)
        );
    }

    // Optional "Q" class (generated in real project)
    public static final class ClientQ {
        private ClientQ() {}
        public static final PropertyTerm<Client, Long> ID = Terms.prop(ClientMeta.ID);
        public static final PropertyTerm<Client, String> EMAIL = Terms.prop(ClientMeta.EMAIL);
        public static final PropertyTerm<Client, String> NAME = Terms.prop(ClientMeta.NAME);
        public static final PropertyTerm<Client, Timestamp> CREATED_AT = Terms.prop(ClientMeta.CREATED_AT);
    }

    public static void main(String[] args) {
        // Server: register meta
        var registry = new DefaultMetaRegistry()
                .register("PUBLIC.CLIENT", ClientMeta._META);

        var binder = new QueryBinder(registry);

        // In-memory "database" (single object)
        var client = new Client(
                1L,
                "a@b.com",
                "Alice",
                Timestamp.from(Instant.parse("2025-01-01T00:00:00Z"))
        );
        var data = List.of(client);

        // CLIENT SIDE: build a serializable QuerySpec
        QuerySpec spec = new QuerySpec(
                "PUBLIC.CLIENT",
                new AndSpec(List.of(
                        new EqSpec("email", "a@b.com"),
                        new CmpSpec("id", "GE", 1L)
                )),
                new ScanSpec("id", new RangeSpec(1L, "INCLUSIVE", 10L, "EXCLUSIVE")),
                10
        );

        // Cache key (canonicalized)
        var key = QueryCacheKeyFactory.from(spec);
        System.out.println("Cache key SHA-256: " + key.sha256Hex());
        System.out.println("Canonical string: " + key.canonicalString());

        // SERVER SIDE: bind spec -> typed Query<Client>
        Query<Client> query = binder.fromSpec(spec);

        // Execute in-memory (no DAO): filter list with ConditionEvaluator
        var results = data.stream()
                .filter(bean -> ConditionEvaluator.test(query.where(), bean))
                .limit(query.limit() == null ? Long.MAX_VALUE : query.limit())
                .toList();

        System.out.println("Results: " + results);

        // Also show server-side typed construction -> spec -> cache key
        Query<Client> typed = Query.from(ClientMeta._META)
                .where(ClientQ.EMAIL.eq("a@b.com").and(ClientQ.ID.ge(1L)))
                .scan(ClientMeta.ID, Range.closedOpen(1L, 10L))
                .limit(10)
                .build();

        QuerySpec typedSpec = binder.toSpec("PUBLIC.CLIENT", typed);
        var typedKey = QueryCacheKeyFactory.from(typedSpec);
        System.out.println("Typed->Spec cache key SHA-256: " + typedKey.sha256Hex());
    }
}
