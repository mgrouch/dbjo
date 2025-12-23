# HSQLDB In-Memory Demo (org.github.dbjo)

This is a tiny Maven project that:

1) Starts an embedded **HSQLDB in-memory** database (via JDBC connection)
2) Executes a DDL script (`src/main/resources/schema.sql`)
3) Creates **three tables** and prints them from `INFORMATION_SCHEMA`

## Run

### Option A: exec-maven-plugin

```bash
mvn -q clean compile exec:java
```

### Option B: build + run shaded jar

```bash
mvn -q clean package
java -jar target/dbjo-sim-db-1.0.0-SNAPSHOT.jar
```

## Tables

- `users`
- `products`
- `orders` (FKs to `users` and `products`)
