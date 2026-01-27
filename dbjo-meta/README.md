# dbjo-meta

Core runtime library for DBJO.

This module provides:
- `DbMeta<T>` and `DbMetaUpsertSupport<T>` runtime APIs used by generated `*DbMeta` classes
- JDBC utilities (`Jdbc` binder/helpers, batch count analysis, etc.)
- upsert/batching helpers (`DbBatchBuilder`, `BatchUpsert`)
- (optional) criteria / query spec binding types used by higher-level API

It is intended to be a small, dependency-light runtime shipped with applications that
use generated metadata classes.

## Add dependency

```xml
<dependency>
  <groupId>org.github.dbjo</groupId>
  <artifactId>dbjo-meta</artifactId>
  <version>${dbjo.version}</version>
</dependency>
````

## Key types

### `DbMeta<T>`

Represents table metadata + mapping/binding:

* SQL strings: insert/update/select (and optional upsert variants)
* parameter arrays + JDBC `SQLType[]`
* `fromRow(ResultSet)` mapping

Generator emits `public static final <X>DbMeta INSTANCE` for each table.

### `DbMetaUpsertSupport<T>`

Extension of `DbMeta<T>` that adds dialect-specific upsert + temp-table merge support:

* `upsertByIdSql(dialect)`
* temp table DDL/DML templates
* merge-from-temp templates for MSSQL/Sybase style merges

### `Jdbc`

Utilities around `PreparedStatement` binding and batch counts:

* `Jdbc.bind(ps, params, types)`
* `Jdbc.sumBatchCounts(int[])` (treats `SUCCESS_NO_INFO` as 1)
* `Jdbc.analyzeBatchCounts(int[])` (sum + no-info count + failed count)

### `DbBatchBuilder`

Simple batch helper for `DbMeta<T>`:

* chooses temp-table strategy when supported
* otherwise falls back to plain batched upserts

### `BatchUpsert<T>`

Fluent helper around `DbMetaUpsertSupport<T>`:

* temp-table + merge (MSSQL/Sybase)
* direct prepared statement batching (Oracle/HSQL/etc.)

Example:

```java
try (BatchUpsert<MyRow> bu = BatchUpsert
        .builder(dataSource, dialect, MyRowDbMeta.INSTANCE)
        .batchSize(500)
        .suffix("X")
        .open()) {

    for (MyRow r : rows) bu.add(r);
    int affected = bu.flush(); // or bu.flushResult()
}
```

### Type-safe criteria

```java
import org.github.dbjo.criteria.*;
import static com.acme.db.query.ClientQ.*;

Query<Client> q = Query.from(ClientMeta._META)
    .where(EMAIL.eq("a@b.com").and(AGE.ge(18)))
    .limit(50)
    .build();
```

## Criteria Examples

```java
import org.github.dbjo.criteria.*;
import org.github.dbjo.criteria.eval.QueryEvaluator;

import static com.acme.db.query.ClientQ.*; // generated Q-terms (optional)

// Build a type-safe query (server-side)
Query<Client> q = Query.from(ClientMeta._META)
    .where(
        EMAIL.eq("a@b.com")
            .and(AGE.ge(18))
            .and(REGION.in("US", "CA"))
    )
    // Optional scan bound (useful for key-range / index-range scans)
    .scan(ClientMeta.ID, Range.closedOpen(100L, 200L))
    .limit(100)
    .build();

// In-memory evaluation (works anywhere)
List<Client> filtered = allClients.stream()
    .filter(c -> QueryEvaluator.test(q, c))
    .toList();
```

### Compile to SQL (JDBC)

```java
import org.github.dbjo.criteria.sql.SqlCriteriaCompiler;

var compiled = SqlCriteriaCompiler.compileSelectAll(ClientDbMeta.INSTANCE, q);
System.out.println(compiled.sql());    // SELECT ... WHERE ...
System.out.println(compiled.params()); // JDBC bind params
```

### Wire format: QuerySpec (for REST sending to a server)

```java
import org.github.dbjo.criteria.bind.QueryBinder;
import org.github.dbjo.criteria.spec.*;
import org.github.dbjo.meta.entity.DefaultMetaRegistry;

// JSON -> QuerySpec (using Jackson, etc.)
QuerySpec spec = new QuerySpec(
    Client.class.getName(),
    new AndSpec(List.of(
        new EqSpec("email", "a@b.com"),
        new CmpSpec("age", "GE", 18),
        new InSpec("region", List.of("US", "CA"))
    )),
    new ScanSpec("id", new RangeSpec(100, "INCLUSIVE", 200, "EXCLUSIVE")),
    100
);

// Generated registry wires entityId -> EntityMeta
DefaultMetaRegistry registry = GeneratedMetaRegistry.create();

Query<Client> typed = new QueryBinder(registry).fromSpec(spec);
```