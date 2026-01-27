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

