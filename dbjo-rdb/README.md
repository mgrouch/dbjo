

# dbjo-rdb

RocksDB-backed storage layer for DBJO.

This module provides:
- schema + DAO abstractions for RocksDB (“tables”, column families, indexes)
- stable binary key encoding for ordered iteration/range scans (`IndexKeys`)
- iteration utilities (`DaoSpliterator`)
- transaction integration (`RocksDbTransactionManager`, transactional read/write helpers)

It is designed for:
- embedded deployments
- local/offline data stores
- high-performance ordered scans using RocksDB iterators

## Add dependency

```xml
<dependency>
  <groupId>org.github.dbjo</groupId>
  <artifactId>dbjo-rdb</artifactId>
  <version>${dbjo.version}</version>
</dependency>
````

You also need RocksDB on the classpath:

```xml
<dependency>
  <groupId>org.rocksdb</groupId>
  <artifactId>rocksdbjni</artifactId>
  <version>${rocksdb.version}</version>
</dependency>
```

## Concepts

### Schema

A schema defines:

* column families
* key/value formats
* secondary index layouts (if any)

Your project likely has generators that emit schema/DAO classes; this module is the runtime.

### DAO

A DAO provides:

* put/get/delete
* index lookups / range scans
* iterators (forward/backward)
* optional mapping to/from byte[] values

### Index key encoding (`IndexKeys`)

DBJO uses a binary encoding that preserves ordering so that lexicographic byte ordering
matches the intended sort order for composite keys.

This is what makes range scans efficient and correct.

### Iteration (`DaoSpliterator`)

Wraps RocksDB iterators with:

* inclusive/exclusive bound correctness
* safe resource management patterns
* predictable traversal semantics

### Transactions

`RocksDbTransactionManager` provides a structured way to run transactional work and ensure:

* native resources are closed
* tx state doesn’t leak between threads
* “read writes” behavior inside tx

## Typical usage pattern (sketch)

```java
try (var db = RocksDB.open(options, path)) {
    var schema = MyRocksSchema.open(db, schemaOptions);
    var dao = schema.myEntityDao();

    // write
    dao.put(entityId, bytes);

    // read
    byte[] v = dao.get(entityId);

    // range scan (index)
    try (var it = dao.scanByIndex(indexLower, indexUpper)) {
        while (it.hasNext()) {
            var row = it.next();
            // ...
        }
    }
}
```

(Exact API depends on generated schema/dao classes.)

## Resource management

RocksDB uses native resources heavily. DBJO code in this module generally assumes:

* `RocksIterator`, `WriteOptions`, `ReadOptions`, transactions, etc. must be closed
* try-with-resources wherever possible
* avoid holding iterators open across long application-level operations




