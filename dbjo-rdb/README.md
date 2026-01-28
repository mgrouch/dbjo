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

## RocksDB criteria API

```java
// IndexedRocksDao can push down simple predicates into index scans:
//  - Eq / Cmp / Between / Scan range
//  - small In(...) and small OR-of-Eq
// It still validates *full* criteria on candidates for safety.

Query<Person> q = Query.from(PersonMeta._META)
    .where(PersonQ.REGION.eq("NA")
        .and(PersonQ.AGE.between(30, 39)))
    .limit(100)
    .build();

List<Person> out = personDao.select(q);
```

## Resource management

RocksDB uses native resources heavily. DBJO code in this module generally assumes:

* `RocksIterator`, `WriteOptions`, `ReadOptions`, transactions, etc. must be closed
* try-with-resources wherever possible
* avoid holding iterators open across long application-level operations

## IntelliJ / DataGrip usage (read-only)

This project includes a tiny read-only JDBC driver that lets IntelliJ/DataGrip browse RocksDB “tables”
(backed by column families) and run simple queries like:

- `select * from tables`
- `select * from <table_name>`
- `select count(*) from <table_name>`

### 1) Build the driver and the generated catalog

From repo root:

```bash
mvn -pl dbjo-rdb,dbjo-sim-model -am clean package
````

You should get:

* `dbjo-rdb/target/dbjo-rdb-*.jar` (driver/runtime)
* `dbjo-sim-model/target/dbjo-sim-model-*.jar` (contains `GeneratedRocksJdbcCatalog`)

### 2) Register the driver in IntelliJ

1. Open **View → Tool Windows → Database**

2. Click **+ → Data Source → Driver** (or “Add Driver…”)

3. Add these JARs to the driver classpath (**Driver files / Libraries**):

    * `dbjo-rdb-*.jar`
    * `dbjo-sim-model-*.jar`
    * `rocksdbjni` (if not already on the classpath)
    * protobuf runtime + your proto-mapper module (if separate)

4. Set **Driver class** to:

```text
org.github.dbjo.rdb.jdbc.RocksJdbcDriver
```

### 3) Create a RocksDB data source

Create a new **Data Source** using the driver above and set the JDBC URL.

Example URL format:

The driver must support passing the generated catalog as a parameter, use:

```text
jdbc:rocksdb:/absolute/path/to/rocksdb?catalog=org.github.dbjo.generated.model.rdb.jdbc.GeneratedRocksJdbcCatalog
```

Then click **Test Connection**.

### 4) Querying

Open a query console for the data source and run:

```sql
select * from tables;
select * from client;
select count(*) from client;
```

