# dbjo-reveng

Reverse-engineering + code generation for DBJO.

This module introspects a live database via JDBC metadata and generates:
- `DbMeta` Java classes per table (SQL strings + row mapping + parameter binding)
- optional enum support (generated enum classes + enum column bindings)
- a simple `DbMetas` registry for lookup by `schema.table`

## What it generates

For each table:
- `<TableName>DbMeta` (extends `DbMetaUpsertSupport<T>`)
  - `insertSql()`, `updateByIdSql()`, `selectAllSql()`
  - dialect upsert SQL (`MERGE` variants) where supported
  - `fromRow(ResultSet)` mapper
  - `insertParams()`, `updateByIdParams()`, `upsertByIdParams()` + `SQLType[]`

It also generates:
- `registry/DbMetas` (list + map lookup by `fqn`)

## Running the generator

The main entrypoint is:

- `org.github.dbjo.codegen.DbjoCodegen`

### run with Maven 

From repo root:

```bash
mvn -pl dbjo-reveng -DskipTests exec:java \
  -Dexec.mainClass=org.github.dbjo.codegen.DbjoCodegen \
  -Dexec.args="--driver=org.hsqldb.jdbc.JDBCDriver --url=jdbc:hsqldb:mem:test --user=SA --pass= \
               --outBase=./build/gen \
               --beanPkg=com.acme.db.bean --dbMetaPkg=com.acme.db.meta \
               --sqlQuote=auto"
````

> If you don’t have `exec-maven-plugin` configured in the module, add it in the module POM
> or run via `java -cp` instead.

## Common flags

Exact flag names depend on your `Config` / `ArgMap` implementation, but the generator
expects typical JDBC + output + filtering parameters.

### JDBC

* `--driver` JDBC driver class name (e.g. `org.postgresql.Driver`)
* `--url` JDBC URL
* `--user`
* `--pass`

### Output

* `--outBase` root folder for generated sources (the generator typically writes under `codegenOutJava`)
* `--beanPkg` package for generated bean/record types (if you generate them elsewhere, point here)
* `--dbMetaPkg` package for generated `*DbMeta` classes

### Filtering (optional)

* `--schemaInclude`, `--schemaExclude`
* `--tableInclude`, `--tableExclude`

### SQL identifier quoting 

* `--sqlQuote=none|auto|always`

  * `none`: emit identifiers exactly as returned by metadata
  * `auto` (default): quote when needed (keywords, special chars, case)
  * `always`: quote schema/table/column identifiers aggressively

## Enum overrides

If you have enum tables or want to treat certain columns as enums, `EnumOverrideIndex`
can be loaded from a config file and used to:

* map column raw values (e.g. `int`) to enum values
* generate enum source files
* generate `fromRow()` and `params()` conversions
