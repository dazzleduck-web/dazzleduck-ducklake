# dazzleduck-ducklake

Java utilities for managing DuckLake table compaction and file replacement. The library handles the two-phase snapshot mechanism required to atomically replace data files in a DuckLake catalog, with optional support for a direct Postgres backend that provides race-free snapshot ID generation via `INSERT...RETURNING`.

## Features

- **File replacement** — atomically swap old Parquet files for new ones using DuckLake's snapshot mechanism
- **Partition rewrite** — rewrite files into a new Hive-partitioned layout without committing metadata
- **File listing** — enumerate active data files within a size range for compaction scheduling
- **Postgres backend** — pooled direct JDBC connection with `INSERT...RETURNING` for concurrent-safe snapshot creation
- **Partition-aware deletion** — retire files matching a SQL filter using partition pruning

## Installation

```xml
<dependency>
    <groupId>io.dazzleduck.sql</groupId>
    <artifactId>ducklake</artifactId>
    <version>0.0.17</version>
</dependency>
```

### Build from source

```bash
./mvnw clean install
```

## Requirements

- Java 21+
- Maven 3.6+
- JVM timezone must be a valid IANA name (e.g. `UTC`, `America/Los_Angeles`).
  Non-IANA POSIX aliases such as `US/Pacific` are rejected by Postgres containers.
  Set `-Duser.timezone=UTC` in your JVM args or Surefire `<argLine>`.

## Usage

### Replace files (DuckDB / SQLite backend)

Atomically register new Parquet files and retire old ones under a new DuckLake snapshot:

```java
long snapshotId = MergeTableOpsUtil.replace(
    catalog,     // DuckLake catalog name (e.g. "my_ducklake")
    tableId,     // table_id from ducklake_table metadata
    mdDatabase,  // metadata database name (e.g. "__ducklake_metadata_my_ducklake")
    toAdd,       // List<String> of absolute paths of new Parquet files
    toRemove     // List<String> of file names to retire
);
```

### Replace files (Postgres backend)

When a Postgres metadata backend is used, initialise the connection pool once at startup.
`replace()` then uses `INSERT...RETURNING` for race-free snapshot ID generation:

```java
// Once at application startup
MetadataConfig.initPostgres(host, port, database, user, password);

// Use replace() exactly as above — backend is selected automatically
long snapshotId = MergeTableOpsUtil.replace(catalog, tableId, mdDatabase, toAdd, toRemove);

// On shutdown
MetadataConfig.reset();
```

The pool is backed by HikariCP (`maximumPoolSize=5`, `minimumIdle=0`).

### Rewrite with partitioning

Rewrite files into a new Hive-partitioned layout. Does **not** update metadata — pair with `replace()` to commit:

```java
// Multiple input files
List<String> newFiles = MergeTableOpsUtil.rewriteWithPartitionNoCommit(
    inputFiles,    // List<String> of source Parquet paths
    baseLocation,  // destination directory
    partition      // List<String> of partition column names; empty = no partitioning
);

// Single input file
List<String> newFiles = MergeTableOpsUtil.rewriteWithPartitionNoCommit(
    inputFile,     // source Parquet path
    baseLocation,  // destination directory or file path
    partition      // partition column names
);
```

### List files

Enumerate active data files within a size range, useful for identifying compaction candidates:

```java
List<FileStatus> files = MergeTableOpsUtil.listFiles(
    mdDatabase,  // metadata database name
    tableId,     // table ID
    minSize,     // minimum file size in bytes (inclusive)
    maxSize      // maximum file size in bytes (inclusive)
);
```

### Delete files by partition filter

Retire files matching a SQL WHERE clause using partition pruning:

```java
List<String> retired = MergeTableOpsUtil.deleteDirectlyFromMetadata(
    tableId,     // table ID
    mdDatabase,  // metadata database name
    filter       // SQL SELECT with WHERE clause identifying partitions to delete
);
```

### Look up table ID

```java
Long tableId = MergeTableOpsUtil.lookupTableId(mdDatabase, tableName); // null if not found
long tableId = MergeTableOpsUtil.requireTableId(mdDatabase, tableName); // throws if not found
```

## Running tests

```bash
./mvnw test
```

Tests use Testcontainers and require a running Docker daemon. The Postgres and MinIO containers are started automatically.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.
