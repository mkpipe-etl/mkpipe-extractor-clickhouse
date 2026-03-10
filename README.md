# mkpipe-extractor-clickhouse

ClickHouse extractor plugin for [MkPipe](https://github.com/mkpipe-etl/mkpipe). Reads ClickHouse tables using the **native `clickhouse-spark` connector**, which uses ClickHouse's binary HTTP protocol for columnar data transfer — faster than JDBC, especially for analytical queries.

## Documentation

For more detailed documentation, please visit the [GitHub repository](https://github.com/mkpipe-etl/mkpipe).

## License

This project is licensed under the Apache 2.0 License - see the [LICENSE](LICENSE) file for details.

---

## Connection Configuration

```yaml
connections:
  clickhouse_source:
    variant: clickhouse
    host: localhost
    port: 8123
    database: source_db
    user: default
    password: mypassword
```

---

## Table Configuration

```yaml
pipelines:
  - name: clickhouse_to_pg
    source: clickhouse_source
    destination: pg_target
    tables:
      - name: events
        target_name: stg_events
        replication_method: full
        fetchsize: 100000
```

### Incremental Replication

```yaml
      - name: events
        target_name: stg_events
        replication_method: incremental
        iterate_column: updated_at
        iterate_column_type: datetime
        partitions_column: id
        partitions_count: 8
        fetchsize: 50000
```

### Custom SQL

```yaml
      - name: events
        target_name: stg_events
        replication_method: full
        custom_query: "SELECT id, user_id, event_type, created_at FROM events WHERE {query_filter}"
```

Use `{query_filter}` as a placeholder — it is replaced with the incremental `WHERE` clause on incremental runs, or `WHERE 1=1` on full runs.

---

## Read Parallelism

ClickHouse extractor uses JDBC with Spark's native partition support. For large tables, set `partitions_column` and `partitions_count` to read in parallel:

```yaml
      - name: events
        target_name: stg_events
        replication_method: incremental
        iterate_column: created_at
        iterate_column_type: datetime
        partitions_column: id      # numeric column to split on
        partitions_count: 8        # number of parallel JDBC partitions
        fetchsize: 50000
```

### How it works

- Spark reads the min/max of `partitions_column` and divides the range into `partitions_count` equal slices
- Each slice is fetched by a separate Spark task via a separate JDBC connection
- `fetchsize` controls how many rows each connection fetches per round-trip

### Performance Notes

- **Full replication:** partitioning is not applied (only works with `incremental`).
- **`partitions_column`** should be a numeric column with good distribution (e.g. primary key).
- **`fetchsize`**: ClickHouse is a columnar store — large `fetchsize` (50,000–200,000) works well.
- ClickHouse handles large scans efficiently; for distributed tables parallelism is less critical than for row-based databases.

---

## All Table Parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | ClickHouse table name |
| `target_name` | string | required | Destination table name |
| `replication_method` | `full` / `incremental` | `full` | Replication strategy |
| `iterate_column` | string | — | Column used for incremental watermark |
| `iterate_column_type` | `int` / `datetime` | — | Type of `iterate_column` |
| `partitions_column` | string | same as `iterate_column` | Column to split JDBC reads on |
| `partitions_count` | int | `10` | Number of parallel JDBC partitions |
| `fetchsize` | int | `100000` | Rows per JDBC fetch |
| `custom_query` | string | — | Override SQL with `{query_filter}` placeholder |
| `custom_query_file` | string | — | Path to SQL file (relative to `sql/` dir) |
| `write_partitions` | int | — | Coalesce to N partitions before writing |
| `tags` | list | `[]` | Tags for selective pipeline execution |
| `pass_on_error` | bool | `false` | Skip table on error instead of failing |



