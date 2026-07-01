# Postgres Smart Migration

Production-ready PostgreSQL table migration runner for full loads and high-watermark incremental syncs.

## Install

```bash
pip install -r requirements.txt
python migration_sync.py --config config.yaml
```

## What it does

- Runs full and incremental table loads.
- Uses a high-watermark incremental window plus business-key ordering for resumable batches.
- Writes checkpoints atomically to avoid corrupt checkpoint JSON.
- Uses a PostgreSQL advisory lock to prevent overlapping runs.
- Can disable and re-enable user triggers per table using `migration.disable_triggers_globally`.
- Resets sequence-backed columns after successful full or incremental table loads using `pg_get_serial_sequence`.
- Keeps UPSERT behavior using business keys and protects inserts with `ON CONFLICT DO NOTHING`.
- Auto-detects a primary key or unique key when `business_key_columns` is not configured.
- Creates missing range/list partitions when configured.
- Validates NOT NULL values, string length, JSON/JSONB values, and array literals.
- Logs bad rows into `migration_error_log`; numeric/decimal values are stored as strings, dates/timestamps as ISO text, UUIDs as strings, and binary values as Base64 text in `row_data` JSON.
- Continues other tables after a table failure when `stop_on_table_error: false`.

## Configuration Reference

### Database connections

Define both `source` and `target` using the same keys:

| Key | Required | Meaning |
| --- | --- | --- |
| `host` | Yes | PostgreSQL server host name or IP address. |
| `port` | No | PostgreSQL port; defaults to `5432`. |
| `database` | Yes | Database name. |
| `user` | Yes | Login role. The target role needs table DML, partition DDL, trigger, sequence, and error-table permissions for enabled features. |
| `password` | Yes | Login password. |
| `schema` | Yes | Source or target schema used by all configured tables. |

```yaml
source:
  host: "source-db.example.com"
  port: 5432
  database: "application"
  user: "migration_reader"
  password: "change-me"
  schema: "public"

target:
  host: "target-db.example.com"
  port: 5432
  database: "application"
  user: "migration_writer"
  password: "change-me"
  schema: "public"
```

### Global migration settings

All keys below belong under `migration`.

| Key | Required/default | Meaning |
| --- | --- | --- |
| `batch_size` | Required | Maximum source rows read and merged per batch. |
| `sleep_seconds` | Required | Delay between successful batches. Use `0` for no delay. |
| `max_retries` | Required | Attempts per batch before the table is marked failed. Use at least `1`. |
| `checkpoint_file` | Required | Atomic JSON checkpoint file path. Relative paths use the process working directory. |
| `error_table` | Required | Target table used for rejected rows; it is created automatically. |
| `timezone` | `UTC` | Session timezone applied to source and target connections. It does not rewrite `timestamp without time zone` values. |
| `default_load_type` | `incremental` | Load type used when a table does not define `load_type`. |
| `stop_on_table_error` | `false` | Stop the run after one table failure when `true`; otherwise continue with remaining tables. |
| `create_missing_partitions` | `true` | Create range/list child partitions before loading configured partitioned targets. |
| `partition_granularity` | `monthly` | Range partition interval: `daily` or `monthly`. |
| `disable_triggers_globally` | `false` | Disable target USER triggers during each table load. A table can override this setting. |
| `validate_lengths` | `true` | Validate values written to target `varchar(n)` columns. |
| `validate_not_null` | `true` | Reject rows containing NULL for target NOT NULL columns after defaults are applied. |
| `truncate_long_strings` | `false` | Truncate over-length strings when `true`; otherwise reject the row. |
| `skip_bad_rows` | `true` | Log validation failures and continue. A table can override this setting. |
| `fail_on_error_log_failure` | `false` | Continue when writing `migration_error_log` fails. Set `true` to fail the table instead. |
| `use_advisory_lock` | `true` | Prevent concurrent runs against the same target database. |
| `advisory_lock_key` | `987654321` | Signed bigint lock identifier. Use a stable, application-specific value. |
| `reset_sequences` | `true` | Reset sequence-backed target columns after successful full and incremental table loads. |
| `check_disabled_triggers_after_run` | `true` | Report target USER triggers that remain disabled at the end of the run. |
| `tables` | Required | List of per-table migration definitions. |

### Per-table settings

| Key | Required/default | Meaning |
| --- | --- | --- |
| `source_table` | Required | Source table name in `source.schema`. |
| `target_table` | Required | Target table name in `target.schema`. Names may differ. |
| `enabled` | `true` | Set `false` to skip this table without deleting its configuration. |
| `load_type` | Global default | `full` scans all source rows once; `incremental` uses a high-watermark column. Incremental requires a business/PK/unique key. Full load merges by key when one exists, or uses insert-only mode when no key exists. Full load does not truncate the target. |
| `incremental_column` | Incremental only | Source/target column used for high-watermark ordering, such as `updated_at` or `crtdate`. NULL values are not selected. |
| `business_key_columns` | Auto-detected | Stable columns used for keyset pagination, updates, inserts, and resume. Detection order is target PK, source PK, target unique key, source unique key. Required for incremental, optional for full load. |
| `partition_column` | `null` | Target partition key. Use `null` for a non-partitioned target. The column must exist in source and target. |
| `partition_type` | `null` | `range`, `list`, or `null`. Must match the target parent's PostgreSQL partition strategy. |
| `column_defaults` | `{}` | Values applied when a source value is NULL, and also used for target-only columns that are not present in the source. |
| `skip_bad_rows` | Global setting | Optional table override for bad-row behavior. |
| `disable_triggers_during_load` | Global setting | Optional table override for trigger disable/enable behavior. |

Business keys should be non-NULL and backed by a target primary-key or unique constraint. This makes checkpoint replay and `ON CONFLICT` handling reliable. For `load_type: full` tables without any key, the script uses insert-only mode with `OFFSET`-based batching; use it for stable source tables, and preferably start from an empty target to avoid duplicates when no target unique constraint exists.

### Table examples

Incremental load into a non-partitioned target:

```yaml
- source_table: orders
  target_table: orders
  enabled: true
  load_type: incremental
  incremental_column: updated_at
  partition_column: null
  partition_type: null
  business_key_columns:
    - order_id
  column_defaults:
    status: "NEW"
```

Full UPSERT into a non-partitioned target:

```yaml
- source_table: customer_source
  target_table: customer_target
  enabled: true
  load_type: full
  partition_column: null
  partition_type: null
  business_key_columns:
    - customer_id
  column_defaults: {}
```

A completed full load is skipped on later runs while its checkpoint status is `COMPLETED`. Remove only that table's checkpoint entry when an intentional full rerun is required.

Full insert-only load without a business key:

```yaml
- source_table: audit_archive
  target_table: audit_archive
  enabled: true
  load_type: full
  partition_column: null
  partition_type: null
  column_defaults: {}
```

This mode is useful when the source and target table have no PK/unique/business key. Because there is no stable keyset cursor, checkpoint resume uses the completed row count as an offset.

Non-partitioned source into a range-partitioned target:

```yaml
- source_table: order_history
  target_table: order_history_partitioned
  enabled: true
  load_type: incremental
  incremental_column: updated_at
  partition_column: created_at
  partition_type: range
  business_key_columns:
    - order_id
    - created_at
  column_defaults: {}
```

For range partitions, the script reads the source minimum/maximum partition values and creates daily or monthly children according to `partition_granularity`.

Non-partitioned source into a list-partitioned target:

```yaml
- source_table: customer_events
  target_table: customer_events_partitioned
  enabled: true
  load_type: incremental
  incremental_column: updated_at
  partition_column: channel_id
  partition_type: list
  business_key_columns:
    - channel_id
    - event_id
  disable_triggers_during_load: false
  skip_bad_rows: true
  column_defaults: {}
```

For list partitions, the script reads distinct non-NULL source partition values and creates one child partition per value.

### Partition requirements

The target parent table must already exist with PostgreSQL `PARTITION BY RANGE (...)` or `PARTITION BY LIST (...)`. The script creates missing child partitions; it does not convert a normal target table into a partitioned parent. Set both `partition_column` and `partition_type` to `null` for ordinary non-partitioned tables. The source may itself be partitioned or non-partitioned; no special source setting is required.

### Validation and defaults

`column_defaults` values are applied when the source value is NULL. They can also fill target-only columns that do not exist in the source table. Values must be compatible with the target column type:

```yaml
column_defaults:
  status: "active"
  retry_count: 0
  processed_at: "2026-01-01 00:00:00"
```

Pre-migration validation checks table existence, common columns, incremental/business/partition columns, and datatype compatibility. Compatible string mappings such as `text` to `varchar(n)` are allowed with a warning, then row-level length validation enforces the target limit. Target-only columns are skipped when nullable or when the target database has its own default/identity value. Target `GENERATED ALWAYS` identity columns are skipped even when the source has the same column, so PostgreSQL can assign the identity value. A target-only `NOT NULL` column without a database default must be provided in `column_defaults`, otherwise the table is skipped with a clear validation error. Row validation covers target NOT NULL constraints, string lengths, JSON/JSONB, and arrays. During bulk COPY, empty strings are preserved as empty strings and only Python `None` is written as SQL NULL. Rejected row payloads are written to `migration_error_log` using JSON-safe representations.
## Checkpoints

Checkpoints store:

- `last_incremental_value`
- `last_key_values`
- `high_watermark`
- `total_rows`
- `status`
- `business_key_columns`

Old checkpoints with a mismatched business-key definition are ignored for that table.

If a run stops unexpectedly, committed batches remain in the target and the next run resumes from the last atomic checkpoint. A batch committed immediately before its checkpoint update may be replayed; UPSERT/`ON CONFLICT` handling makes that replay safe when the configured business key is backed by a reliable unique constraint.

When `disable_triggers_globally: true`, normal exceptions re-enable triggers in cleanup. A forced process or server termination can leave table triggers disabled because `ALTER TABLE ... DISABLE TRIGGER USER` is persistent. Run `sql/02_disabled_triggers.sql` before restarting and re-enable any affected triggers.

## Validation SQL

The `sql/` folder contains helper scripts:

- `01_error_log_summary.sql`
- `02_disabled_triggers.sql`
- `03_sequence_health.sql`
- `04_table_row_counts.sql`

Run them against the target database after migration.
