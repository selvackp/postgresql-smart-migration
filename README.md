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
- Uses durable target commits before advancing the external checkpoint.
- Writes checkpoints atomically to avoid corrupt checkpoint JSON.
- Uses a PostgreSQL advisory lock to prevent overlapping runs.
- Can disable and re-enable user triggers per table using `migration.disable_triggers_globally`.
- Resets sequence-backed columns after successful full or incremental table loads using `pg_get_serial_sequence`.
- Keeps UPSERT behavior using business keys and protects inserts with `ON CONFLICT DO NOTHING`.
- Auto-detects a primary key or unique key when `business_key_columns` is not configured.
- Creates missing range/list partitions when configured.
- Validates NOT NULL values, string length, JSON/JSONB values, and array literals.
- Logs bad rows into `migration_error_log`; numeric/decimal values are stored as strings, dates/timestamps as ISO text, UUIDs as strings, and binary values as Base64 text in `row_data` JSON.
- Isolates PostgreSQL data/integrity errors to individual rows and records conflict-skipped rows instead of losing them silently.
- Creates configured range/list partitions from each fetched batch instead of scanning the full source partition range up front.
- Records trigger state in `migration_trigger_state` and restores it automatically after an interrupted run.
- Reports per-table source-window, processed, inserted, updated, rejected, conflict-skipped, and source/target count reconciliation.
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
| `truncate_target_before_full_load` | `false` | On a full load with no existing table checkpoint, truncate the target before reading batches. Truncating a partitioned parent includes all child partition data but keeps partition definitions. Never uses `CASCADE`. A table can override this setting. |
| `stop_on_table_error` | `false` | Stop the run after one table failure when `true`; otherwise continue with remaining tables. |
| `create_missing_partitions` | `true` | Create range/list child partitions before loading configured partitioned targets. |
| `partition_granularity` | `monthly` | Range partition interval: `daily` or `monthly`. |
| `range_partition_name_format` | Existing convention | Optional range child name template. Supports `{table}`, `{yyyy}`, `{mm}`, `{dd}`, `{yyyymm}`, and `{yyyymmdd}`. |
| `list_partition_name_format` | Existing convention | Optional list child name template. Supports `{table}`, `{column}`, and `{value}`. |
| `create_default_partition` | `true` | After a configured incremental partitioned table finishes loading, ensure one DEFAULT child exists as a safety catch-all. Ignored for full loads. |
| `default_partition_name_format` | `{table}_default` | Optional DEFAULT child name template. Supports `{table}`. |
| `disable_triggers_globally` | `false` | Disable target USER triggers during each table load. A table can override this setting. |
| `validate_lengths` | `true` | Validate values written to target `varchar(n)` columns. |
| `validate_not_null` | `true` | Reject rows containing NULL for target NOT NULL columns after defaults are applied. |
| `truncate_long_strings` | `false` | Truncate over-length strings when `true`; otherwise reject the row. |
| `skip_bad_rows` | `true` | Log validation failures and continue. A table can override this setting. |
| `fail_on_error_log_failure` | `false` | Continue when writing `migration_error_log` fails. Set `true` to fail the table instead. |
| `use_advisory_lock` | `true` | Prevent concurrent runs against the same target database. |
| `advisory_lock_key` | `987654321` | Signed bigint lock identifier. Use a stable, application-specific value. |
| `allow_incremental_without_target_unique_index` | `false` | Allow incremental merge without a target unique index after scanning current target data for duplicate business keys. This reduces safety against concurrent application writes and may be slower. A table can override this setting. |
| `reset_sequences` | `true` | Synchronize sequence-backed target columns after successful full and incremental loads. The sequence normally stores the preceding value with `is_called=true`, making `pg_sequences.last_value` visible immediately while keeping the next generated value at least the table maximum plus its configured increment. Boundary cases use `is_called=false`. The table is briefly locked against concurrent writes while values are reconciled. |
| `check_disabled_triggers_after_run` | `true` | Report target USER triggers that remain disabled at the end of the run. |
| `tables` | Required | List of per-table migration definitions. |

Sequence detection uses `pg_get_serial_sequence`, which returns the exact schema-qualified sequence owned by each serial, bigserial, or identity column. A manually assigned `nextval(...)` default must also have the correct `OWNED BY schema.table.column` relationship; otherwise PostgreSQL does not report it as the column's sequence and the migration logs that no sequence-backed column was found.

### Per-table settings

| Key | Required/default | Meaning |
| --- | --- | --- |
| `source_table` | Required | Source table or normal view name in `source.schema`. Metadata uses `information_schema.columns` with a `pg_catalog` fallback for accessible views. |
| `target_table` | Required | Target table name in `target.schema`. Names may differ. |
| `enabled` | `true` | Set `false` to skip this table without deleting its configuration. |
| `load_type` | Global default | `full` scans all source rows once; `incremental` uses a high-watermark column. Incremental requires a business/PK/unique key. Full load merges by key when one exists, or uses insert-only mode when no key exists. Full load truncates only when `truncate_target_before_full_load: true` is explicitly enabled. |
| `truncate_target_before_full_load` | Global setting | Optional per-table opt-in to empty the target only on the first checkpoint-free full-load attempt. Resume runs do not truncate again. |
| `incremental_column` | Incremental only | Source/target column used for high-watermark ordering, such as `updated_at` or `crtdate`. NULL values are not selected. |
| `business_key_columns` | Auto-detected | Stable, non-NULL data columns used for keyset pagination, updates, inserts, and resume. Detection order is target PK, source PK, target unique key, source unique key. Incremental loads require a matching non-partial target unique index; full loads only warn when it is absent. When the source lacks one, the script scans current source data for NULL and duplicate keys before loading. Required for incremental, optional for full load. |
| `allow_incremental_without_target_unique_index` | Global setting | Optional per-table opt-in for legacy targets that cannot add a unique index. Current target keys are checked for duplicates before loading. |
| `partition_column` | `null` | Target partition key. Use `null` for a non-partitioned target. The column must exist in source and target. |
| `partition_type` | `null` | `range`, `list`, or `null`. Must match the target parent's PostgreSQL partition strategy. |
| `range_partition_name_format` | Global setting | Optional per-table override for range partition names. |
| `list_partition_name_format` | Global setting | Optional per-table override for list partition names. |
| `create_default_partition` | Global setting | Optional per-table override for automatic DEFAULT partition creation on incremental loads. |
| `default_partition_name_format` | Global setting | Optional per-table override for the DEFAULT partition name. |
| `column_defaults` | `{}` | Values applied when a source value is NULL, and also used for target-only columns that are not present in the source. |
| `skip_bad_rows` | Global setting | Optional table override for bad-row behavior. |
| `disable_triggers_during_load` | Global setting | Optional table override for trigger disable/enable behavior. |

Business-key values in the source must contain no NULLs or duplicates. Incremental loads require the target key to have a matching non-partial unique index for replay-safe UPSERT behavior. Full loads may proceed without that target index and emit a warning, though merges can be slower. A source unique index is preferred for performance but is not mandatory; without one, pre-migration validation scans current source data. The first keyset page has no artificial lower-bound sentinel, so negative numbers, empty strings, and dates before 1900 are not skipped. For `load_type: full` tables without any key, the script uses insert-only mode with `OFFSET`-based batching; use it only for stable source tables, and preferably start from an empty target to avoid duplicates when no target unique constraint exists.

For a legacy incremental target that cannot add a unique index, opt in explicitly:

```yaml
- source_table: prorptthreatpriority
  target_table: prorptthreatpriority
  load_type: incremental
  incremental_column: crtdate
  business_key_columns:
    - id
  allow_incremental_without_target_unique_index: true
  enabled: true
```

The script verifies that current target `id` values are not duplicated before loading. Keep application writes controlled during migration because the database still cannot enforce uniqueness.

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

Source-authoritative first full load that starts from an empty target:

```yaml
- source_table: prosmchn
  target_table: prosmchn
  enabled: true
  load_type: full
  business_key_columns:
    - chnid
  truncate_target_before_full_load: true
```

This option applies only to enabled full-load table entries in the active config. After source/target existence validation, it issues `TRUNCATE TABLE schema.table RESTART IDENTITY` only when that table has no checkpoint entry, then immediately records a `TRUNCATED` checkpoint before loading batches. Missing source/target tables are logged and skipped before truncation. For a partitioned parent, PostgreSQL clears all child/default partition data and retains the partition definitions. The script does not use `CASCADE`; foreign-key dependencies must be handled explicitly before the run. Owned sequences restart during truncation and are synchronized again to migrated maximum values after the load.

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

For range partitions, each fetched batch creates only the daily or monthly children needed by that batch according to `partition_granularity`.

To create names such as `procmcustmappkey_p20260402`, configure:

```yaml
range_partition_name_format: "{table}_p{yyyymmdd}"
```

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

For list partitions, each fetched batch creates children for the distinct non-NULL partition values in that batch.

To create names such as `procmcust_p4101`, configure:

```yaml
list_partition_name_format: "{table}_p{value}"
```

Both format keys can be defined globally under `migration` or overridden inside an individual table entry. Existing partitions are matched by their bounds, so changing the format affects only newly created partitions.

After a configured incremental range/list table finishes loading, the script creates `{table}_default` when no DEFAULT child already exists. Full-load tables never create a DEFAULT partition automatically. This catch-all prevents later incremental inserts from failing when no explicit child covers a value. Before attaching a future range/list partition, move or remove any matching rows from the DEFAULT partition; PostgreSQL will reject an overlapping child while matching rows remain there. Set `create_default_partition: false` globally or per table to disable this behavior.

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

Pre-migration validation checks table existence, common columns, incremental/business/partition columns, datatype compatibility, and business-key safety. Compatible mappings such as `text` to `varchar(n)`, `varchar` to fixed `character(n)`, `date` to timestamp, and floating-point to `numeric` are allowed with a warning; row-level and PostgreSQL validation still enforce target limits. Target-only columns are skipped when nullable or when the target database has its own default/identity value. Target `GENERATED ALWAYS` identity columns are skipped even when the source has the same column, so PostgreSQL can assign the identity value. A target-only `NOT NULL` column without a database default must be provided in `column_defaults`, otherwise the table is skipped with a clear validation error. Row validation covers target NOT NULL constraints, string lengths, JSON/JSONB, and arrays. PostgreSQL data and integrity errors such as numeric overflow, CHECK, foreign-key, enum, and domain violations are isolated to individual rows. During bulk COPY, empty strings are preserved as empty strings and only Python `None` is written as SQL NULL. Rejected and conflict-skipped row payloads are written to `migration_error_log` using JSON-safe representations. The final summary lists every failed table with its error, every skipped table with its reason, and per-table reconciliation totals.
## Checkpoints

Checkpoints store:

- `last_incremental_value`
- `last_key_values`
- `high_watermark`
- `total_rows`
- `status`
- `business_key_columns`

Old checkpoints with a mismatched business-key definition are ignored for that table.

If a run stops unexpectedly, committed batches remain in the target and the next run resumes from the last atomic checkpoint. Target commits use `synchronous_commit=on` for the migration session. A batch committed immediately before its checkpoint update may be replayed; strict unique business-key validation plus UPSERT/`ON CONFLICT` handling makes that replay safe for keyed loads.

When `disable_triggers_globally: true`, the original state of each user trigger is stored in the target schema's `migration_trigger_state` table before triggers are disabled. Normal cleanup restores that state. If the process or server terminates, the next run restores recorded trigger states immediately after acquiring the advisory lock and before migrating tables. `sql/02_disabled_triggers.sql` remains useful as an independent safety check.

The reconciliation report uses source and target `COUNT(*)` queries. These provide strong operational visibility but may take time on very large tables. For incremental loads, `SourceWindow` is the number of rows eligible from the starting checkpoint through the captured high watermark; total source/target difference is informational because updates do not increase target row count.

## Production Run Checklist

- Back up the target and test the complete YAML configuration in staging first.
- Disable table entries whose source tables do not exist, and verify source/target schema names.
- Use stable business keys that match the logical row identity; partitioned-table unique keys normally include the partition column.
- Control application writes when `allow_incremental_without_target_unique_index: true` is used.
- Reconcile source/target values that conflict with alternate target unique constraints; full load does not bypass uniqueness or truncate the target.
- Remove only the affected table checkpoint before intentionally rerunning a completed full load.
- Review failed/skipped tables, `Unaccounted`, `WindowDifference`, conflict-skipped rows, and `migration_error_log` after every run.
- Confirm the disabled-trigger check is clean before returning the target to normal operation.
- Verify sequence state directly with `SELECT last_value, is_called FROM schema.sequence_name`; `pg_sequences.last_value` can be NULL for users without sequence privileges.

## Validation SQL

The `sql/` folder contains helper scripts:

- `01_error_log_summary.sql`
- `02_disabled_triggers.sql`
- `03_sequence_health.sql`
- `04_table_row_counts.sql`

Run them against the target database after migration.
