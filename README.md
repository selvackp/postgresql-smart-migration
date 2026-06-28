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

## Configuration Notes

The main settings live under `migration`.

- `batch_size`: rows per batch.
- `checkpoint_file`: JSON checkpoint path.
- `error_table`: target-side bad-row table name.
- `fail_on_error_log_failure`: when `false` (default), a failure writing to the error table is logged and migration continues; set to `true` to fail the table.
- `timezone`: database session timezone used while reading/writing timestamps. The example uses `Asia/Kolkata`; use `UTC` if your migration convention is UTC.
- `disable_triggers_globally`: disables user triggers during each table load unless a table overrides it with `disable_triggers_during_load`.
- `reset_sequences`: resets serial/bigserial/identity-backed sequences after each successful table load.
- `stop_on_table_error`: when `false`, one table failure is logged and the next table continues.
- `truncate_long_strings`: when `true`, trims over-length `varchar(n)` values and logs a warning; when `false`, logs those rows to `migration_error_log`.
- `business_key_columns`: optional; if omitted, the script detects target PK, source PK, target unique key, then source unique key.
- `partition_type`: set to `range` or `list` with `partition_column` to create missing partitions.

For incremental loads, each table must set `incremental_column`.

Per-table `column_defaults` can fill NULL source values before target NOT NULL validation:

```yaml
column_defaults:
  status: "active"
  retry_count: 0
```

`timezone` runs `SET TIME ZONE` on both source and target sessions. It affects `timestamptz` interpretation/display and PostgreSQL time functions, but it does not rewrite `timestamp without time zone` values.

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
