import os
import io
import csv
import json
import time
import yaml
import logging
import ast
import argparse
import base64
import hashlib
from datetime import datetime, timedelta, date
from decimal import Decimal
from uuid import UUID

import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values, Json

logging.basicConfig(filename="migration_incremental_sync.log", level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger("").addHandler(console)

NUMERIC_TYPES = ("bigint", "integer", "smallint", "numeric", "decimal", "real", "double precision")
JSON_TYPES = ("json", "jsonb")


class PreMigrationValidationError(Exception):
    pass


class ConflictRowsDetected(Exception):
    pass


def load_config(config_file="config_incremental_load.yaml"):
    with open(config_file, "r") as f:
        cfg = yaml.safe_load(f)
    validate_config(cfg)
    return cfg


def validate_config(cfg):
    if not isinstance(cfg, dict):
        raise ValueError("Config file must contain a YAML mapping")
    for section in ("source", "target", "migration"):
        if section not in cfg:
            raise ValueError(f"Missing required config section: {section}")
    migration_cfg = cfg["migration"]
    if "migration" in migration_cfg:
        raise ValueError("Config has nested migration.migration. Remove the extra 'migration:' line under migration.")
    if not migration_cfg.get("tables"):
        raise ValueError("Config must include migration.tables")


def get_connection(db_cfg, app_name):
    return psycopg2.connect(
        host=db_cfg["host"], port=db_cfg.get("port", 5432), dbname=db_cfg["database"],
        user=db_cfg["user"], password=db_cfg["password"], connect_timeout=10, application_name=app_name
    )


def read_checkpoint(file_name):
    if not os.path.exists(file_name):
        return {}
    with open(file_name, "r") as f:
        return json.load(f)


def write_checkpoint(file_name, data):
    """
    Atomic checkpoint write.
    Prevents corrupted JSON if the process/server stops during checkpoint update.
    """
    tmp_file = f"{file_name}.tmp"
    with open(tmp_file, "w") as f:
        json.dump(data, f, default=str, indent=2)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp_file, file_name)


def get_load_type(cfg, table_cfg):
    if not table_cfg.get("enabled", True):
        return "skip"
    return table_cfg.get("load_type", cfg["migration"].get("default_load_type", "incremental")).lower()


def is_incremental_insert_only_duplicate_mode(table_cfg, load_type=None):
    load_type = load_type or str(table_cfg.get("load_type", "")).lower()
    return (
        load_type == "incremental"
        and table_cfg.get("insert_only") is True
        and table_cfg.get("allow_duplicates") is True
    )


def parse_value(value):
    if value in (None, "", "None", "null", "NULL"):
        return None
    if isinstance(value, (datetime, date, int, float, Decimal)):
        return value
    s = str(value)
    for fn in (datetime.fromisoformat, lambda x: date.fromisoformat(x[:10]), int, float):
        try:
            return fn(s)
        except Exception:
            pass
    return value


def get_table_columns(conn, schema_name, table_name):
    query = """
        SELECT column_name, data_type, udt_name, character_maximum_length,
               numeric_precision, numeric_scale, is_nullable,
               column_default, is_identity, identity_generation
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
    """
    with conn.cursor() as cur:
        cur.execute(query, (schema_name, table_name))
        rows = cur.fetchall()
    return {
        r[0]: {
            "data_type": r[1],
            "udt_name": r[2],
            "max_length": r[3],
            "numeric_precision": r[4],
            "numeric_scale": r[5],
            "is_nullable": r[6],
            "column_default": r[7],
            "is_identity": r[8],
            "identity_generation": r[9],
        }
        for r in rows
    }

def get_primary_key_columns(conn, schema_name, table_name):
    query = """
        SELECT kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name
         AND tc.table_schema = kcu.table_schema
         AND tc.table_name = kcu.table_name
        WHERE tc.constraint_type = 'PRIMARY KEY'
          AND tc.table_schema = %s AND tc.table_name = %s
        ORDER BY kcu.ordinal_position
    """
    with conn.cursor() as cur:
        cur.execute(query, (schema_name, table_name))
        return [r[0] for r in cur.fetchall()]


def get_unique_key_columns(conn, schema_name, table_name):
    query = """
        WITH selected_constraint AS (
            SELECT tc.constraint_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema = kcu.table_schema
             AND tc.table_name = kcu.table_name
            WHERE tc.constraint_type = 'UNIQUE'
              AND tc.table_schema = %s
              AND tc.table_name = %s
            GROUP BY tc.constraint_name
            ORDER BY count(*) ASC, tc.constraint_name
            LIMIT 1
        )
        SELECT kcu.column_name
        FROM selected_constraint sc
        JOIN information_schema.key_column_usage kcu
          ON kcu.constraint_name = sc.constraint_name
         AND kcu.table_schema = %s
         AND kcu.table_name = %s
        ORDER BY kcu.ordinal_position
    """
    with conn.cursor() as cur:
        cur.execute(query, (schema_name, table_name, schema_name, table_name))
        return [r[0] for r in cur.fetchall()]


def get_unique_indexes(conn, schema_name, table_name):
    query = """
        SELECT idx.relname, att.attname, key_col.ordinality
        FROM pg_class tbl
        JOIN pg_namespace ns ON ns.oid = tbl.relnamespace
        JOIN pg_index ind ON ind.indrelid = tbl.oid
        JOIN pg_class idx ON idx.oid = ind.indexrelid
        JOIN LATERAL unnest(ind.indkey::smallint[]) WITH ORDINALITY
          AS key_col(attnum, ordinality)
          ON key_col.ordinality <= ind.indnkeyatts
        JOIN pg_attribute att
          ON att.attrelid = tbl.oid
         AND att.attnum = key_col.attnum
        WHERE ns.nspname = %s
          AND tbl.relname = %s
          AND ind.indisunique
          AND ind.indisvalid
          AND ind.indisready
          AND ind.indpred IS NULL
          AND ind.indexprs IS NULL
        ORDER BY idx.relname, key_col.ordinality
    """
    with conn.cursor() as cur:
        cur.execute(query, (schema_name, table_name))
        rows = cur.fetchall()
    indexes = {}
    for index_name, column_name, _ in rows:
        indexes.setdefault(index_name, []).append(column_name)
    return list(indexes.values())


def has_matching_unique_index(conn, schema_name, table_name, business_key_columns):
    expected = set(business_key_columns)
    return any(len(columns) == len(expected) and set(columns) == expected
               for columns in get_unique_indexes(conn, schema_name, table_name))


def source_business_key_has_null(conn, schema_name, table_name, business_key_columns):
    null_condition = sql.SQL(" OR ").join(
        sql.SQL("{column} IS NULL").format(column=sql.Identifier(column))
        for column in business_key_columns
    )
    query = sql.SQL("SELECT 1 FROM {schema}.{table} WHERE {condition} LIMIT 1").format(
        schema=sql.Identifier(schema_name),
        table=sql.Identifier(table_name),
        condition=null_condition,
    )
    with conn.cursor() as cur:
        cur.execute(query)
        return cur.fetchone() is not None


def source_business_key_has_duplicates(conn, schema_name, table_name, business_key_columns):
    key_columns = sql.SQL(", ").join(sql.Identifier(column) for column in business_key_columns)
    query = sql.SQL("""
        SELECT 1
        FROM {schema}.{table}
        GROUP BY {key_columns}
        HAVING COUNT(*) > 1
        LIMIT 1
    """).format(
        schema=sql.Identifier(schema_name),
        table=sql.Identifier(table_name),
        key_columns=key_columns,
    )
    with conn.cursor() as cur:
        cur.execute(query)
        return cur.fetchone() is not None


def validate_business_key_safety(
    source_conn,
    target_conn,
    source_schema,
    target_schema,
    source_table,
    target_table,
    source_meta,
    target_meta,
    business_key_columns,
    load_type="incremental",
    allow_incremental_without_target_unique_index=False,
):
    if not business_key_columns:
        return
    if len(set(business_key_columns)) != len(business_key_columns):
        raise PreMigrationValidationError(
            f"[{target_table}] business_key_columns contains duplicate column names: {business_key_columns}"
        )
    nullable_source = [c for c in business_key_columns if source_meta[c]["is_nullable"] != "NO"]
    nullable_target = [c for c in business_key_columns if target_meta[c]["is_nullable"] != "NO"]
    if nullable_source and source_business_key_has_null(
        source_conn, source_schema, source_table, business_key_columns
    ):
        raise PreMigrationValidationError(
            f"[{target_table}] Source business key {business_key_columns} contains NULL values"
        )
    if nullable_source:
        logging.warning(
            f"[{target_table}] Source business-key columns are nullable by schema but current data contains no NULL keys: {nullable_source}"
        )
    if nullable_target:
        logging.warning(
            f"[{target_table}] Target business-key columns are nullable by schema: {nullable_target}"
        )
    if not has_matching_unique_index(source_conn, source_schema, source_table, business_key_columns):
        logging.warning(
            f"[{target_table}] Source business key has no matching unique index; validating source data for duplicates"
        )
        if source_business_key_has_duplicates(
            source_conn, source_schema, source_table, business_key_columns
        ):
            raise PreMigrationValidationError(
                f"[{target_table}] Source business key {business_key_columns} contains duplicate values"
            )
        logging.warning(
            f"[{target_table}] Source business key {business_key_columns} is unique in current data but is not index-backed"
        )
    if not has_matching_unique_index(target_conn, target_schema, target_table, business_key_columns):
        if load_type == "incremental" and not allow_incremental_without_target_unique_index:
            raise PreMigrationValidationError(
                f"[{target_table}] Target business key {business_key_columns} is not backed by a non-partial unique index"
            )
        if load_type == "incremental":
            if source_business_key_has_duplicates(
                target_conn, target_schema, target_table, business_key_columns
            ):
                raise PreMigrationValidationError(
                    f"[{target_table}] Incremental load without a target unique index is enabled, "
                    f"but target business key {business_key_columns} contains duplicate values"
                )
            logging.warning(
                f"[{target_table}] Incremental load proceeding without a target unique index for "
                f"business key {business_key_columns}. Current target data is unique, but concurrent "
                "application writes can still create duplicates and merge performance may be reduced."
            )
            return
        logging.warning(
            f"[{target_table}] Full load proceeding without a target unique index for business key "
            f"{business_key_columns}; merge performance and concurrent-write protection may be reduced"
        )

def normalize_type_name(meta):
    data_type = meta["data_type"]
    udt_name = meta.get("udt_name")
    if data_type == "USER-DEFINED" and udt_name:
        return udt_name
    return data_type


def is_type_compatible(source_meta, target_meta):
    source_type = normalize_type_name(source_meta)
    target_type = normalize_type_name(target_meta)
    if source_type == target_type:
        return True, False

    source_udt = source_meta.get("udt_name")
    target_udt = target_meta.get("udt_name")
    if source_udt and target_udt and source_udt == target_udt:
        return True, False

    compatible_pairs = {
        ("smallint", "integer"),
        ("smallint", "bigint"),
        ("smallint", "numeric"),
        ("smallint", "decimal"),
        ("integer", "bigint"),
        ("integer", "numeric"),
        ("integer", "decimal"),
        ("bigint", "numeric"),
        ("bigint", "decimal"),
        ("numeric", "integer"),
        ("numeric", "bigint"),
        ("numeric", "smallint"),
        ("decimal", "integer"),
        ("decimal", "bigint"),
        ("decimal", "smallint"),
        ("bigint", "integer"),
        ("bigint", "smallint"),
        ("real", "double precision"),
        ("real", "numeric"),
        ("real", "decimal"),
        ("double precision", "numeric"),
        ("double precision", "decimal"),
        ("character varying", "text"),
        ("character varying", "character"),
        ("character", "text"),
        ("text", "character varying"),
        ("text", "character"),
        ("date", "timestamp without time zone"),
        ("date", "timestamp with time zone"),
        ("timestamp without time zone", "timestamp with time zone"),
        ("timestamp without time zone", "date"),
        ("timestamp with time zone", "date"),
    }
    if (source_type, target_type) in compatible_pairs:
        return True, True

    if source_type in JSON_TYPES and target_type in JSON_TYPES:
        return True, True

    if source_type == "ARRAY" and target_type == "ARRAY" and source_udt == target_udt:
        return True, False

    return False, False


def validate_datatype_compatibility(source_meta, target_meta, common_columns, target_table):
    errors = []
    for col in common_columns:
        compatible, warn = is_type_compatible(source_meta[col], target_meta[col])
        source_type = normalize_type_name(source_meta[col])
        target_type = normalize_type_name(target_meta[col])
        if compatible and warn:
            logging.warning(f"[{target_table}] Compatible datatype difference for {col}: source={source_type}, target={target_type}")
        elif not compatible:
            errors.append(f"{col}: source={source_type}, target={target_type}")
    if errors:
        raise PreMigrationValidationError(f"[{target_table}] Incompatible datatype mappings: {'; '.join(errors)}")


def detect_business_key(source_conn, target_conn, cfg, table_cfg):
    source_schema, target_schema = cfg["source"]["schema"], cfg["target"]["schema"]
    source_table, target_table = table_cfg["source_table"], table_cfg["target_table"]
    if table_cfg.get("business_key_columns"):
        return table_cfg["business_key_columns"]
    for label, conn, schema, table in [
        ("target PK", target_conn, target_schema, target_table),
        ("source PK", source_conn, source_schema, source_table),
        ("target unique key", target_conn, target_schema, target_table),
        ("source unique key", source_conn, source_schema, source_table),
    ]:
        cols = get_primary_key_columns(conn, schema, table) if "PK" in label else get_unique_key_columns(conn, schema, table)
        if cols:
            logging.info(f"[{target_table}] Auto detected {label}: {cols}")
            return cols
    raise Exception(f"[{target_table}] No business_key_columns and no PK/unique key found. Add business_key_columns in YAML.")


def get_min_max_column(conn, schema_name, table_name, column_name):
    query = sql.SQL("SELECT MIN({col}), MAX({col}) FROM {schema}.{table} WHERE {col} IS NOT NULL").format(
        col=sql.Identifier(column_name), schema=sql.Identifier(schema_name), table=sql.Identifier(table_name))
    with conn.cursor() as cur:
        cur.execute(query)
        return cur.fetchone()


def get_distinct_partition_values(conn, schema_name, table_name, column_name):
    query = sql.SQL("SELECT DISTINCT {col} FROM {schema}.{table} WHERE {col} IS NOT NULL ORDER BY {col}").format(
        col=sql.Identifier(column_name), schema=sql.Identifier(schema_name), table=sql.Identifier(table_name))
    with conn.cursor() as cur:
        cur.execute(query)
        return [r[0] for r in cur.fetchall()]


def get_initial_incremental_value(min_value):
    if isinstance(min_value, datetime): return min_value - timedelta(seconds=1)
    if isinstance(min_value, date): return min_value - timedelta(days=1)
    if isinstance(min_value, int): return min_value - 1
    if isinstance(min_value, float): return min_value - 1
    if isinstance(min_value, Decimal): return min_value - Decimal(1)
    return min_value


def get_initial_key_values(target_meta, business_key_columns):
    values = []
    for col in business_key_columns:
        data_type = target_meta[col]["data_type"]
        if data_type in NUMERIC_TYPES: values.append(-1)
        elif data_type == "date": values.append(date(1900, 1, 1))
        elif data_type in ("timestamp without time zone", "timestamp with time zone"): values.append(datetime(1900, 1, 1, 0, 0, 0))
        else: values.append("")
    return values


def convert_key_values(values, target_meta, business_key_columns):
    converted = []
    for value, col in zip(values, business_key_columns):
        if value in (None, "", "None", "null", "NULL"):
            converted.append(None); continue
        data_type = target_meta[col]["data_type"]
        if data_type in ("bigint", "integer", "smallint"): converted.append(int(value))
        elif data_type in ("numeric", "decimal"): converted.append(Decimal(str(value)))
        elif data_type in ("real", "double precision"): converted.append(float(value))
        elif data_type == "date": converted.append(date.fromisoformat(str(value)[:10]))
        elif data_type in ("timestamp without time zone", "timestamp with time zone"): converted.append(datetime.fromisoformat(str(value)))
        else: converted.append(value)
    return converted



def checkpoint_matches_business_key(table_checkpoint, business_key_columns):
    """
    Old checkpoints may contain different key columns or different key count.
    If mismatch happens, ignore that checkpoint for the table and restart that table safely.
    """
    if not table_checkpoint:
        return False

    old_keys = table_checkpoint.get("business_key_columns")
    old_values = table_checkpoint.get("last_key_values")

    if old_keys and old_keys != business_key_columns:
        return False

    if old_values is None:
        return False

    if len(old_values) != len(business_key_columns):
        return False

    return True


def serialize_key_values(values):
    if values is None:
        return None
    return [str(value) for value in values]


def create_error_table(target_conn, target_schema, error_table):
    query = sql.SQL("""
        CREATE TABLE IF NOT EXISTS {schema}.{error_table}
        (id bigserial PRIMARY KEY, table_name text, business_key text, error_message text, row_data jsonb, error_time timestamptz DEFAULT now())
    """).format(schema=sql.Identifier(target_schema), error_table=sql.Identifier(error_table))
    with target_conn.cursor() as cur: cur.execute(query)
    target_conn.commit()


def create_trigger_state_table(target_conn, target_schema):
    query = sql.SQL("""
        CREATE TABLE IF NOT EXISTS {schema}.migration_trigger_state (
            advisory_lock_key bigint NOT NULL,
            table_schema text NOT NULL,
            table_name text NOT NULL,
            trigger_name text NOT NULL,
            previous_state "char" NOT NULL,
            recorded_at timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (advisory_lock_key, table_schema, table_name, trigger_name)
        )
    """).format(schema=sql.Identifier(target_schema))
    with target_conn.cursor() as cur:
        cur.execute(query)
    target_conn.commit()


def json_safe_value(value):
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, (bytes, bytearray, memoryview)):
        return base64.b64encode(bytes(value)).decode("ascii")
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, dict):
        return {str(key): json_safe_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_safe_value(item) for item in value]
    return str(value)


def log_bad_rows(target_conn, target_schema, error_table, table_name, bad_rows, fail_on_error=False):
    if not bad_rows:
        return
    try:
        insert_sql = sql.SQL("INSERT INTO {schema}.{error_table} (table_name,business_key,error_message,row_data) VALUES %s").format(
            schema=sql.Identifier(target_schema), error_table=sql.Identifier(error_table))
        values = [
            (
                table_name,
                x["business_key"],
                x["error_message"],
                Json(json_safe_value(x["row_data"])),
            )
            for x in bad_rows
        ]
        with target_conn.cursor() as cur:
            execute_values(cur, insert_sql.as_string(target_conn), values)
        target_conn.commit()
    except Exception as e:
        logging.exception(f"[{table_name}] Failed to write {len(bad_rows)} bad row(s) to {error_table}: {e}")
        try:
            target_conn.rollback()
        except Exception:
            logging.exception(f"[{table_name}] Failed to roll back error-log transaction")
        if fail_on_error:
            raise


def target_has_database_default(meta):
    return meta.get("column_default") is not None or meta.get("is_identity") == "YES"


def is_generated_always_identity(meta):
    return meta.get("is_identity") == "YES" and str(meta.get("identity_generation") or "").upper() == "ALWAYS"


def prepare_metadata(source_conn, target_conn, cfg, table_cfg, load_type):
    source_schema, target_schema = cfg["source"]["schema"], cfg["target"]["schema"]
    source_table, target_table = table_cfg["source_table"], table_cfg["target_table"]
    source_meta, target_meta = get_table_columns(source_conn, source_schema, source_table), get_table_columns(target_conn, target_schema, target_table)
    if not source_meta: raise PreMigrationValidationError(f"[{target_table}] Source table not found: {source_schema}.{source_table}")
    if not target_meta: raise PreMigrationValidationError(f"[{target_table}] Target table not found: {target_schema}.{target_table}")
    source_columns = [c for c in target_meta.keys() if c in source_meta.keys()]
    if not source_columns: raise PreMigrationValidationError(f"[{target_table}] No common columns found")
    validate_datatype_compatibility(source_meta, target_meta, source_columns, target_table)

    column_defaults = table_cfg.get("column_defaults") or {}
    unknown_defaults = [c for c in column_defaults if c not in target_meta]
    if unknown_defaults:
        raise PreMigrationValidationError(f"[{target_table}] column_defaults configured for target columns that do not exist: {unknown_defaults}")

    generated_always_columns = [c for c, meta in target_meta.items() if is_generated_always_identity(meta)]
    target_only_columns = [c for c in target_meta.keys() if c not in source_meta]
    missing_required_columns = [
        c for c in target_only_columns
        if target_meta[c]["is_nullable"] == "NO"
        and c not in column_defaults
        and not target_has_database_default(target_meta[c])
    ]
    if missing_required_columns:
        raise PreMigrationValidationError(
            f"[{target_table}] Target NOT NULL columns missing from source and without DB/config default: {missing_required_columns}"
        )

    target_columns = [
        c for c in target_meta.keys()
        if not is_generated_always_identity(target_meta[c])
        and (c in source_meta or (c in column_defaults and not target_has_database_default(target_meta[c])))
    ]
    default_only_columns = [c for c in target_columns if c not in source_meta]
    if default_only_columns:
        logging.info(f"[{target_table}] Target-only columns filled from column_defaults: {default_only_columns}")
    skipped_identity_columns = [c for c in generated_always_columns if c in source_meta or c in column_defaults]
    if skipped_identity_columns:
        logging.info(f"[{target_table}] Skipping GENERATED ALWAYS identity columns so PostgreSQL can assign values: {skipped_identity_columns}")

    incremental_column = table_cfg.get("incremental_column")
    if load_type == "incremental" and not incremental_column: raise PreMigrationValidationError(f"[{target_table}] incremental_column is required for incremental load")
    if incremental_column and incremental_column not in source_columns: raise PreMigrationValidationError(f"[{target_table}] incremental_column {incremental_column} not found in source and target")
    if incremental_column and incremental_column in generated_always_columns:
        raise PreMigrationValidationError(f"[{target_table}] incremental_column {incremental_column} is a target GENERATED ALWAYS identity column and cannot be loaded")
    configured_business_key = bool(table_cfg.get("business_key_columns"))
    incremental_insert_only = is_incremental_insert_only_duplicate_mode(table_cfg, load_type)
    if incremental_insert_only:
        business_key_columns = []
        logging.info(f"[{target_table}] Incremental insert-only duplicate mode enabled")
    else:
        try:
            business_key_columns = detect_business_key(source_conn, target_conn, cfg, table_cfg)
        except Exception as e:
            if load_type == "full":
                logging.warning(
                    f"[{target_table}] No business key/PK/unique key found. "
                    "Proceeding with full load using insert-only mode."
                )
                business_key_columns = []
            else:
                raise PreMigrationValidationError(str(e))

    generated_identity_key_columns = [c for c in business_key_columns if c in generated_always_columns]
    if generated_identity_key_columns:
        if load_type == "full" and not configured_business_key:
            logging.warning(
                f"[{target_table}] Auto-detected key uses target GENERATED ALWAYS identity columns "
                f"{generated_identity_key_columns}. Proceeding with full load using insert-only mode."
            )
            business_key_columns = []
        else:
            raise PreMigrationValidationError(
                f"[{target_table}] Business key columns are target GENERATED ALWAYS identity columns and cannot be loaded: {generated_identity_key_columns}"
            )

    for key_col in business_key_columns:
        if key_col not in source_columns:
            raise PreMigrationValidationError(
                f"[{target_table}] Business key {key_col} not found in source and target"
            )
    if not incremental_insert_only:
        validate_business_key_safety(
            source_conn,
            target_conn,
            source_schema,
            target_schema,
            source_table,
            target_table,
            source_meta,
            target_meta,
            business_key_columns,
            load_type,
            table_cfg.get(
                "allow_incremental_without_target_unique_index",
                cfg["migration"].get("allow_incremental_without_target_unique_index", False),
            ),
        )
    partition_column = table_cfg.get("partition_column")
    if partition_column and partition_column not in source_columns: raise PreMigrationValidationError(f"[{target_table}] Partition column {partition_column} not found in source and target")
    if partition_column and partition_column in generated_always_columns:
        raise PreMigrationValidationError(f"[{target_table}] Partition column {partition_column} is a target GENERATED ALWAYS identity column and cannot be loaded")
    logging.info(f"[{target_table}] Source mapped columns: {source_columns}")
    logging.info(f"[{target_table}] Target load columns: {target_columns}")
    logging.info(f"[{target_table}] Business key columns: {business_key_columns}")
    logging.info(f"[{target_table}] Load type: {load_type}")
    return source_columns, target_columns, source_meta, target_meta, business_key_columns

def is_array_column(meta):
    return meta.get("data_type") == "ARRAY" or str(meta.get("udt_name", "")).startswith("_")


def normalize_json_value(value):
    if value is None: return None
    if isinstance(value, (dict, list, int, float, bool)): return json.dumps(value, default=str)
    if isinstance(value, Decimal): return json.dumps(float(value))
    if isinstance(value, str):
        s = value.strip()
        if s == "": return None
        try:
            json.loads(s)
            return s
        except Exception:
            pass
        try:
            return json.dumps(ast.literal_eval(s), default=str)
        except Exception as e:
            raise ValueError(f"invalid JSON/JSONB value: {e}")
    return json.dumps(value, default=str)


def normalize_array_value(value):
    if value is None: return None
    if isinstance(value, list): values = value
    elif isinstance(value, tuple): values = list(value)
    elif isinstance(value, str):
        s = value.strip()
        if s == "": return None
        if s.startswith("{") and s.endswith("}"):
            return s
        try:
            parsed = ast.literal_eval(s)
            values = list(parsed) if isinstance(parsed, (list, tuple)) else [parsed]
        except Exception as e:
            raise ValueError(f"invalid ARRAY value: {e}")
    else:
        values = [value]
    escaped = []
    for item in values:
        if item is None: escaped.append("NULL")
        else: escaped.append('"' + str(item).replace("\\", "\\\\").replace('"', '\\"') + '"')
    return "{" + ",".join(escaped) + "}"


def validate_normalized_value(col, value, meta):
    if value is None:
        return None
    if meta["data_type"] in JSON_TYPES:
        try:
            json.loads(value)
        except Exception as e:
            return f"{col} contains invalid {meta['data_type']}: {e}"
    if is_array_column(meta) and not (isinstance(value, str) and value.startswith("{") and value.endswith("}")):
        return f"{col} contains invalid ARRAY literal"
    return None


def is_limited_string_column(meta):
    return meta.get("data_type") in ("character varying", "character") and meta.get("max_length")


def apply_column_defaults(row_dict, table_cfg):
    defaults = table_cfg.get("column_defaults") or {}
    for col, default_value in defaults.items():
        if col not in row_dict or row_dict[col] is None:
            row_dict[col] = default_value
    return row_dict


def normalize_row_for_target(row, source_columns, target_columns, target_meta, table_cfg):
    row_dict = apply_column_defaults(dict(zip(source_columns, row)), table_cfg)
    normalized = []
    for col in target_columns:
        value, meta = row_dict.get(col), target_meta[col]
        if meta["data_type"] in JSON_TYPES: value = normalize_json_value(value)
        elif is_array_column(meta): value = normalize_array_value(value)
        normalized.append(value)
    return tuple(normalized)


def validate_rows(rows, source_columns, target_columns, target_meta, business_key_columns, cfg, table_cfg):
    validate_lengths = cfg["migration"].get("validate_lengths", True)
    validate_not_null = cfg["migration"].get("validate_not_null", True)
    truncate_long_strings = cfg["migration"].get("truncate_long_strings", False)
    skip_bad_rows = table_cfg.get("skip_bad_rows", cfg["migration"].get("skip_bad_rows", True))
    good_rows, bad_rows = [], []
    for row in rows:
        errors = []
        try:
            normalized_row = normalize_row_for_target(row, source_columns, target_columns, target_meta, table_cfg)
            row_dict = dict(zip(target_columns, normalized_row))
        except Exception as e:
            row_dict = apply_column_defaults(dict(zip(source_columns, row)), table_cfg)
            errors.append(str(e))
        for col, value in row_dict.items():
            meta = target_meta[col]
            if validate_not_null and meta["is_nullable"] == "NO" and value is None:
                errors.append(f"{col} is NULL but target column is NOT NULL")
            if validate_lengths and value is not None and is_limited_string_column(meta) and isinstance(value, str) and len(value) > meta["max_length"]:
                if truncate_long_strings:
                    logging.warning(f"Truncated long string in column {col}: length {len(value)} exceeds target max length {meta['max_length']}")
                    row_dict[col] = value[:meta["max_length"]]
                else:
                    errors.append(f"{col} length {len(value)} exceeds target max length {meta['max_length']}")
            validation_error = validate_normalized_value(col, value, meta)
            if validation_error:
                errors.append(validation_error)
        if errors:
            business_key = "|".join(str(row_dict.get(c)) for c in business_key_columns)
            error_message = "; ".join(errors)
            bad_rows.append({"business_key": business_key, "error_message": error_message, "row_data": row_dict})
            if not skip_bad_rows: raise Exception(f"Bad row found for key {business_key}: {error_message}")
        else:
            good_rows.append(tuple(row_dict[c] for c in target_columns))
    return good_rows, bad_rows

def normalize_date(value):
    if isinstance(value, datetime): return value.date()
    if isinstance(value, date): return value
    raise Exception(f"Range partition column must be date/timestamp, got {type(value)}")


def partition_start(value, granularity):
    d = normalize_date(value)
    if granularity == "daily": return d
    if granularity == "monthly": return date(d.year, d.month, 1)
    raise Exception("partition_granularity must be daily or monthly")


def next_partition_date(d, granularity):
    if granularity == "daily": return d + timedelta(days=1)
    if granularity == "monthly":
        return date(d.year + (d.month // 12), d.month % 12 + 1, 1)
    raise Exception("partition_granularity must be daily or monthly")


def render_range_partition_name(target_table, partition_date, granularity, name_format=None):
    if name_format:
        daily_tokens = "{yyyymmdd}" in name_format or all(
            token in name_format for token in ("{yyyy}", "{mm}", "{dd}")
        )
        monthly_tokens = "{yyyymm}" in name_format or all(
            token in name_format for token in ("{yyyy}", "{mm}")
        )
        if granularity == "daily" and not daily_tokens:
            raise ValueError("Daily range partition name format must uniquely include year, month, and day")
        if granularity == "monthly" and not monthly_tokens:
            raise ValueError("Monthly range partition name format must uniquely include year and month")
        values = {
            "table": target_table,
            "yyyy": f"{partition_date.year:04d}",
            "mm": f"{partition_date.month:02d}",
            "dd": f"{partition_date.day:02d}",
            "yyyymm": f"{partition_date.year:04d}{partition_date.month:02d}",
            "yyyymmdd": f"{partition_date.year:04d}{partition_date.month:02d}{partition_date.day:02d}",
        }
        try:
            return bounded_identifier(name_format.format_map(values), "")
        except KeyError as error:
            raise ValueError(f"Unsupported range partition name placeholder: {error}") from error
    suffix = f"{partition_date.year}_{partition_date.month:02d}_{partition_date.day:02d}" if granularity == "daily" else f"{partition_date.year}_{partition_date.month:02d}"
    return bounded_identifier(target_table, suffix)


def create_range_partitions(target_conn, target_schema, target_table, min_value, max_value, granularity, name_format=None):
    if min_value is None or max_value is None: return
    current = partition_start(min_value, granularity)
    end = next_partition_date(partition_start(max_value, granularity), granularity)
    while current < end:
        next_date = next_partition_date(current, granularity)
        partition_name = render_range_partition_name(target_table, current, granularity, name_format)
        query = sql.SQL("CREATE TABLE IF NOT EXISTS {schema}.{partition} PARTITION OF {schema}.{parent} FOR VALUES FROM (%s) TO (%s)").format(
            schema=sql.Identifier(target_schema), partition=sql.Identifier(partition_name), parent=sql.Identifier(target_table))
        try:
            with target_conn.cursor() as cur: cur.execute(query, (current, next_date))
            target_conn.commit(); logging.info(f"[{target_table}] Range partition ensured: {partition_name}")
        except Exception as e:
            target_conn.rollback()
            if "would overlap partition" in str(e): logging.warning(f"[{target_table}] Range partition overlap. Skipping {current} to {next_date}")
            else: raise e
        current = next_date


def safe_partition_suffix(value):
    return str(value).replace("-", "_").replace(" ", "_").replace(":", "_").replace(".", "_")


def bounded_identifier(prefix, suffix, max_bytes=63):
    candidate = f"{prefix}_{suffix}" if suffix else prefix
    if len(candidate.encode("utf-8")) <= max_bytes:
        return candidate
    digest = hashlib.sha256(candidate.encode("utf-8")).hexdigest()[:12]
    available = max_bytes - len(digest) - 1
    shortened = prefix
    while shortened and len(shortened.encode("utf-8")) > available:
        shortened = shortened[:-1]
    return f"{shortened}_{digest}"


def render_default_partition_name(target_table, name_format=None):
    if not name_format:
        return bounded_identifier(target_table, "default")
    try:
        return bounded_identifier(name_format.format_map({"table": target_table}), "")
    except KeyError as error:
        raise ValueError(f"Unsupported default partition name placeholder: {error}") from error


def get_default_partition(target_conn, target_schema, target_table):
    query = """
        SELECT child.relname
        FROM pg_inherits inheritance
        JOIN pg_class parent ON parent.oid = inheritance.inhparent
        JOIN pg_namespace parent_schema ON parent_schema.oid = parent.relnamespace
        JOIN pg_class child ON child.oid = inheritance.inhrelid
        WHERE parent_schema.nspname = %s
          AND parent.relname = %s
          AND pg_get_expr(child.relpartbound, child.oid) = 'DEFAULT'
        LIMIT 1
    """
    with target_conn.cursor() as cur:
        cur.execute(query, (target_schema, target_table))
        row = cur.fetchone()
    return row[0] if row else None


def ensure_default_partition(target_conn, cfg, table_cfg, load_type):
    if load_type != "incremental":
        return None
    if not table_cfg.get("partition_column"):
        return None
    enabled = table_cfg.get(
        "create_default_partition",
        cfg["migration"].get("create_default_partition", True),
    )
    if not enabled:
        return None
    target_schema = cfg["target"]["schema"]
    target_table = table_cfg["target_table"]
    existing = get_default_partition(target_conn, target_schema, target_table)
    if existing:
        logging.info(f"[{target_table}] Default partition already exists: {existing}")
        return existing
    name_format = table_cfg.get(
        "default_partition_name_format",
        cfg["migration"].get("default_partition_name_format"),
    )
    partition_name = render_default_partition_name(target_table, name_format)
    query = sql.SQL(
        "CREATE TABLE {schema}.{partition} PARTITION OF {schema}.{parent} DEFAULT"
    ).format(
        schema=sql.Identifier(target_schema),
        partition=sql.Identifier(partition_name),
        parent=sql.Identifier(target_table),
    )
    with target_conn.cursor() as cur:
        cur.execute(query)
    target_conn.commit()
    logging.info(f"[{target_table}] Default partition created: {partition_name}")
    return partition_name


def render_list_partition_name(target_table, partition_column, value, name_format=None):
    safe_value = safe_partition_suffix(value)
    if name_format:
        if "{value}" not in name_format:
            raise ValueError("List partition name format must include {value}")
        values = {"table": target_table, "column": partition_column, "value": safe_value}
        try:
            return bounded_identifier(name_format.format_map(values), "")
        except KeyError as error:
            raise ValueError(f"Unsupported list partition name placeholder: {error}") from error
    return bounded_identifier(target_table, f"{partition_column}_{safe_value}")


def create_list_partitions(target_conn, target_schema, target_table, partition_column, values, name_format=None):
    for value in values:
        partition_name = render_list_partition_name(
            target_table, partition_column, value, name_format
        )
        query = sql.SQL("CREATE TABLE IF NOT EXISTS {schema}.{partition} PARTITION OF {schema}.{parent} FOR VALUES IN (%s)").format(
            schema=sql.Identifier(target_schema), partition=sql.Identifier(partition_name), parent=sql.Identifier(target_table))
        try:
            with target_conn.cursor() as cur: cur.execute(query, (value,))
            target_conn.commit(); logging.info(f"[{target_table}] List partition ensured: {partition_name}")
        except Exception as e:
            target_conn.rollback()
            if "would overlap partition" in str(e): logging.warning(f"[{target_table}] List partition already covers value {value}")
            else: raise e


def ensure_partitions(source_conn, target_conn, cfg, table_cfg):
    if not cfg["migration"].get("create_missing_partitions", True): return
    source_schema, target_schema = cfg["source"]["schema"], cfg["target"]["schema"]
    source_table, target_table = table_cfg["source_table"], table_cfg["target_table"]
    partition_column, partition_type = table_cfg.get("partition_column"), table_cfg.get("partition_type")
    name_format_key = f"{partition_type}_partition_name_format"
    name_format = table_cfg.get(name_format_key, cfg["migration"].get(name_format_key))
    if not partition_column:
        logging.info(f"[{target_table}] Partition creation skipped"); return
    if partition_type == "range":
        part_min, part_max = get_min_max_column(source_conn, source_schema, source_table, partition_column)
        create_range_partitions(target_conn, target_schema, target_table, part_min, part_max, cfg["migration"].get("partition_granularity", "monthly"), name_format)
    elif partition_type == "list":
        values = get_distinct_partition_values(source_conn, source_schema, source_table, partition_column)
        create_list_partitions(target_conn, target_schema, target_table, partition_column, values, name_format)
    else:
        raise Exception(f"[{target_table}] Invalid partition_type: {partition_type}")


def ensure_partitions_for_rows(target_conn, cfg, table_cfg, source_columns, rows):
    if not cfg["migration"].get("create_missing_partitions", True):
        return
    partition_column = table_cfg.get("partition_column")
    if not partition_column or not rows:
        return
    target_schema = cfg["target"]["schema"]
    target_table = table_cfg["target_table"]
    partition_type = table_cfg.get("partition_type")
    name_format_key = f"{partition_type}_partition_name_format"
    name_format = table_cfg.get(name_format_key, cfg["migration"].get(name_format_key))
    column_index = source_columns.index(partition_column)
    values = [row[column_index] for row in rows if row[column_index] is not None]
    if not values:
        return
    if partition_type == "range":
        granularity = cfg["migration"].get("partition_granularity", "monthly")
        required_starts = sorted({partition_start(value, granularity) for value in values})
        for required_start in required_starts:
            create_range_partitions(
                target_conn,
                target_schema,
                target_table,
                required_start,
                required_start,
                granularity,
                name_format,
            )
    elif partition_type == "list":
        create_list_partitions(
            target_conn,
            target_schema,
            target_table,
            partition_column,
            list(dict.fromkeys(values)),
            name_format,
        )
    else:
        raise Exception(f"[{target_table}] Invalid partition_type: {partition_type}")


def create_temp_table(target_conn, target_schema, target_table, temp_table, columns):
    col_sql = sql.SQL(", ").join(sql.Identifier(c) for c in columns)
    query = sql.SQL("DROP TABLE IF EXISTS {temp_table}; CREATE TEMP TABLE {temp_table} AS SELECT {cols} FROM {schema}.{table} WHERE 1 = 2").format(
        temp_table=sql.Identifier(temp_table), cols=col_sql, schema=sql.Identifier(target_schema), table=sql.Identifier(target_table))
    with target_conn.cursor() as cur: cur.execute(query)
    target_conn.commit()


def fetch_incremental_batch(source_conn, source_schema, source_table, columns, incremental_column, business_key_columns, last_incremental_value, last_key_values, high_watermark, batch_size):
    select_cols = sql.SQL(", ").join(sql.Identifier(c) for c in columns)
    order_columns = [incremental_column] + business_key_columns
    order_sql = sql.SQL(", ").join(sql.Identifier(c) for c in order_columns)
    key_cols_sql = sql.SQL(", ").join(sql.Identifier(c) for c in business_key_columns)
    key_placeholders = sql.SQL(", ").join(sql.Placeholder() for _ in business_key_columns)
    if last_incremental_value is None:
        query = sql.SQL("""
            SELECT {select_cols} FROM {schema}.{table}
            WHERE {inc} IS NOT NULL AND {inc} <= %s
            ORDER BY {order_cols} LIMIT %s
        """).format(
            select_cols=select_cols,
            schema=sql.Identifier(source_schema),
            table=sql.Identifier(source_table),
            inc=sql.Identifier(incremental_column),
            order_cols=order_sql,
        )
        params = [high_watermark, batch_size]
    else:
        query = sql.SQL("""
            SELECT {select_cols} FROM {schema}.{table}
            WHERE {inc} IS NOT NULL AND {inc} <= %s
              AND ({inc} > %s OR ({inc} = %s AND ({key_cols}) > ({key_values})))
            ORDER BY {order_cols} LIMIT %s
        """).format(select_cols=select_cols, schema=sql.Identifier(source_schema), table=sql.Identifier(source_table), inc=sql.Identifier(incremental_column), key_cols=key_cols_sql, key_values=key_placeholders, order_cols=order_sql)
        params = [high_watermark, last_incremental_value, last_incremental_value] + list(last_key_values) + [batch_size]
    with source_conn.cursor() as cur:
        cur.execute(query, params)
        return cur.fetchall()


def fetch_incremental_insert_only_batch(
    source_conn, source_schema, source_table, columns, incremental_column,
    last_incremental_value, high_watermark, batch_size
):
    select_cols = sql.SQL(", ").join(sql.Identifier(c) for c in columns)
    query = sql.SQL("""
        SELECT {select_cols} FROM {schema}.{table}
        WHERE {inc} > %s AND {inc} <= %s
        ORDER BY {inc}
        LIMIT %s
    """).format(
        select_cols=select_cols,
        schema=sql.Identifier(source_schema),
        table=sql.Identifier(source_table),
        inc=sql.Identifier(incremental_column),
    )
    with source_conn.cursor() as cur:
        cur.execute(query, (last_incremental_value, high_watermark, batch_size))
        return cur.fetchall()


def fetch_full_batch(source_conn, source_schema, source_table, columns, business_key_columns, last_key_values, batch_size):
    select_cols = sql.SQL(", ").join(sql.Identifier(c) for c in columns)
    order_sql = sql.SQL(", ").join(sql.Identifier(c) for c in business_key_columns)
    key_cols_sql = sql.SQL(", ").join(sql.Identifier(c) for c in business_key_columns)
    key_placeholders = sql.SQL(", ").join(sql.Placeholder() for _ in business_key_columns)
    if last_key_values is None:
        query = sql.SQL("SELECT {select_cols} FROM {schema}.{table} ORDER BY {order_cols} LIMIT %s").format(
            select_cols=select_cols,
            schema=sql.Identifier(source_schema),
            table=sql.Identifier(source_table),
            order_cols=order_sql,
        )
        params = [batch_size]
    else:
        query = sql.SQL("SELECT {select_cols} FROM {schema}.{table} WHERE ({key_cols}) > ({key_values}) ORDER BY {order_cols} LIMIT %s").format(
            select_cols=select_cols, schema=sql.Identifier(source_schema), table=sql.Identifier(source_table), key_cols=key_cols_sql, key_values=key_placeholders, order_cols=order_sql)
        params = list(last_key_values) + [batch_size]
    with source_conn.cursor() as cur:
        cur.execute(query, params)
        return cur.fetchall()



def fetch_full_insert_only_batch(source_conn, source_schema, source_table, columns, batch_size, offset_rows):
    """
    Fetch full-load rows for tables without business key.
    Uses OFFSET because there is no stable key to resume by.
    Recommended only for full-load tables without PK/unique key.
    """
    select_cols = sql.SQL(", ").join(sql.Identifier(c) for c in columns)

    query = sql.SQL("""
        SELECT {select_cols}
        FROM {schema}.{table}
        OFFSET %s
        LIMIT %s
    """).format(
        select_cols=select_cols,
        schema=sql.Identifier(source_schema),
        table=sql.Identifier(source_table)
    )

    with source_conn.cursor() as cur:
        cur.execute(query, (offset_rows, batch_size))
        return cur.fetchall()


def get_table_row_count(conn, schema_name, table_name):
    query = sql.SQL("SELECT COUNT(*) FROM {schema}.{table}").format(
        schema=sql.Identifier(schema_name),
        table=sql.Identifier(table_name),
    )
    with conn.cursor() as cur:
        cur.execute(query)
        return int(cur.fetchone()[0])


def count_source_rows_for_run(
    source_conn,
    source_schema,
    source_table,
    load_type,
    incremental_column,
    business_key_columns,
    last_incremental_value,
    last_key_values,
    high_watermark,
    offset_rows,
    incremental_insert_only=False,
):
    if load_type == "full" and not business_key_columns:
        return max(get_table_row_count(source_conn, source_schema, source_table) - offset_rows, 0)
    if load_type == "full":
        if last_key_values is None:
            return get_table_row_count(source_conn, source_schema, source_table)
        key_cols = sql.SQL(", ").join(sql.Identifier(c) for c in business_key_columns)
        placeholders = sql.SQL(", ").join(sql.Placeholder() for _ in business_key_columns)
        query = sql.SQL("SELECT COUNT(*) FROM {schema}.{table} WHERE ({keys}) > ({values})").format(
            schema=sql.Identifier(source_schema),
            table=sql.Identifier(source_table),
            keys=key_cols,
            values=placeholders,
        )
        params = list(last_key_values)
    elif incremental_insert_only:
        query = sql.SQL("""
            SELECT COUNT(*) FROM {schema}.{table}
            WHERE {incremental} > %s AND {incremental} <= %s
        """).format(
            schema=sql.Identifier(source_schema),
            table=sql.Identifier(source_table),
            incremental=sql.Identifier(incremental_column),
        )
        params = [last_incremental_value, high_watermark]
    elif last_incremental_value is None:
        query = sql.SQL("""
            SELECT COUNT(*) FROM {schema}.{table}
            WHERE {incremental} IS NOT NULL AND {incremental} <= %s
        """).format(
            schema=sql.Identifier(source_schema),
            table=sql.Identifier(source_table),
            incremental=sql.Identifier(incremental_column),
        )
        params = [high_watermark]
    else:
        key_cols = sql.SQL(", ").join(sql.Identifier(c) for c in business_key_columns)
        placeholders = sql.SQL(", ").join(sql.Placeholder() for _ in business_key_columns)
        query = sql.SQL("""
            SELECT COUNT(*) FROM {schema}.{table}
            WHERE {incremental} IS NOT NULL AND {incremental} <= %s
              AND ({incremental} > %s OR ({incremental} = %s AND ({keys}) > ({values})))
        """).format(
            schema=sql.Identifier(source_schema),
            table=sql.Identifier(source_table),
            incremental=sql.Identifier(incremental_column),
            keys=key_cols,
            values=placeholders,
        )
        params = [high_watermark, last_incremental_value, last_incremental_value] + list(last_key_values)
    with source_conn.cursor() as cur:
        cur.execute(query, params)
        return int(cur.fetchone()[0])


def new_reconciliation_stats(target_table, load_type):
    return {
        "table": target_table,
        "load_type": load_type,
        "source_rows": 0,
        "source_total": 0,
        "processed": 0,
        "inserted": 0,
        "updated": 0,
        "rejected": 0,
        "conflict_skipped": 0,
        "target_total": 0,
        "target_difference": 0,
        "status": "SUCCESS",
    }


def finalize_reconciliation_stats(stats, source_conn, target_conn, cfg, table_cfg):
    stats["source_total"] = get_table_row_count(
        source_conn, cfg["source"]["schema"], table_cfg["source_table"]
    )
    stats["target_total"] = get_table_row_count(
        target_conn, cfg["target"]["schema"], table_cfg["target_table"]
    )
    stats["target_difference"] = stats["target_total"] - stats["source_total"]
    accounted = (
        stats["inserted"] + stats["updated"] + stats["rejected"] + stats["conflict_skipped"]
    )
    stats["unaccounted"] = stats["processed"] - accounted
    stats["window_difference"] = stats["source_rows"] - stats["processed"]
    return stats

def truncate_temp(target_conn, temp_table):
    with target_conn.cursor() as cur: cur.execute(sql.SQL("TRUNCATE TABLE {temp_table}").format(temp_table=sql.Identifier(temp_table)))


def copy_to_temp(target_conn, temp_table, columns, rows):
    buffer = io.StringIO(); writer = csv.writer(buffer)
    for row in rows:
        writer.writerow(["\\N" if value is None else value for value in row])
    buffer.seek(0)
    col_sql = ", ".join([f'"{c}"' for c in columns])
    copy_sql = f"COPY \"{temp_table}\" ({col_sql}) FROM STDIN WITH CSV NULL '\\N'"
    with target_conn.cursor() as cur:
        cur.copy_expert(copy_sql, buffer)

def merge_from_temp_business_key(target_conn, target_schema, target_table, temp_table, columns, business_key_columns, incremental_column, load_type):
    update_columns = [c for c in columns if c not in business_key_columns]
    join_condition = sql.SQL(" AND ").join(sql.SQL("tgt.{col} = src.{col}").format(col=sql.Identifier(c)) for c in business_key_columns)
    insert_cols = sql.SQL(", ").join(sql.Identifier(c) for c in columns)
    incremental_filter = sql.SQL("AND src.{inc} >= tgt.{inc}").format(inc=sql.Identifier(incremental_column)) if load_type == "incremental" and incremental_column else sql.SQL("")
    if update_columns:
        update_set_sql = sql.SQL(", ").join(sql.SQL("{col} = src.{col}").format(col=sql.Identifier(c)) for c in update_columns)
        update_sql = sql.SQL("UPDATE {schema}.{target_table} tgt SET {update_set} FROM {temp_table} src WHERE {join_condition} {incremental_filter};").format(
            schema=sql.Identifier(target_schema), target_table=sql.Identifier(target_table), temp_table=sql.Identifier(temp_table), update_set=update_set_sql, join_condition=join_condition, incremental_filter=incremental_filter)
    else:
        update_sql = None
    insert_sql = sql.SQL("""
        INSERT INTO {schema}.{target_table} ({insert_cols})
        SELECT {insert_cols}
        FROM {temp_table} src
        WHERE NOT EXISTS (
            SELECT 1
            FROM {schema}.{target_table} tgt
            WHERE {join_condition}
        )
        ON CONFLICT DO NOTHING;
    """).format(
        schema=sql.Identifier(target_schema),
        target_table=sql.Identifier(target_table),
        temp_table=sql.Identifier(temp_table),
        insert_cols=insert_cols,
        join_condition=join_condition
    )
    with target_conn.cursor() as cur:
        updated_rows = 0
        if update_sql is not None:
            cur.execute(update_sql)
            updated_rows = cur.rowcount
        cur.execute(insert_sql)
        inserted_rows = cur.rowcount
    return updated_rows, inserted_rows



def insert_from_temp_only(target_conn, target_schema, target_table, temp_table, columns):
    """
    Full-load insert-only mode for tables without PK/unique key/business key.
    This is used only when load_type=full and no business key can be detected.
    Recommended to truncate target before running this mode to avoid duplicate rows.
    """
    insert_cols = sql.SQL(", ").join(sql.Identifier(c) for c in columns)

    insert_sql = sql.SQL("""
        INSERT INTO {schema}.{target_table} ({insert_cols})
        SELECT {insert_cols}
        FROM {temp_table}
        ON CONFLICT DO NOTHING;
    """).format(
        schema=sql.Identifier(target_schema),
        target_table=sql.Identifier(target_table),
        temp_table=sql.Identifier(temp_table),
        insert_cols=insert_cols
    )

    with target_conn.cursor() as cur:
        cur.execute(insert_sql)
        inserted_rows = cur.rowcount
    return 0, inserted_rows


def insert_from_temp_allow_duplicates(target_conn, target_schema, target_table, temp_table, columns):
    insert_cols = sql.SQL(", ").join(sql.Identifier(c) for c in columns)
    insert_sql = sql.SQL("""
        INSERT INTO {schema}.{target_table} ({insert_cols})
        SELECT {insert_cols}
        FROM {temp_table};
    """).format(
        schema=sql.Identifier(target_schema),
        target_table=sql.Identifier(target_table),
        temp_table=sql.Identifier(temp_table),
        insert_cols=insert_cols,
    )
    with target_conn.cursor() as cur:
        cur.execute(insert_sql)
        return 0, cur.rowcount


def is_row_level_database_error(error):
    sqlstate = getattr(error, "pgcode", None)
    return bool(sqlstate and sqlstate[:2] in ("22", "23"))


def row_error_record(row, columns, business_key_columns, error_message):
    row_dict = dict(zip(columns, row))
    business_key = "|".join(str(row_dict.get(column)) for column in business_key_columns)
    return {
        "business_key": business_key,
        "error_message": error_message,
        "row_data": row_dict,
    }


def load_rows_with_isolation(
    target_conn,
    target_schema,
    target_table,
    temp_table,
    columns,
    business_key_columns,
    incremental_column,
    load_type,
    rows,
    depth=0,
    incremental_insert_only=False,
):
    if not rows:
        return {"inserted": 0, "updated": 0, "conflict_skipped": 0}, [], []

    savepoint = sql.Identifier(f"migration_chunk_{depth}")
    with target_conn.cursor() as cur:
        cur.execute(sql.SQL("SAVEPOINT {}").format(savepoint))
    try:
        truncate_temp(target_conn, temp_table)
        copy_to_temp(target_conn, temp_table, columns, rows)
        if incremental_insert_only:
            updated_rows, inserted_rows = insert_from_temp_allow_duplicates(
                target_conn, target_schema, target_table, temp_table, columns
            )
        elif load_type == "full" and not business_key_columns:
            updated_rows, inserted_rows = insert_from_temp_only(
                target_conn, target_schema, target_table, temp_table, columns
            )
        else:
            updated_rows, inserted_rows = merge_from_temp_business_key(
                target_conn,
                target_schema,
                target_table,
                temp_table,
                columns,
                business_key_columns,
                incremental_column,
                load_type,
            )
        conflict_rows = len(rows) - updated_rows - inserted_rows
        if conflict_rows < 0:
            raise RuntimeError(
                f"[{target_table}] Database affected more rows than supplied; verify business-key uniqueness"
            )
        if conflict_rows and len(rows) > 1:
            raise ConflictRowsDetected()
        with target_conn.cursor() as cur:
            cur.execute(sql.SQL("RELEASE SAVEPOINT {}").format(savepoint))
        conflicts = []
        if conflict_rows:
            conflicts.append(row_error_record(
                rows[0],
                columns,
                business_key_columns,
                "Row skipped by ON CONFLICT DO NOTHING",
            ))
        return {
            "inserted": inserted_rows,
            "updated": updated_rows,
            "conflict_skipped": conflict_rows,
        }, [], conflicts
    except Exception as error:
        with target_conn.cursor() as cur:
            cur.execute(sql.SQL("ROLLBACK TO SAVEPOINT {}").format(savepoint))
            cur.execute(sql.SQL("RELEASE SAVEPOINT {}").format(savepoint))
        if isinstance(error, ConflictRowsDetected) or is_row_level_database_error(error):
            if len(rows) == 1:
                if isinstance(error, ConflictRowsDetected):
                    raise RuntimeError("Conflict isolation reached an unexpected leaf state")
                bad_row = row_error_record(
                    rows[0],
                    columns,
                    business_key_columns,
                    f"PostgreSQL rejected row: {error}",
                )
                return {"inserted": 0, "updated": 0, "conflict_skipped": 0}, [bad_row], []
            midpoint = len(rows) // 2
            left = load_rows_with_isolation(
                target_conn, target_schema, target_table, temp_table, columns,
                business_key_columns, incremental_column, load_type, rows[:midpoint], depth + 1,
                incremental_insert_only
            )
            right = load_rows_with_isolation(
                target_conn, target_schema, target_table, temp_table, columns,
                business_key_columns, incremental_column, load_type, rows[midpoint:], depth + 1,
                incremental_insert_only
            )
            stats = {
                key: left[0][key] + right[0][key]
                for key in ("inserted", "updated", "conflict_skipped")
            }
            return stats, left[1] + right[1], left[2] + right[2]
        raise

def set_table_triggers(target_conn, schema_name, table_name, action):
    if action not in ("DISABLE", "ENABLE"): raise Exception("Trigger action must be DISABLE or ENABLE")
    query = sql.SQL("ALTER TABLE {schema}.{table} {action} TRIGGER USER").format(schema=sql.Identifier(schema_name), table=sql.Identifier(table_name), action=sql.SQL(action))
    with target_conn.cursor() as cur: cur.execute(query)
    target_conn.commit()


def get_user_trigger_states(target_conn, schema_name, table_name):
    query = """
        SELECT t.tgname, t.tgenabled
        FROM pg_trigger t
        JOIN pg_class c ON c.oid = t.tgrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = %s
          AND c.relname = %s
          AND NOT t.tgisinternal
        ORDER BY t.tgname
    """
    with target_conn.cursor() as cur:
        cur.execute(query, (schema_name, table_name))
        return cur.fetchall()


def record_and_disable_table_triggers(target_conn, state_schema, table_schema, table_name, lock_key):
    trigger_states = get_user_trigger_states(target_conn, table_schema, table_name)
    if not trigger_states:
        return False
    insert_query = sql.SQL("""
        INSERT INTO {state_schema}.migration_trigger_state
            (advisory_lock_key, table_schema, table_name, trigger_name, previous_state)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (advisory_lock_key, table_schema, table_name, trigger_name)
        DO UPDATE SET previous_state = EXCLUDED.previous_state, recorded_at = now()
    """).format(state_schema=sql.Identifier(state_schema))
    with target_conn.cursor() as cur:
        for trigger_name, previous_state in trigger_states:
            cur.execute(insert_query, (lock_key, table_schema, table_name, trigger_name, previous_state))
    target_conn.commit()
    set_table_triggers(target_conn, table_schema, table_name, "DISABLE")
    return True


def restore_recorded_table_triggers(target_conn, state_schema, table_schema, table_name, lock_key):
    select_query = sql.SQL("""
        SELECT trigger_name, previous_state
        FROM {state_schema}.migration_trigger_state
        WHERE advisory_lock_key = %s AND table_schema = %s AND table_name = %s
        ORDER BY trigger_name
    """).format(state_schema=sql.Identifier(state_schema))
    with target_conn.cursor() as cur:
        cur.execute(select_query, (lock_key, table_schema, table_name))
        trigger_states = cur.fetchall()
        for trigger_name, previous_state in trigger_states:
            if previous_state == "D":
                action = sql.SQL("DISABLE TRIGGER")
            elif previous_state == "R":
                action = sql.SQL("ENABLE REPLICA TRIGGER")
            elif previous_state == "A":
                action = sql.SQL("ENABLE ALWAYS TRIGGER")
            else:
                action = sql.SQL("ENABLE TRIGGER")
            cur.execute(
                sql.SQL("ALTER TABLE {schema}.{table} {action} {trigger}").format(
                    schema=sql.Identifier(table_schema),
                    table=sql.Identifier(table_name),
                    action=action,
                    trigger=sql.Identifier(trigger_name),
                )
            )
        cur.execute(
            sql.SQL("""
                DELETE FROM {state_schema}.migration_trigger_state
                WHERE advisory_lock_key = %s AND table_schema = %s AND table_name = %s
            """).format(state_schema=sql.Identifier(state_schema)),
            (lock_key, table_schema, table_name),
        )
    target_conn.commit()
    return len(trigger_states)


def recover_recorded_triggers(target_conn, state_schema, lock_key):
    query = sql.SQL("""
        SELECT DISTINCT table_schema, table_name
        FROM {state_schema}.migration_trigger_state
        WHERE advisory_lock_key = %s
        ORDER BY table_schema, table_name
    """).format(state_schema=sql.Identifier(state_schema))
    with target_conn.cursor() as cur:
        cur.execute(query, (lock_key,))
        tables = cur.fetchall()
    for table_schema, table_name in tables:
        restored = restore_recorded_table_triggers(
            target_conn, state_schema, table_schema, table_name, lock_key
        )
        logging.warning(
            f"[{table_name}] Recovered {restored} trigger state(s) left by an interrupted migration"
        )


def migrate_table(source_conn, target_conn, cfg, table_cfg, checkpoint_data):
    source_schema, target_schema, m = cfg["source"]["schema"], cfg["target"]["schema"], cfg["migration"]
    source_table, target_table, incremental_column = table_cfg["source_table"], table_cfg["target_table"], table_cfg.get("incremental_column")
    load_type = get_load_type(cfg, table_cfg)
    if load_type == "skip": logging.info(f"[{target_table}] Skipped"); return
    if load_type not in ("full", "incremental"): raise Exception(f"[{target_table}] Invalid load_type: {load_type}")
    incremental_insert_only = is_incremental_insert_only_duplicate_mode(table_cfg, load_type)
    stats = new_reconciliation_stats(target_table, load_type)
    table_key, temp_table = f"{source_schema}.{source_table}_to_{target_schema}.{target_table}", f"tmp_migration_{target_table}"
    source_columns, target_columns, source_meta, target_meta, business_key_columns = prepare_metadata(source_conn, target_conn, cfg, table_cfg, load_type)
    if load_type == "incremental":
        min_value, max_value = get_min_max_column(source_conn, source_schema, source_table, incremental_column)
        if min_value is None or max_value is None:
            logging.info(f"[{target_table}] No data found")
            stats["status"] = "NO_DATA"
            return finalize_reconciliation_stats(stats, source_conn, target_conn, cfg, table_cfg)
    else:
        min_value = max_value = None
    table_checkpoint = checkpoint_data.get(table_key)

    if incremental_insert_only and table_checkpoint and "last_incremental_value" in table_checkpoint:
        last_incremental_value = parse_value(table_checkpoint.get("last_incremental_value"))
        last_key_values = []
        high_watermark = max_value
        total_rows = 0
    elif table_checkpoint and checkpoint_matches_business_key(table_checkpoint, business_key_columns):
        status = table_checkpoint.get("status")

        if load_type == "full" and status == "COMPLETED":
            logging.info(f"[{target_table}] Full load already completed. Skipping.")
            stats["status"] = "ALREADY_COMPLETED"
            return finalize_reconciliation_stats(stats, source_conn, target_conn, cfg, table_cfg)

        last_key_values = convert_key_values(
            table_checkpoint["last_key_values"],
            target_meta,
            business_key_columns
        )

        last_incremental_value = parse_value(
            table_checkpoint.get("last_incremental_value")
        )

        high_watermark = parse_value(
            table_checkpoint.get("high_watermark")
        )

        total_rows = int(table_checkpoint.get("total_rows") or 0)

        if load_type == "incremental":
            high_watermark = max_value

    else:
        if table_checkpoint:
            logging.warning(
                f"[{target_table}] Ignoring old checkpoint because business key changed "
                f"or last_key_values count mismatch. Old checkpoint will be replaced."
            )

        last_key_values = None if business_key_columns else []
        total_rows = 0

        if load_type == "incremental":
            if incremental_insert_only:
                configured_initial = table_cfg.get("initial_incremental_value")
                last_incremental_value = (
                    parse_value(configured_initial)
                    if configured_initial is not None
                    else get_initial_incremental_value(min_value)
                )
            else:
                last_incremental_value = None
            high_watermark = max_value
        else:
            last_incremental_value = None
            high_watermark = None

        logging.info(f"[{target_table}] Start load_type={load_type}, last_incremental={last_incremental_value}, high_watermark={high_watermark}")
    stats["source_rows"] = count_source_rows_for_run(
        source_conn,
        source_schema,
        source_table,
        load_type,
        incremental_column,
        business_key_columns,
        last_incremental_value,
        last_key_values,
        high_watermark,
        total_rows,
        incremental_insert_only,
    )
    triggers_disabled = False
    lock_key = int(m.get("advisory_lock_key", 987654321))
    try:
        disable_triggers = table_cfg.get(
            "disable_triggers_during_load",
            cfg["migration"].get("disable_triggers_globally", False)
        )

        if disable_triggers:
            triggers_disabled = record_and_disable_table_triggers(
                target_conn, target_schema, target_schema, target_table, lock_key
            )
            if triggers_disabled:
                logging.info(f"[{target_table}] User triggers disabled and recovery state recorded")
        create_temp_table(target_conn, target_schema, target_table, temp_table, target_columns)
        while True:
            retry_count = 0
            while retry_count < m["max_retries"]:
                try:
                    if load_type == "full" and not business_key_columns:
                        rows = fetch_full_insert_only_batch(
                            source_conn,
                            source_schema,
                            source_table,
                            source_columns,
                            m["batch_size"],
                            total_rows
                        )
                    elif load_type == "full":
                        rows = fetch_full_batch(
                            source_conn,
                            source_schema,
                            source_table,
                            source_columns,
                            business_key_columns,
                            last_key_values,
                            m["batch_size"]
                        )
                    elif incremental_insert_only:
                        rows = fetch_incremental_insert_only_batch(
                            source_conn,
                            source_schema,
                            source_table,
                            source_columns,
                            incremental_column,
                            last_incremental_value,
                            high_watermark,
                            m["batch_size"],
                        )
                    else:
                        rows = fetch_incremental_batch(
                            source_conn,
                            source_schema,
                            source_table,
                            source_columns,
                            incremental_column,
                            business_key_columns,
                            last_incremental_value,
                            last_key_values,
                            high_watermark,
                            m["batch_size"]
                        )
                    if not rows:
                        final_status = "WAITING_FOR_NEXT_RUN" if load_type == "incremental" else "COMPLETED"
                        if incremental_insert_only:
                            checkpoint_data[table_key] = {
                                "last_incremental_value": str(last_incremental_value)
                            }
                        else:
                            checkpoint_data[table_key] = {
                                "last_incremental_value": str(last_incremental_value) if last_incremental_value is not None else None,
                                "last_key_values": serialize_key_values(last_key_values),
                                "high_watermark": str(high_watermark) if high_watermark is not None else None,
                                "total_rows": total_rows,
                                "status": final_status,
                                "load_type": load_type,
                                "business_key_columns": business_key_columns,
                            }
                        write_checkpoint(m["checkpoint_file"], checkpoint_data)
                        stats["status"] = final_status
                        logging.info(f"[{target_table}] No rows found. Status={final_status}")
                        return finalize_reconciliation_stats(stats, source_conn, target_conn, cfg, table_cfg)
                    ensure_partitions_for_rows(target_conn, cfg, table_cfg, source_columns, rows)
                    good_rows, bad_rows = validate_rows(rows, source_columns, target_columns, target_meta, business_key_columns, cfg, table_cfg)
                    batch_stats = {"inserted": 0, "updated": 0, "conflict_skipped": 0}
                    database_bad_rows = []
                    conflict_rows = []
                    if good_rows:
                        batch_stats, database_bad_rows, conflict_rows = load_rows_with_isolation(
                            target_conn,
                            target_schema,
                            target_table,
                            temp_table,
                            target_columns,
                            business_key_columns,
                            incremental_column,
                            load_type,
                            good_rows,
                            incremental_insert_only=incremental_insert_only,
                        )
                        target_conn.commit()
                    rows_to_log = bad_rows + database_bad_rows + conflict_rows
                    if rows_to_log:
                        for bad_row in rows_to_log:
                            logging.error(
                                f"[{target_table}] Bad row: key={bad_row['business_key']}, "
                                f"error={bad_row['error_message']}"
                            )
                        log_bad_rows(
                            target_conn,
                            target_schema,
                            m["error_table"],
                            target_table,
                            rows_to_log,
                            fail_on_error=m.get("fail_on_error_log_failure", False),
                        )
                        logging.warning(f"[{target_table}] Rows logged: {len(rows_to_log)}")

                    last_row = rows[-1]
                    last_row_dict = dict(zip(source_columns, last_row))
                    next_key_values = [last_row_dict[c] for c in business_key_columns] if business_key_columns else []
                    next_incremental_value = (
                        last_row_dict[incremental_column]
                        if load_type == "incremental"
                        else last_incremental_value
                    )
                    next_total_rows = total_rows + len(rows)
                    if incremental_insert_only:
                        checkpoint_data[table_key] = {
                            "last_incremental_value": str(next_incremental_value)
                        }
                    else:
                        checkpoint_data[table_key] = {
                            "last_incremental_value": str(next_incremental_value) if next_incremental_value is not None else None,
                            "last_key_values": serialize_key_values(next_key_values),
                            "high_watermark": str(high_watermark) if high_watermark is not None else None,
                            "total_rows": next_total_rows,
                            "status": "RUNNING",
                            "load_type": load_type,
                            "business_key_columns": business_key_columns,
                        }
                    write_checkpoint(m["checkpoint_file"], checkpoint_data)
                    last_key_values = next_key_values
                    last_incremental_value = next_incremental_value
                    total_rows = next_total_rows
                    stats["processed"] += len(rows)
                    stats["inserted"] += batch_stats["inserted"]
                    stats["updated"] += batch_stats["updated"]
                    stats["rejected"] += len(bad_rows) + len(database_bad_rows)
                    stats["conflict_skipped"] += batch_stats["conflict_skipped"]
                    logging.info(
                        f"[{target_table}] Batch={len(rows)}, Inserted={batch_stats['inserted']}, "
                        f"Updated={batch_stats['updated']}, Rejected={len(bad_rows) + len(database_bad_rows)}, "
                        f"ConflictSkipped={batch_stats['conflict_skipped']}, Total={total_rows}"
                    )
                    break
                except Exception as e:
                    target_conn.rollback(); retry_count += 1
                    logging.exception(f"[{target_table}] Retry {retry_count}/{m['max_retries']} failed: {e}")
                    time.sleep(10)
                    if retry_count == m["max_retries"]:
                        if incremental_insert_only:
                            checkpoint_data[table_key] = {
                                "last_incremental_value": str(last_incremental_value)
                            }
                        else:
                            checkpoint_data[table_key] = {"last_incremental_value": str(last_incremental_value) if last_incremental_value is not None else None, "last_key_values": serialize_key_values(last_key_values), "high_watermark": str(high_watermark) if high_watermark is not None else None, "total_rows": total_rows, "status": "FAILED", "load_type": load_type, "business_key_columns": business_key_columns, "error": str(e)}
                        write_checkpoint(m["checkpoint_file"], checkpoint_data); raise e
            time.sleep(m["sleep_seconds"])
    finally:
        if triggers_disabled:
            try:
                restored = restore_recorded_table_triggers(
                    target_conn, target_schema, target_schema, target_table, lock_key
                )
                logging.info(f"[{target_table}] Restored {restored} user trigger state(s)")
            except Exception as e:
                logging.error(f"[{target_table}] Failed to restore triggers: {e}"); raise e



def acquire_advisory_lock(conn, lock_key):
    """
    Prevents two scheduler jobs from running the same migration at the same time.
    Returns True if lock acquired, False if another job already holds it.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT pg_try_advisory_lock(%s)", (lock_key,))
        return bool(cur.fetchone()[0])


def release_advisory_lock(conn, lock_key):
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_unlock(%s)", (lock_key,))
        conn.commit()
    except Exception as e:
        logging.error(f"Failed to release advisory lock {lock_key}: {e}")


def check_disabled_triggers(conn, schema_name):
    """
    Safety check after migration. If any USER triggers remain disabled,
    log them clearly so they can be enabled manually.
    """
    query = """
        SELECT n.nspname, c.relname, t.tgname
        FROM pg_trigger t
        JOIN pg_class c ON c.oid = t.tgrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = %s
          AND NOT t.tgisinternal
          AND t.tgenabled = 'D'
        ORDER BY c.relname, t.tgname
    """
    with conn.cursor() as cur:
        cur.execute(query, (schema_name,))
        rows = cur.fetchall()

    if rows:
        for schema_name, table_name, trigger_name in rows:
            logging.error(
                f"DISABLED TRIGGER FOUND AFTER MIGRATION: "
                f"{schema_name}.{table_name}.{trigger_name}"
            )
    else:
        logging.info("Trigger safety check passed. No disabled user triggers found.")
    return len(rows)


def get_bad_row_count(conn, schema_name, error_table):
    query = sql.SQL("SELECT COUNT(*) FROM {schema}.{error_table}").format(
        schema=sql.Identifier(schema_name),
        error_table=sql.Identifier(error_table)
    )
    with conn.cursor() as cur:
        cur.execute(query)
        return int(cur.fetchone()[0])


def calculate_next_sequence_value(table_value, current_value, is_called, increment, start_value):
    table_next = table_value + increment
    if current_value is None:
        current_next = start_value
    else:
        current_next = current_value + increment if is_called else current_value
    return max(table_next, current_next) if increment > 0 else min(table_next, current_next)


def calculate_sequence_setval(next_value, increment, minimum_value, maximum_value):
    previous_value = next_value - increment
    if minimum_value <= previous_value <= maximum_value:
        return previous_value, True
    return next_value, False


def reset_table_sequences(conn, schema_name, table_name):
    """
    Resets serial/bigserial/identity-backed sequences after full load.
    Safe to run even if table has no sequence-backed columns.
    """
    query = """
        SELECT
            c.column_name,
            pg_get_serial_sequence(format('%%I.%%I', c.table_schema, c.table_name), c.column_name)
        FROM information_schema.columns c
        WHERE c.table_schema = %s
          AND c.table_name = %s
          AND pg_get_serial_sequence(format('%%I.%%I', c.table_schema, c.table_name), c.column_name) IS NOT NULL
    """
    with conn.cursor() as cur:
        cur.execute(query, (schema_name, table_name))
        rows = cur.fetchall()

    if not rows:
        logging.info(f"[{table_name}] No sequence-backed columns found")
        return 0

    lock_query = sql.SQL("LOCK TABLE {schema}.{table} IN SHARE ROW EXCLUSIVE MODE").format(
        schema=sql.Identifier(schema_name),
        table=sql.Identifier(table_name),
    )
    with conn.cursor() as cur:
        cur.execute(lock_query)

    for column_name, seq_name in rows:
        metadata_query = """
            SELECT sequence_metadata.seqstart,
                   sequence_metadata.seqincrement,
                   sequence_metadata.seqmin,
                   sequence_metadata.seqmax,
                   sequence_schema.nspname,
                   sequence_class.relname
            FROM pg_sequence sequence_metadata
            JOIN pg_class sequence_class ON sequence_class.oid = sequence_metadata.seqrelid
            JOIN pg_namespace sequence_schema ON sequence_schema.oid = sequence_class.relnamespace
            WHERE sequence_metadata.seqrelid = %s::regclass
        """
        with conn.cursor() as cur:
            cur.execute(metadata_query, (seq_name,))
            metadata = cur.fetchone()
        if not metadata:
            raise RuntimeError(f"[{table_name}] Could not read metadata for sequence {seq_name}")
        seq_start, seq_increment, seq_min, seq_max, seq_schema, seq_table = metadata

        sequence_state_query = sql.SQL("SELECT last_value, is_called FROM {schema}.{sequence}").format(
            schema=sql.Identifier(seq_schema),
            sequence=sql.Identifier(seq_table),
        )
        with conn.cursor() as cur:
            cur.execute(sequence_state_query)
            current_value, is_called = cur.fetchone()

        aggregate = sql.SQL("MAX") if seq_increment > 0 else sql.SQL("MIN")
        value_query = sql.SQL("SELECT {aggregate}({col}) FROM {schema}.{table}").format(
            aggregate=aggregate,
            col=sql.Identifier(column_name),
            schema=sql.Identifier(schema_name),
            table=sql.Identifier(table_name),
        )
        with conn.cursor() as cur:
            cur.execute(value_query)
            table_value = cur.fetchone()[0]

        if table_value is None:
            logging.info(f"[{table_name}] Sequence {seq_name} unchanged because the table is empty")
            continue
        desired_value = calculate_next_sequence_value(
            table_value,
            current_value,
            is_called,
            seq_increment,
            seq_start,
        )
        stored_value, is_called_after_reset = calculate_sequence_setval(
            desired_value,
            seq_increment,
            seq_min,
            seq_max,
        )

        with conn.cursor() as cur:
            cur.execute(
                "SELECT setval(%s::regclass, %s, %s)",
                (seq_name, stored_value, is_called_after_reset),
            )

        logging.info(
            f"[{table_name}] Sequence synchronized for {column_name}: "
            f"{seq_name}, stored value={stored_value}, next value={desired_value}, "
            f"is_called={is_called_after_reset}"
        )

    conn.commit()
    return len(rows)


def should_reset_sequences(cfg):
    migration_cfg = cfg["migration"]
    return migration_cfg.get(
        "reset_sequences",
        migration_cfg.get("reset_sequences_after_full_load", True)
    )


def parse_args():
    parser = argparse.ArgumentParser(description="PostgreSQL smart incremental migration sync")
    parser.add_argument("--config", default="config_incremental_load.yaml", help="Path to YAML config file")
    return parser.parse_args()


def main():
    args = parse_args()
    cfg = load_config(args.config)

    source_conn = None
    target_conn = None
    lock_acquired = False

    success_tables = []
    failed_tables = []
    skipped_tables = []
    reconciliation_results = []
    bad_row_count = 0
    disabled_trigger_count = 0

    try:
        source_conn = get_connection(cfg["source"], "source_migration_sync")
        target_conn = get_connection(cfg["target"], "target_migration_sync")

        source_conn.autocommit = True
        target_conn.autocommit = False

        timezone = cfg["migration"].get("timezone", "UTC")

        with source_conn.cursor() as cur:
            cur.execute("SET TIME ZONE %s", (timezone,))

        with target_conn.cursor() as cur:
            cur.execute("SET TIME ZONE %s", (timezone,))
            cur.execute("SET lock_timeout = '5s'")
            cur.execute("SET statement_timeout = '0'")
            cur.execute("SET synchronous_commit = on")

        target_conn.commit()

        # Prevent overlapping scheduled runs.
        lock_key = int(cfg["migration"].get("advisory_lock_key", 987654321))
        if cfg["migration"].get("use_advisory_lock", True):
            lock_acquired = acquire_advisory_lock(target_conn, lock_key)
            if not lock_acquired:
                logging.warning(
                    f"Migration already running. Advisory lock {lock_key} not acquired. Exiting."
                )
                return
            logging.info(f"Advisory lock acquired: {lock_key}")

        create_error_table(
            target_conn,
            cfg["target"]["schema"],
            cfg["migration"]["error_table"]
        )
        create_trigger_state_table(target_conn, cfg["target"]["schema"])
        recover_recorded_triggers(target_conn, cfg["target"]["schema"], lock_key)
        bad_row_start = get_bad_row_count(
            target_conn,
            cfg["target"]["schema"],
            cfg["migration"]["error_table"],
        )

        checkpoint_data = read_checkpoint(cfg["migration"]["checkpoint_file"])

        for table_cfg in cfg["migration"]["tables"]:
            table_name = table_cfg.get("target_table")
            load_type = get_load_type(cfg, table_cfg)

            if load_type == "skip":
                logging.info(f"[{table_name}] Skipped due to load_type=skip or enabled=false")
                skipped_tables.append({
                    "table": table_name,
                    "reason": "load_type=skip or enabled=false"
                })
                continue

            try:
                logging.info("=" * 100)
                logging.info(f"[{table_name}] Table migration started")

                reconciliation = migrate_table(
                    source_conn,
                    target_conn,
                    cfg,
                    table_cfg,
                    checkpoint_data
                )
                if reconciliation:
                    reconciliation_results.append(reconciliation)

                # Add a catch-all child only after all expected batch partitions are loaded.
                ensure_default_partition(target_conn, cfg, table_cfg, load_type)

                # Reset sequence-backed columns after successful full or incremental load.
                if should_reset_sequences(cfg):
                    sequence_count = reset_table_sequences(
                        target_conn,
                        cfg["target"]["schema"],
                        table_name
                    )
                    logging.info(f"[{table_name}] Sequence reset check completed; detected={sequence_count}")

                success_tables.append(table_name)
                logging.info(f"[{table_name}] Table migration completed")

            except PreMigrationValidationError as e:
                target_conn.rollback()
                logging.error(f"[{table_name}] Pre-migration validation failed: {e}")
                if cfg["migration"].get("stop_on_table_error", False):
                    raise e
                skipped_tables.append({
                    "table": table_name,
                    "reason": str(e)
                })
                continue

            except Exception as e:
                target_conn.rollback()

                failed_tables.append({
                    "table": table_name,
                    "error": str(e)
                })

                logging.exception(f"[{table_name}] Table migration failed: {e}")

                if cfg["migration"].get("stop_on_table_error", False):
                    raise e

                continue

        bad_row_count = get_bad_row_count(
            target_conn,
            cfg["target"]["schema"],
            cfg["migration"]["error_table"]
        ) - bad_row_start

        if cfg["migration"].get("check_disabled_triggers_after_run", True):
            disabled_trigger_count = check_disabled_triggers(target_conn, cfg["target"]["schema"])

        logging.info("=" * 100)
        logging.info("MIGRATION SUMMARY")
        logging.info(f"Success tables: {len(success_tables)}")
        logging.info(f"Failed tables : {len(failed_tables)}")
        logging.info(f"Skipped tables: {len(skipped_tables)}")
        logging.info(f"Bad rows logged: {bad_row_count}")
        logging.info(f"Disabled user triggers: {disabled_trigger_count}")

        for item in failed_tables:
            logging.error(f"FAILED TABLE: {item['table']} | ERROR: {item['error']}")

        for item in skipped_tables:
            logging.warning(f"SKIPPED TABLE: {item['table']} | REASON: {item['reason']}")

        logging.info("PER-TABLE RECONCILIATION")
        for item in reconciliation_results:
            log_method = logging.error if item["unaccounted"] or item["window_difference"] else logging.info
            log_method(
                f"RECONCILIATION: {item['table']} | SourceWindow={item['source_rows']} | "
                f"Processed={item['processed']} | Inserted={item['inserted']} | "
                f"Updated={item['updated']} | Rejected={item['rejected']} | "
                f"ConflictSkipped={item['conflict_skipped']} | Unaccounted={item['unaccounted']} | "
                f"WindowDifference={item['window_difference']} | "
                f"SourceTotal={item['source_total']} | TargetTotal={item['target_total']} | "
                f"TargetDifference={item['target_difference']} | Status={item['status']}"
            )

        logging.info("Migration sync completed")

    except Exception as e:
        logging.error(f"Migration stopped: {e}")

    finally:
        if target_conn and lock_acquired:
            release_advisory_lock(
                target_conn,
                int(cfg["migration"].get("advisory_lock_key", 987654321))
            )

        if source_conn:
            source_conn.close()

        if target_conn:
            target_conn.close()


if __name__ == "__main__":
    main()
