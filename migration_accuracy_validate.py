import argparse
import json
import logging
from datetime import datetime
from uuid import uuid4

import psycopg2
import yaml
from psycopg2 import sql
from psycopg2.extras import Json


logging.basicConfig(
    filename="migration_accuracy_validate.log",
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger("").addHandler(console)


def load_config(config_file):
    with open(config_file, "r") as f:
        cfg = yaml.safe_load(f)
    for section in ("source", "target", "migration"):
        if section not in cfg:
            raise ValueError(f"Missing required config section: {section}")
    if not cfg["migration"].get("tables"):
        raise ValueError("Config must include migration.tables")
    return cfg


def get_connection(db_cfg, app_name):
    return psycopg2.connect(
        host=db_cfg["host"],
        port=db_cfg.get("port", 5432),
        dbname=db_cfg["database"],
        user=db_cfg["user"],
        password=db_cfg["password"],
        connect_timeout=10,
        application_name=app_name,
    )


def get_table_columns(conn, schema_name, table_name):
    query = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
    """
    with conn.cursor() as cur:
        cur.execute(query, (schema_name, table_name))
        return [r[0] for r in cur.fetchall()]


def get_primary_key_columns(conn, schema_name, table_name):
    query = """
        SELECT kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name
         AND tc.table_schema = kcu.table_schema
         AND tc.table_name = kcu.table_name
        WHERE tc.constraint_type = 'PRIMARY KEY'
          AND tc.table_schema = %s
          AND tc.table_name = %s
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


def detect_key_columns(source_conn, target_conn, cfg, table_cfg):
    configured = table_cfg.get("business_key_columns")
    if configured is not None:
        return list(configured or [])

    source_schema = cfg["source"]["schema"]
    target_schema = cfg["target"]["schema"]
    source_table = table_cfg["source_table"]
    target_table = table_cfg["target_table"]

    for conn, schema_name, table_name in (
        (target_conn, target_schema, target_table),
        (source_conn, source_schema, source_table),
    ):
        columns = get_primary_key_columns(conn, schema_name, table_name)
        if columns:
            return columns

    for conn, schema_name, table_name in (
        (target_conn, target_schema, target_table),
        (source_conn, source_schema, source_table),
    ):
        columns = get_unique_key_columns(conn, schema_name, table_name)
        if columns:
            return columns

    return []


def create_validation_table(conn, schema_name, table_name):
    query = sql.SQL("""
        CREATE TABLE IF NOT EXISTS {schema}.{table} (
            id bigserial PRIMARY KEY,
            run_id text NOT NULL,
            validation_time timestamptz NOT NULL DEFAULT now(),
            source_table text NOT NULL,
            target_table text NOT NULL,
            load_type text,
            status text NOT NULL,
            source_total bigint,
            source_scope_count bigint,
            target_total bigint,
            target_difference bigint,
            source_incremental_null_count bigint,
            source_min_incremental text,
            source_max_incremental text,
            target_min_incremental text,
            target_max_incremental text,
            key_columns jsonb,
            compared_columns jsonb,
            source_key_hash text,
            target_key_hash text,
            source_row_hash text,
            target_row_hash text,
            details jsonb
        )
    """).format(schema=sql.Identifier(schema_name), table=sql.Identifier(table_name))
    with conn.cursor() as cur:
        cur.execute(query)
    conn.commit()


def create_bucket_validation_table(conn, schema_name, table_name):
    query = sql.SQL("""
        CREATE TABLE IF NOT EXISTS {schema}.{table} (
            id bigserial PRIMARY KEY,
            run_id text NOT NULL,
            validation_time timestamptz NOT NULL DEFAULT now(),
            source_table text NOT NULL,
            target_table text NOT NULL,
            load_type text,
            bucket_column text NOT NULL,
            bucket_granularity text NOT NULL,
            bucket_value text NOT NULL,
            status text NOT NULL,
            source_count bigint,
            target_count bigint,
            count_difference bigint,
            key_columns jsonb,
            compared_columns jsonb,
            source_key_hash text,
            target_key_hash text,
            source_row_hash text,
            target_row_hash text,
            details jsonb
        )
    """).format(schema=sql.Identifier(schema_name), table=sql.Identifier(table_name))
    with conn.cursor() as cur:
        cur.execute(query)
    conn.commit()


def text_expr(columns):
    parts = [
        sql.SQL("COALESCE({col}::text, chr(30))").format(col=sql.Identifier(column))
        for column in columns
    ]
    if not parts:
        return None
    return sql.SQL("concat_ws(chr(31), {parts})").format(parts=sql.SQL(", ").join(parts))


def hash_sum_sql(expr):
    return sql.SQL("""
        COALESCE(SUM((('x' || substr(md5({expr}), 1, 16))::bit(64)::bigint)::numeric), 0)::text
        || ':'
        || COALESCE(SUM((('x' || substr(md5({expr}), 17, 16))::bit(64)::bigint)::numeric), 0)::text
    """).format(expr=expr)


def aggregate_table(
    conn,
    schema_name,
    table_name,
    compared_columns,
    key_columns,
    incremental_column=None,
    source_incremental_scope=False,
):
    where_clause = sql.SQL("")
    if source_incremental_scope and incremental_column:
        where_clause = sql.SQL("WHERE {inc} IS NOT NULL").format(
            inc=sql.Identifier(incremental_column)
        )

    row_expr = text_expr(compared_columns)
    row_hash = hash_sum_sql(row_expr) if row_expr is not None else sql.SQL("NULL")

    key_expr = text_expr(key_columns)
    key_hash = hash_sum_sql(key_expr) if key_expr is not None else sql.SQL("NULL")

    if incremental_column:
        min_inc = sql.SQL("MIN({inc})::text").format(inc=sql.Identifier(incremental_column))
        max_inc = sql.SQL("MAX({inc})::text").format(inc=sql.Identifier(incremental_column))
        null_inc = sql.SQL("COUNT(*) FILTER (WHERE {inc} IS NULL)").format(
            inc=sql.Identifier(incremental_column)
        )
    else:
        min_inc = sql.SQL("NULL")
        max_inc = sql.SQL("NULL")
        null_inc = sql.SQL("NULL")

    query = sql.SQL("""
        SELECT
            COUNT(*)::bigint,
            {min_inc},
            {max_inc},
            {null_inc},
            {key_hash},
            {row_hash}
        FROM {schema}.{table}
        {where_clause}
    """).format(
        schema=sql.Identifier(schema_name),
        table=sql.Identifier(table_name),
        min_inc=min_inc,
        max_inc=max_inc,
        null_inc=null_inc,
        key_hash=key_hash,
        row_hash=row_hash,
        where_clause=where_clause,
    )
    with conn.cursor() as cur:
        cur.execute(query)
        row = cur.fetchone()
    return {
        "count": row[0],
        "min_incremental": row[1],
        "max_incremental": row[2],
        "incremental_null_count": row[3],
        "key_hash": row[4],
        "row_hash": row[5],
    }


def bucket_expr(column_name, granularity):
    column = sql.Identifier(column_name)
    if granularity == "daily":
        return sql.SQL("date_trunc('day', {column})::date::text").format(column=column)
    if granularity == "monthly":
        return sql.SQL("date_trunc('month', {column})::date::text").format(column=column)
    if granularity == "value":
        return sql.SQL("{column}::text").format(column=column)
    raise ValueError("bucket granularity must be daily, monthly, or value")


def aggregate_table_by_bucket(
    conn,
    schema_name,
    table_name,
    compared_columns,
    key_columns,
    bucket_column,
    granularity,
    incremental_column=None,
    source_incremental_scope=False,
):
    where_parts = [sql.SQL("{bucket} IS NOT NULL").format(bucket=sql.Identifier(bucket_column))]
    if source_incremental_scope and incremental_column:
        where_parts.append(sql.SQL("{inc} IS NOT NULL").format(inc=sql.Identifier(incremental_column)))
    where_clause = sql.SQL("WHERE {conditions}").format(
        conditions=sql.SQL(" AND ").join(where_parts)
    )

    bucket_sql = bucket_expr(bucket_column, granularity)
    row_hash = hash_sum_sql(text_expr(compared_columns))
    key_expr = text_expr(key_columns)
    key_hash = hash_sum_sql(key_expr) if key_expr is not None else sql.SQL("NULL")

    query = sql.SQL("""
        SELECT
            {bucket_expr} AS bucket_value,
            COUNT(*)::bigint AS row_count,
            {key_hash} AS key_hash,
            {row_hash} AS row_hash
        FROM {schema}.{table}
        {where_clause}
        GROUP BY 1
        ORDER BY 1
    """).format(
        bucket_expr=bucket_sql,
        schema=sql.Identifier(schema_name),
        table=sql.Identifier(table_name),
        where_clause=where_clause,
        key_hash=key_hash,
        row_hash=row_hash,
    )
    with conn.cursor() as cur:
        cur.execute(query)
        rows = cur.fetchall()
    return {
        row[0]: {
            "count": row[1],
            "key_hash": row[2],
            "row_hash": row[3],
        }
        for row in rows
    }


def insert_bucket_validation_results(conn, schema_name, table_name, results):
    if not results:
        return
    query = sql.SQL("""
        INSERT INTO {schema}.{table} (
            run_id, source_table, target_table, load_type,
            bucket_column, bucket_granularity, bucket_value, status,
            source_count, target_count, count_difference,
            key_columns, compared_columns,
            source_key_hash, target_key_hash,
            source_row_hash, target_row_hash,
            details
        )
        VALUES (
            %(run_id)s, %(source_table)s, %(target_table)s, %(load_type)s,
            %(bucket_column)s, %(bucket_granularity)s, %(bucket_value)s, %(status)s,
            %(source_count)s, %(target_count)s, %(count_difference)s,
            %(key_columns)s, %(compared_columns)s,
            %(source_key_hash)s, %(target_key_hash)s,
            %(source_row_hash)s, %(target_row_hash)s,
            %(details)s
        )
    """).format(schema=sql.Identifier(schema_name), table=sql.Identifier(table_name))
    with conn.cursor() as cur:
        for result in results:
            values = dict(result)
            values["key_columns"] = Json(result["key_columns"])
            values["compared_columns"] = Json(result["compared_columns"])
            values["details"] = Json(result["details"])
            cur.execute(query, values)
    conn.commit()


def insert_validation_result(conn, schema_name, table_name, result):
    query = sql.SQL("""
        INSERT INTO {schema}.{table} (
            run_id, source_table, target_table, load_type, status,
            source_total, source_scope_count, target_total, target_difference,
            source_incremental_null_count,
            source_min_incremental, source_max_incremental,
            target_min_incremental, target_max_incremental,
            key_columns, compared_columns,
            source_key_hash, target_key_hash,
            source_row_hash, target_row_hash,
            details
        )
        VALUES (
            %(run_id)s, %(source_table)s, %(target_table)s, %(load_type)s, %(status)s,
            %(source_total)s, %(source_scope_count)s, %(target_total)s, %(target_difference)s,
            %(source_incremental_null_count)s,
            %(source_min_incremental)s, %(source_max_incremental)s,
            %(target_min_incremental)s, %(target_max_incremental)s,
            %(key_columns)s, %(compared_columns)s,
            %(source_key_hash)s, %(target_key_hash)s,
            %(source_row_hash)s, %(target_row_hash)s,
            %(details)s
        )
    """).format(schema=sql.Identifier(schema_name), table=sql.Identifier(table_name))
    values = dict(result)
    values["key_columns"] = Json(result["key_columns"])
    values["compared_columns"] = Json(result["compared_columns"])
    values["details"] = Json(result["details"])
    with conn.cursor() as cur:
        cur.execute(query, values)
    conn.commit()


def validate_table(source_conn, target_conn, cfg, table_cfg, run_id):
    source_schema = cfg["source"]["schema"]
    target_schema = cfg["target"]["schema"]
    source_table = table_cfg["source_table"]
    target_table = table_cfg["target_table"]
    load_type = table_cfg.get("load_type", cfg["migration"].get("default_load_type", "incremental"))
    incremental_column = table_cfg.get("incremental_column")

    source_columns = get_table_columns(source_conn, source_schema, source_table)
    target_columns = get_table_columns(target_conn, target_schema, target_table)
    common_columns = [column for column in target_columns if column in set(source_columns)]
    key_columns = detect_key_columns(source_conn, target_conn, cfg, table_cfg)
    key_columns = [column for column in key_columns if column in common_columns]

    if not common_columns:
        raise RuntimeError(f"[{target_table}] No common columns found for validation")

    source_total = aggregate_table(
        source_conn,
        source_schema,
        source_table,
        common_columns,
        key_columns,
        incremental_column,
        source_incremental_scope=False,
    )
    source_scope = aggregate_table(
        source_conn,
        source_schema,
        source_table,
        common_columns,
        key_columns,
        incremental_column,
        source_incremental_scope=(load_type == "incremental"),
    )
    target_scope = aggregate_table(
        target_conn,
        target_schema,
        target_table,
        common_columns,
        key_columns,
        incremental_column,
        source_incremental_scope=False,
    )

    target_difference = int(target_scope["count"] or 0) - int(source_total["count"] or 0)
    scoped_difference = int(target_scope["count"] or 0) - int(source_scope["count"] or 0)
    key_match = not key_columns or source_scope["key_hash"] == target_scope["key_hash"]
    row_match = source_scope["row_hash"] == target_scope["row_hash"]
    count_match = scoped_difference == 0

    details = {
        "source_columns": source_columns,
        "target_columns": target_columns,
        "missing_in_target_schema": [c for c in source_columns if c not in set(target_columns)],
        "target_only_columns": [c for c in target_columns if c not in set(source_columns)],
        "source_scope_note": "incremental_column IS NOT NULL" if load_type == "incremental" and incremental_column else "all rows",
        "scoped_difference": scoped_difference,
        "count_match_on_scope": count_match,
        "key_hash_match": key_match,
        "row_hash_match": row_match,
    }

    if count_match and key_match and row_match:
        status = "MATCH"
    elif count_match:
        status = "CONTENT_MISMATCH"
    else:
        status = "COUNT_MISMATCH"

    return {
        "run_id": run_id,
        "source_table": f"{source_schema}.{source_table}",
        "target_table": f"{target_schema}.{target_table}",
        "load_type": load_type,
        "status": status,
        "source_total": source_total["count"],
        "source_scope_count": source_scope["count"],
        "target_total": target_scope["count"],
        "target_difference": target_difference,
        "source_incremental_null_count": source_total["incremental_null_count"],
        "source_min_incremental": source_scope["min_incremental"],
        "source_max_incremental": source_scope["max_incremental"],
        "target_min_incremental": target_scope["min_incremental"],
        "target_max_incremental": target_scope["max_incremental"],
        "key_columns": key_columns,
        "compared_columns": common_columns,
        "source_key_hash": source_scope["key_hash"],
        "target_key_hash": target_scope["key_hash"],
        "source_row_hash": source_scope["row_hash"],
        "target_row_hash": target_scope["row_hash"],
        "details": details,
    }


def validate_table_buckets(
    source_conn,
    target_conn,
    cfg,
    table_cfg,
    run_id,
    bucket_column,
    bucket_granularity,
):
    source_schema = cfg["source"]["schema"]
    target_schema = cfg["target"]["schema"]
    source_table = table_cfg["source_table"]
    target_table = table_cfg["target_table"]
    load_type = table_cfg.get("load_type", cfg["migration"].get("default_load_type", "incremental"))
    incremental_column = table_cfg.get("incremental_column")

    source_columns = get_table_columns(source_conn, source_schema, source_table)
    target_columns = get_table_columns(target_conn, target_schema, target_table)
    common_columns = [column for column in target_columns if column in set(source_columns)]
    key_columns = detect_key_columns(source_conn, target_conn, cfg, table_cfg)
    key_columns = [column for column in key_columns if column in common_columns]

    if bucket_column not in common_columns:
        raise RuntimeError(f"[{target_table}] bucket column not found in both source and target: {bucket_column}")
    if not common_columns:
        raise RuntimeError(f"[{target_table}] No common columns found for bucket validation")

    source_buckets = aggregate_table_by_bucket(
        source_conn,
        source_schema,
        source_table,
        common_columns,
        key_columns,
        bucket_column,
        bucket_granularity,
        incremental_column,
        source_incremental_scope=(load_type == "incremental"),
    )
    target_buckets = aggregate_table_by_bucket(
        target_conn,
        target_schema,
        target_table,
        common_columns,
        key_columns,
        bucket_column,
        bucket_granularity,
        incremental_column,
        source_incremental_scope=False,
    )

    results = []
    for bucket_value in sorted(set(source_buckets) | set(target_buckets)):
        source_bucket = source_buckets.get(bucket_value, {})
        target_bucket = target_buckets.get(bucket_value, {})
        source_count = int(source_bucket.get("count") or 0)
        target_count = int(target_bucket.get("count") or 0)
        count_difference = target_count - source_count
        key_match = not key_columns or source_bucket.get("key_hash") == target_bucket.get("key_hash")
        row_match = source_bucket.get("row_hash") == target_bucket.get("row_hash")

        if count_difference == 0 and key_match and row_match:
            status = "MATCH"
        elif count_difference == 0:
            status = "CONTENT_MISMATCH"
        else:
            status = "COUNT_MISMATCH"

        results.append({
            "run_id": run_id,
            "source_table": f"{source_schema}.{source_table}",
            "target_table": f"{target_schema}.{target_table}",
            "load_type": load_type,
            "bucket_column": bucket_column,
            "bucket_granularity": bucket_granularity,
            "bucket_value": bucket_value,
            "status": status,
            "source_count": source_count,
            "target_count": target_count,
            "count_difference": count_difference,
            "key_columns": key_columns,
            "compared_columns": common_columns,
            "source_key_hash": source_bucket.get("key_hash"),
            "target_key_hash": target_bucket.get("key_hash"),
            "source_row_hash": source_bucket.get("row_hash"),
            "target_row_hash": target_bucket.get("row_hash"),
            "details": {
                "key_hash_match": key_match,
                "row_hash_match": row_match,
                "source_scope_note": "incremental_column IS NOT NULL" if load_type == "incremental" and incremental_column else "all rows",
            },
        })
    return results


def main():
    parser = argparse.ArgumentParser(description="Validate migrated PostgreSQL data accuracy")
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--table", help="Validate only one target table name")
    parser.add_argument("--validation-table", default=None)
    parser.add_argument("--bucket", action="store_true", help="Also write bucket-level validation results")
    parser.add_argument("--bucket-column", default=None, help="Column used for bucket validation; defaults to partition_column then incremental_column")
    parser.add_argument("--bucket-granularity", choices=("daily", "monthly", "value"), default="daily")
    parser.add_argument("--bucket-validation-table", default=None)
    args = parser.parse_args()

    cfg = load_config(args.config)
    target_schema = cfg["target"]["schema"]
    validation_table = (
        args.validation_table
        or cfg["migration"].get("accuracy_table")
        or "migration_accuracy_log"
    )
    bucket_validation_table = (
        args.bucket_validation_table
        or cfg["migration"].get("accuracy_bucket_table")
        or "migration_accuracy_bucket_log"
    )
    run_id = f"{datetime.utcnow().strftime('%Y%m%d%H%M%S')}-{uuid4().hex[:8]}"

    source_conn = get_connection(cfg["source"], "source_migration_accuracy")
    target_conn = get_connection(cfg["target"], "target_migration_accuracy")
    try:
        create_validation_table(target_conn, target_schema, validation_table)
        if args.bucket:
            create_bucket_validation_table(target_conn, target_schema, bucket_validation_table)
        for table_cfg in cfg["migration"]["tables"]:
            if not table_cfg.get("enabled", True):
                continue
            if args.table and table_cfg.get("target_table") != args.table:
                continue
            target_table = table_cfg.get("target_table")
            try:
                logging.info(f"[{target_table}] Accuracy validation started")
                result = validate_table(source_conn, target_conn, cfg, table_cfg, run_id)
                insert_validation_result(target_conn, target_schema, validation_table, result)
                logging.info(
                    f"[{target_table}] Accuracy={result['status']} "
                    f"SourceScope={result['source_scope_count']} Target={result['target_total']} "
                    f"Diff={result['details']['scoped_difference']}"
                )
                if args.bucket:
                    bucket_column = (
                        args.bucket_column
                        or table_cfg.get("partition_column")
                        or table_cfg.get("incremental_column")
                    )
                    if bucket_column:
                        bucket_results = validate_table_buckets(
                            source_conn,
                            target_conn,
                            cfg,
                            table_cfg,
                            run_id,
                            bucket_column,
                            args.bucket_granularity,
                        )
                        insert_bucket_validation_results(
                            target_conn,
                            target_schema,
                            bucket_validation_table,
                            bucket_results,
                        )
                        mismatch_count = sum(1 for item in bucket_results if item["status"] != "MATCH")
                        logging.info(
                            f"[{target_table}] Bucket validation completed: "
                            f"buckets={len(bucket_results)}, mismatches={mismatch_count}"
                        )
                    else:
                        logging.warning(f"[{target_table}] Bucket validation skipped; no bucket column configured")
            except Exception as e:
                logging.exception(f"[{target_table}] Accuracy validation failed: {e}")
                failure = {
                    "run_id": run_id,
                    "source_table": f"{cfg['source']['schema']}.{table_cfg.get('source_table')}",
                    "target_table": f"{target_schema}.{target_table}",
                    "load_type": table_cfg.get("load_type"),
                    "status": "VALIDATION_FAILED",
                    "source_total": None,
                    "source_scope_count": None,
                    "target_total": None,
                    "target_difference": None,
                    "source_incremental_null_count": None,
                    "source_min_incremental": None,
                    "source_max_incremental": None,
                    "target_min_incremental": None,
                    "target_max_incremental": None,
                    "key_columns": [],
                    "compared_columns": [],
                    "source_key_hash": None,
                    "target_key_hash": None,
                    "source_row_hash": None,
                    "target_row_hash": None,
                    "details": {"error": str(e)},
                }
                insert_validation_result(target_conn, target_schema, validation_table, failure)
    finally:
        source_conn.close()
        target_conn.close()

    logging.info(f"Accuracy validation run completed. run_id={run_id}")


if __name__ == "__main__":
    main()
