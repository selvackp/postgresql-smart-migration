SELECT
    schemaname AS schema_name,
    relname AS table_name,
    n_live_tup AS estimated_live_rows
FROM pg_stat_user_tables
ORDER BY schemaname, relname;
