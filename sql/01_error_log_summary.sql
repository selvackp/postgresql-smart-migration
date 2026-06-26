SELECT
    table_name,
    COUNT(*) AS bad_row_count,
    MIN(error_time) AS first_error_time,
    MAX(error_time) AS last_error_time
FROM public.migration_error_log
GROUP BY table_name
ORDER BY bad_row_count DESC, table_name;
