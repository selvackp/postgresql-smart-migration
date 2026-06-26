WITH sequence_columns AS (
    SELECT
        c.table_schema,
        c.table_name,
        c.column_name,
        pg_get_serial_sequence(format('%I.%I', c.table_schema, c.table_name), c.column_name) AS sequence_name
    FROM information_schema.columns c
    WHERE c.table_schema NOT IN ('pg_catalog', 'information_schema')
      AND pg_get_serial_sequence(format('%I.%I', c.table_schema, c.table_name), c.column_name) IS NOT NULL
)
SELECT
    table_schema,
    table_name,
    column_name,
    sequence_name
FROM sequence_columns
ORDER BY table_schema, table_name, column_name;
