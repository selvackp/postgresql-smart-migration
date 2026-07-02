import unittest
from unittest import mock
from decimal import Decimal
from datetime import date

import migration_sync as migration


class FakeCursor:
    def __init__(self, connection):
        self.connection = connection

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return False

    def execute(self, query, params=None):
        self.connection.executions.append((query, params))

    def fetchall(self):
        return self.connection.rows


class FakeConnection:
    def __init__(self, rows=None):
        self.rows = rows or []
        self.executions = []
        self.commits = 0

    def cursor(self):
        return FakeCursor(self)

    def commit(self):
        self.commits += 1


class RowDatabaseError(Exception):
    pgcode = "23514"


class MigrationSafetyTests(unittest.TestCase):
    def test_source_key_without_unique_index_is_allowed_when_data_is_unique(self):
        metadata = {"id": {"is_nullable": "NO"}}
        with mock.patch.object(
            migration, "has_matching_unique_index", side_effect=[False, True]
        ), mock.patch.object(
            migration, "source_business_key_has_duplicates", return_value=False
        ):
            migration.validate_business_key_safety(
                object(), object(), "public", "public", "source_table", "target_table",
                metadata, metadata, ["id"]
            )

    def test_full_load_allows_target_key_without_unique_index(self):
        metadata = {"rptid": {"is_nullable": "NO"}}
        with mock.patch.object(
            migration, "has_matching_unique_index", side_effect=[True, False]
        ):
            migration.validate_business_key_safety(
                object(), object(), "public", "public", "reportscheduler", "reportscheduler",
                metadata, metadata, ["rptid"], load_type="full"
            )

    def test_incremental_load_requires_target_unique_index(self):
        metadata = {"rptid": {"is_nullable": "NO"}}
        with mock.patch.object(
            migration, "has_matching_unique_index", side_effect=[True, False]
        ):
            with self.assertRaises(migration.PreMigrationValidationError):
                migration.validate_business_key_safety(
                    object(), object(), "public", "public", "reportscheduler", "reportscheduler",
                    metadata, metadata, ["rptid"], load_type="incremental"
                )

    def test_incremental_load_can_opt_in_without_target_unique_index(self):
        metadata = {"id": {"is_nullable": "NO"}}
        with mock.patch.object(
            migration, "has_matching_unique_index", side_effect=[True, False]
        ), mock.patch.object(
            migration, "source_business_key_has_duplicates", return_value=False
        ):
            migration.validate_business_key_safety(
                object(), object(), "public", "public", "source_table", "target_table",
                metadata, metadata, ["id"], load_type="incremental",
                allow_incremental_without_target_unique_index=True,
            )

    def test_incremental_opt_in_rejects_existing_target_duplicates(self):
        metadata = {"id": {"is_nullable": "NO"}}
        with mock.patch.object(
            migration, "has_matching_unique_index", side_effect=[True, False]
        ), mock.patch.object(
            migration, "source_business_key_has_duplicates", return_value=True
        ):
            with self.assertRaises(migration.PreMigrationValidationError):
                migration.validate_business_key_safety(
                    object(), object(), "public", "public", "source_table", "target_table",
                    metadata, metadata, ["id"], load_type="incremental",
                    allow_incremental_without_target_unique_index=True,
                )

    def test_nullable_source_key_is_rejected_when_data_contains_null(self):
        source_metadata = {"id": {"is_nullable": "YES"}}
        target_metadata = {"id": {"is_nullable": "NO"}}
        with mock.patch.object(migration, "source_business_key_has_null", return_value=True):
            with self.assertRaises(migration.PreMigrationValidationError):
                migration.validate_business_key_safety(
                    object(), object(), "public", "public", "source_table", "target_table",
                    source_metadata, target_metadata, ["id"]
                )

    def test_date_to_timestamp_is_a_compatible_widening(self):
        compatible = migration.is_type_compatible(
            {"data_type": "date", "udt_name": "date"},
            {"data_type": "timestamp without time zone", "udt_name": "timestamp"},
        )
        self.assertEqual(compatible, (True, True))

    def test_double_precision_to_numeric_is_compatible(self):
        compatible = migration.is_type_compatible(
            {"data_type": "double precision", "udt_name": "float8"},
            {"data_type": "numeric", "udt_name": "numeric"},
        )
        self.assertEqual(compatible, (True, True))

    def test_varchar_to_fixed_character_is_compatible(self):
        compatible = migration.is_type_compatible(
            {"data_type": "character varying", "udt_name": "varchar"},
            {"data_type": "character", "udt_name": "bpchar"},
        )
        self.assertEqual(compatible, (True, True))

    def test_sequence_reset_uses_next_value_after_table_maximum(self):
        self.assertEqual(
            migration.calculate_next_sequence_value(100, 1, True, 1, 1),
            101,
        )

    def test_sequence_reset_stores_visible_previous_value(self):
        self.assertEqual(
            migration.calculate_sequence_setval(101, 1, 1, 9223372036854775807),
            (100, True),
        )

    def test_sequence_reset_uses_uncalled_state_at_sequence_boundary(self):
        self.assertEqual(
            migration.calculate_sequence_setval(1, 1, 1, 100),
            (1, False),
        )

    def test_sequence_reset_never_moves_live_sequence_backward(self):
        self.assertEqual(
            migration.calculate_next_sequence_value(100, 150, True, 1, 1),
            151,
        )

    def test_descending_sequence_uses_value_below_table_minimum(self):
        self.assertEqual(
            migration.calculate_next_sequence_value(-100, -1, True, -1, -1),
            -101,
        )

    def test_duplicate_business_key_configuration_is_rejected(self):
        with self.assertRaises(migration.PreMigrationValidationError):
            migration.validate_business_key_safety(
                object(),
                object(),
                "public",
                "public",
                "source_table",
                "target_table",
                {"id": {"is_nullable": "NO"}},
                {"id": {"is_nullable": "NO"}},
                ["id", "id"],
            )

    def test_decimal_business_key_checkpoint_is_restored_exactly(self):
        values = migration.convert_key_values(
            ["1234567890.123456789"],
            {"amount": {"data_type": "numeric"}},
            ["amount"],
        )
        self.assertEqual(values, [Decimal("1234567890.123456789")])

    def test_partition_identifier_stays_within_postgresql_limit(self):
        name = migration.bounded_identifier("x" * 60, "2026_01_01")
        self.assertLessEqual(len(name.encode("utf-8")), 63)
        self.assertEqual(name, migration.bounded_identifier("x" * 60, "2026_01_01"))

    def test_configured_range_partition_name(self):
        name = migration.render_range_partition_name(
            "procmcustmappkey",
            date(2026, 4, 2),
            "daily",
            "{table}_p{yyyymmdd}",
        )
        self.assertEqual(name, "procmcustmappkey_p20260402")

    def test_configured_list_partition_name(self):
        name = migration.render_list_partition_name(
            "procmcust",
            "custid",
            4101,
            "{table}_p{value}",
        )
        self.assertEqual(name, "procmcust_p4101")

    def test_default_partition_created_after_partitioned_table_load(self):
        connection = FakeConnection()
        cfg = {
            "target": {"schema": "public"},
            "migration": {
                "create_default_partition": True,
                "default_partition_name_format": "{table}_default",
            },
        }
        table_cfg = {
            "target_table": "procmcust",
            "partition_column": "chnid",
        }
        with mock.patch.object(migration, "get_default_partition", return_value=None):
            name = migration.ensure_default_partition(connection, cfg, table_cfg)
        self.assertEqual(name, "procmcust_default")
        self.assertEqual(connection.commits, 1)

    def test_existing_default_partition_is_reused(self):
        connection = FakeConnection()
        cfg = {"target": {"schema": "public"}, "migration": {}}
        table_cfg = {"target_table": "orders", "partition_column": "created_at"}
        with mock.patch.object(
            migration, "get_default_partition", return_value="orders_catchall"
        ):
            name = migration.ensure_default_partition(connection, cfg, table_cfg)
        self.assertEqual(name, "orders_catchall")
        self.assertEqual(connection.commits, 0)

    def test_partition_formats_must_include_unique_value(self):
        with self.assertRaises(ValueError):
            migration.render_range_partition_name(
                "orders", date(2026, 4, 2), "daily", "{table}_p{yyyymm}"
            )
        with self.assertRaises(ValueError):
            migration.render_list_partition_name(
                "customers", "custid", 4101, "{table}_partition"
            )

    def test_range_partition_discovery_creates_only_periods_present_in_batch(self):
        cfg = {
            "target": {"schema": "public"},
            "migration": {"create_missing_partitions": True, "partition_granularity": "monthly"},
        }
        table_cfg = {
            "target_table": "target_table",
            "partition_column": "created_at",
            "partition_type": "range",
        }
        rows = [(1, date(2020, 1, 2)), (2, date(2026, 7, 1))]
        with mock.patch.object(migration, "create_range_partitions") as create_partitions:
            migration.ensure_partitions_for_rows(
                object(), cfg, table_cfg, ["id", "created_at"], rows
            )
        self.assertEqual(create_partitions.call_count, 2)

    def test_first_full_keyset_page_has_no_sentinel_parameters(self):
        connection = FakeConnection(rows=[(1,)])
        result = migration.fetch_full_batch(
            connection, "public", "source_table", ["id"], ["id"], None, 100
        )
        self.assertEqual(result, [(1,)])
        self.assertEqual(connection.executions[-1][1], [100])

    def test_first_incremental_page_has_no_sentinel_parameters(self):
        connection = FakeConnection(rows=[(1,)])
        result = migration.fetch_incremental_batch(
            connection,
            "public",
            "source_table",
            ["id"],
            "updated_at",
            ["id"],
            None,
            None,
            "2026-01-01T00:00:00",
            100,
        )
        self.assertEqual(result, [(1,)])
        self.assertEqual(connection.executions[-1][1], ["2026-01-01T00:00:00", 100])

    def test_conflicts_are_isolated_and_counted(self):
        connection = FakeConnection()
        current_rows = {}

        def remember_rows(_connection, _temp_table, _columns, rows):
            current_rows["rows"] = rows

        with mock.patch.object(migration, "truncate_temp"), \
             mock.patch.object(migration, "copy_to_temp", side_effect=remember_rows), \
             mock.patch.object(migration, "merge_from_temp_business_key", return_value=(0, 0)):
            stats, bad_rows, conflict_rows = migration.load_rows_with_isolation(
                connection,
                "public",
                "target_table",
                "temp_table",
                ["id"],
                ["id"],
                None,
                "full",
                [(1,), (2,)],
            )

        self.assertEqual(stats, {"inserted": 0, "updated": 0, "conflict_skipped": 2})
        self.assertEqual(bad_rows, [])
        self.assertEqual([row["business_key"] for row in conflict_rows], ["1", "2"])

    def test_database_row_error_isolated_without_losing_good_rows(self):
        connection = FakeConnection()
        current_rows = {}

        def remember_rows(_connection, _temp_table, _columns, rows):
            current_rows["rows"] = rows

        def merge_rows(*_args, **_kwargs):
            rows = current_rows["rows"]
            if any(row[0] == 2 for row in rows):
                raise RowDatabaseError("check constraint failed")
            return 0, len(rows)

        with mock.patch.object(migration, "truncate_temp"), \
             mock.patch.object(migration, "copy_to_temp", side_effect=remember_rows), \
             mock.patch.object(migration, "merge_from_temp_business_key", side_effect=merge_rows):
            stats, bad_rows, conflict_rows = migration.load_rows_with_isolation(
                connection,
                "public",
                "target_table",
                "temp_table",
                ["id"],
                ["id"],
                None,
                "full",
                [(1,), (2,), (3,)],
            )

        self.assertEqual(stats, {"inserted": 2, "updated": 0, "conflict_skipped": 0})
        self.assertEqual(len(bad_rows), 1)
        self.assertEqual(bad_rows[0]["business_key"], "2")
        self.assertEqual(conflict_rows, [])

    def test_reconciliation_balance(self):
        stats = migration.new_reconciliation_stats("target_table", "incremental")
        stats.update(processed=10, inserted=4, updated=3, rejected=2, conflict_skipped=1)
        with mock.patch.object(migration, "get_table_row_count", side_effect=[100, 98]):
            result = migration.finalize_reconciliation_stats(
                stats,
                object(),
                object(),
                {"source": {"schema": "public"}, "target": {"schema": "public"}},
                {"source_table": "source_table", "target_table": "target_table"},
            )
        self.assertEqual(result["unaccounted"], 0)
        self.assertEqual(result["target_difference"], -2)


if __name__ == "__main__":
    unittest.main()
