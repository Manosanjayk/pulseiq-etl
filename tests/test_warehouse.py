from __future__ import annotations

import os
import shutil
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from urllib.parse import urlparse
from uuid import uuid4

import pandas as pd
import psycopg
import pytest
from psycopg import sql
from psycopg.rows import dict_row

from src import warehouse


REPO_ROOT = Path(__file__).resolve().parents[1]
CLEAN_DIR = REPO_ROOT / "data" / "clean"
RAW_DIR = REPO_ROOT / "data" / "raw"
REJECTED_DIR = REPO_ROOT / "rejected"


@pytest.fixture
def warehouse_db():
    database_url = os.getenv("WAREHOUSE_TEST_DATABASE_URL")
    if not database_url:
        pytest.skip("Set WAREHOUSE_TEST_DATABASE_URL to run PostgreSQL integration tests")
    database_name = urlparse(database_url).path.lstrip("/")
    if not database_name.endswith("_test"):
        pytest.fail(f"Refusing to run destructive warehouse tests against database {database_name!r}")

    schema_name = f"warehouse_test_{uuid4().hex}"
    conn = psycopg.connect(database_url, row_factory=dict_row)
    assert conn.info.dbname.endswith("_test")
    conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema_name)))
    conn.execute(sql.SQL("SET search_path TO {}, public").format(sql.Identifier(schema_name)))
    conn.commit()
    warehouse.apply_migrations(conn)
    try:
        yield conn, database_url, schema_name
    finally:
        conn.commit()
        conn.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema_name)))
        conn.commit()
        conn.close()


def load_real_source(conn: psycopg.Connection, clean_dir: Path = CLEAN_DIR) -> dict:
    return warehouse.load_warehouse(conn, clean_dir, REJECTED_DIR, RAW_DIR)


def test_migrations_create_schema_indexes_and_are_repeatable(warehouse_db) -> None:
    conn, _, _ = warehouse_db
    warehouse.apply_migrations(conn)
    migration_count = conn.execute("SELECT count(*) AS n FROM schema_migrations").fetchone()["n"]
    table_names = {
        row["table_name"]
        for row in conn.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = current_schema()"
        ).fetchall()
    }
    index_names = {
        row["indexname"]
        for row in conn.execute("SELECT indexname FROM pg_indexes WHERE schemaname = current_schema()").fetchall()
    }

    assert migration_count == 1
    assert {
        "dim_employee", "dim_department", "dim_role", "dim_date", "dim_leave_type",
        "fact_attendance_monthly", "fact_payroll", "fact_leave", "fact_headcount_monthly",
        "warehouse_batch", "warehouse_rejection",
    } <= table_names
    assert {
        "ux_dim_employee_one_current", "ix_payroll_period_department",
        "ix_attendance_month_department", "ix_leave_start_status_type",
        "ix_headcount_department_month",
    } <= index_names


def test_real_source_load_reconciles_independent_csv_aggregates(warehouse_db) -> None:
    conn, _, _ = warehouse_db
    result = load_real_source(conn)
    payroll = pd.read_csv(CLEAN_DIR / "fact_payroll.csv", dtype=str)
    attendance = pd.read_csv(CLEAN_DIR / "fact_attendance_monthly.csv", dtype=str)
    leave = pd.read_csv(CLEAN_DIR / "fact_leave.csv", dtype=str)
    expected_gross = sum((Decimal(value) for value in payroll["gross_pay"]), Decimal(0))
    expected_employer_cost = sum((Decimal(value) for value in payroll["employer_cost"]), Decimal(0))
    expected_present = sum(int(value) for value in attendance["present_days"])
    expected_scheduled = sum(int(value) for value in attendance["scheduled_days"])
    expected_leave = sum(
        (Decimal(row.days) for row in leave.itertuples() if row.status == "Approved"), Decimal(0)
    )
    payroll_duplicate_mask = payroll.drop(columns=["payroll_item_id"]).duplicated(keep=False)
    payroll_duplicate_extras = payroll.loc[
        payroll.drop(columns=["payroll_item_id"]).duplicated(keep="first")
    ]
    facts = conn.execute(
        "SELECT (SELECT count(*) FROM fact_attendance_monthly) AS attendance_rows, "
        "(SELECT count(*) FROM fact_payroll) AS payroll_rows, "
        "(SELECT sum(gross_pay) FROM fact_payroll) AS gross_pay, "
        "(SELECT sum(employer_cost) FROM fact_payroll) AS employer_cost, "
        "(SELECT sum(present_days) FROM fact_attendance_monthly) AS present_days, "
        "(SELECT sum(scheduled_days) FROM fact_attendance_monthly) AS scheduled_days, "
        "(SELECT sum(days) FROM fact_leave WHERE status = 'Approved') AS approved_leave_days"
    ).fetchone()
    audit = conn.execute(
        "SELECT source_counts, accepted_counts, rejected_counts, reconciliation "
        "FROM warehouse_batch WHERE batch_id = %s",
        (result["batch_id"],),
    ).fetchone()

    assert result["status"] == "success"
    assert facts["attendance_rows"] == len(attendance)
    assert facts["payroll_rows"] == len(payroll)
    assert facts["gross_pay"] == expected_gross
    assert facts["employer_cost"] == expected_employer_cost
    assert facts["present_days"] == expected_present == 141139
    assert facts["scheduled_days"] == expected_scheduled == 150155
    assert facts["approved_leave_days"] == expected_leave == Decimal("7274.00")
    assert payroll_duplicate_mask.sum() == 0
    assert len(payroll_duplicate_extras) == 0
    assert audit["rejected_counts"]["fact_leave"] == 2
    assert audit["rejected_counts"]["fact_payroll"] == 4
    assert audit["rejected_counts"]["fact_attendance_monthly"] == 0
    assert audit["accepted_counts"]["dim_employee"] == 1606
    assert set(audit["accepted_counts"]) == set(warehouse.DATASETS) | {"fact_headcount_monthly"}
    assert audit["reconciliation"]["payroll_duplicate_candidate_groups"] == "0"
    assert audit["reconciliation"]["payroll_duplicate_candidate_rows"] == "0"
    assert audit["reconciliation"]["payroll_duplicate_extra_rows"] == "0"
    assert audit["reconciliation"]["payroll_duplicate_extra_gross_pay"] == "0"
    assert audit["reconciliation"]["payroll_duplicate_extra_employer_cost"] == "0"
    assert conn.execute("SELECT count(*) AS n FROM warehouse_rejection").fetchone()["n"] == 6
    assert conn.execute("SELECT count(*) AS n FROM dim_employee WHERE is_current").fetchone()["n"] == 745
    assert conn.execute("SELECT count(*) AS n FROM fact_headcount_monthly").fetchone()["n"] > 0


def test_repeated_loads_upsert_without_duplicate_facts(warehouse_db) -> None:
    conn, _, _ = warehouse_db
    load_real_source(conn)
    first = {
        table: conn.execute(sql.SQL("SELECT count(*) AS n FROM {}").format(sql.Identifier(table))).fetchone()["n"]
        for table in ("dim_employee", "fact_attendance_monthly", "fact_payroll", "fact_leave", "fact_headcount_monthly")
    }

    second_result = load_real_source(conn)
    second = {
        table: conn.execute(sql.SQL("SELECT count(*) AS n FROM {}").format(sql.Identifier(table))).fetchone()["n"]
        for table in first
    }

    assert first == second
    assert second_result["status"] == "success"
    assert conn.execute("SELECT count(*) AS n FROM warehouse_batch WHERE status = 'success'").fetchone()["n"] == 2


def test_postgres_constraints_reject_duplicate_current_invalid_fk_and_bad_leave_range(warehouse_db) -> None:
    conn, _, _ = warehouse_db
    load_real_source(conn)
    current = conn.execute("SELECT * FROM dim_employee WHERE is_current LIMIT 1").fetchone()
    columns = [column for column in current if column != "employee_sk"]
    duplicate = dict(current)
    duplicate["employee_sk"] = None
    duplicate["source_employee_key"] = int(current["source_employee_key"]) + 9000000
    duplicate["valid_from"] = date(1900, 1, 1)
    duplicate["valid_to"] = date(1900, 1, 2)
    duplicate["source_valid_to"] = date(1900, 1, 2)
    duplicate["manager_sk"] = None
    with pytest.raises(psycopg.errors.UniqueViolation):
        with conn.transaction():
            conn.execute(
                sql.SQL("INSERT INTO dim_employee ({}) VALUES ({})").format(
                    sql.SQL(", ").join(map(sql.Identifier, columns)),
                    sql.SQL(", ").join(sql.Placeholder() for _ in columns),
                ),
                [duplicate[column] for column in columns],
            )

    with pytest.raises(psycopg.errors.ForeignKeyViolation):
        with conn.transaction():
            conn.execute(
                "INSERT INTO dim_role(role_id, department_id, role_title, level, grade, band_min_annual, "
                "band_mid_annual, band_max_annual, overtime_eligible, band_reference_year) "
                "VALUES ('ROLE-BAD', 'UNKNOWN-DEPT', 'Test', 1, 'T1', 1, 1, 1, false, 2026)"
            )

    leave = conn.execute("SELECT leave_request_id, start_date FROM fact_leave LIMIT 1").fetchone()
    with pytest.raises(psycopg.errors.CheckViolation):
        with conn.transaction():
            conn.execute(
                "UPDATE fact_leave SET end_date = start_date - 1 WHERE leave_request_id = %s",
                (leave["leave_request_id"],),
            )


def test_scd_department_transfer_is_test_only_and_historical(warehouse_db, tmp_path: Path) -> None:
    conn, _, _ = warehouse_db
    test_clean = tmp_path / "clean"
    test_clean.mkdir()
    for source in CLEAN_DIR.glob("*.csv"):
        shutil.copy2(source, test_clean / source.name)
    employees = pd.read_csv(test_clean / "dim_employee.csv", dtype=str, keep_default_na=False)
    departments = pd.read_csv(test_clean / "dim_department.csv", dtype=str)
    roles = pd.read_csv(test_clean / "dim_role.csv", dtype=str)
    current_index = employees.index[employees["is_current"].eq("Y")][0]
    previous = employees.loc[current_index].copy()
    transfer_date = date.fromisoformat(previous["valid_from"][:10]) + timedelta(days=60)
    previous["valid_to"] = (transfer_date - timedelta(days=1)).isoformat()
    previous["is_current"] = "N"
    employees.loc[current_index] = previous
    destination = next(value for value in departments.department_id if value != previous["department_id"])
    role = roles.loc[roles.department_id.eq(destination)].iloc[0]
    moved = previous.copy()
    moved["employee_key"] = str(max(map(int, employees.employee_key)) + 9000000)
    moved["department_id"] = destination
    moved["role_id"] = role.role_id
    moved["role_title"] = role.role_title
    moved["job_level"] = str(role.level)
    moved["valid_from"] = transfer_date.isoformat()
    moved["valid_to"] = "9999-12-31"
    moved["is_current"] = "Y"
    moved["change_reason"] = "TEST ONLY: synthetic department transfer"
    employees = pd.concat([employees, pd.DataFrame([moved])], ignore_index=True)
    employees.to_csv(test_clean / "dim_employee.csv", index=False)

    load_real_source(conn, test_clean)
    before, after = transfer_date - timedelta(days=1), transfer_date
    historical = conn.execute(
        "SELECT department_id FROM dim_employee WHERE employee_id = %s "
        "AND %s BETWEEN valid_from AND valid_to",
        (previous["employee_id"], before),
    ).fetchone()
    current = conn.execute(
        "SELECT department_id FROM dim_employee WHERE employee_id = %s "
        "AND %s BETWEEN valid_from AND valid_to",
        (previous["employee_id"], after),
    ).fetchone()

    assert historical["department_id"] == previous["department_id"]
    assert current["department_id"] == destination
    assert conn.execute(
        "SELECT count(*) AS n FROM dim_employee WHERE employee_id = %s AND is_current",
        (previous["employee_id"],),
    ).fetchone()["n"] == 1
    assert not any(row["change_reason"] == "TEST ONLY: synthetic department transfer" for row in pd.read_csv(CLEAN_DIR / "dim_employee.csv", dtype=str).to_dict("records"))


def test_failed_load_rolls_back_all_data_and_records_failure(warehouse_db, tmp_path: Path) -> None:
    conn, _, _ = warehouse_db
    load_real_source(conn)
    before = conn.execute(
        "SELECT (SELECT count(*) FROM dim_employee) AS employees, "
        "(SELECT count(*) FROM fact_payroll) AS payroll"
    ).fetchone()
    test_clean = tmp_path / "clean"
    test_clean.mkdir()
    for source in CLEAN_DIR.glob("*.csv"):
        shutil.copy2(source, test_clean / source.name)
    payroll = pd.read_csv(test_clean / "fact_payroll.csv", dtype=str)
    payroll.loc[0, "employee_key"] = "999999999999"
    payroll.to_csv(test_clean / "fact_payroll.csv", index=False)

    with pytest.raises(KeyError):
        load_real_source(conn, test_clean)

    after = conn.execute(
        "SELECT (SELECT count(*) FROM dim_employee) AS employees, "
        "(SELECT count(*) FROM fact_payroll) AS payroll"
    ).fetchone()
    failed = conn.execute("SELECT status FROM warehouse_batch ORDER BY batch_id DESC LIMIT 1").fetchone()
    assert before == after
    assert failed["status"] == "failed"


def test_concurrent_loads_serialize_and_remain_idempotent(warehouse_db) -> None:
    _, database_url, schema_name = warehouse_db

    def run_load() -> dict:
        with psycopg.connect(database_url) as conn:
            conn.execute(sql.SQL("SET search_path TO {}, public").format(sql.Identifier(schema_name)))
            conn.commit()
            return load_real_source(conn)

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: run_load(), range(2)))
    assert all(result["status"] == "success" for result in results)
