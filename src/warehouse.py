from __future__ import annotations

import argparse
import json
import os
from collections.abc import Mapping
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import pandas as pd
import psycopg
from psycopg.types.json import Jsonb


ROOT = Path(__file__).resolve().parents[1]
MIGRATIONS_DIR = ROOT / "sql" / "migrations"
DATASETS = (
    "dim_date",
    "dim_department",
    "dim_leave_type",
    "dim_location",
    "dim_role",
    "dim_employee",
    "fact_attendance_monthly",
    "fact_leave",
    "fact_payroll",
)
PRIMARY_KEYS = {
    "dim_date": "date_key",
    "dim_department": "department_id",
    "dim_leave_type": "leave_type_code",
    "dim_location": "work_location",
    "dim_role": "role_id",
    "dim_employee": "employee_key",
    "fact_attendance_monthly": "attendance_fact_id",
    "fact_leave": "leave_request_id",
    "fact_payroll": "payroll_item_id",
}

DATE_COLUMNS = {
    "dim_date": ("date", "month_start_date", "month_end_date"),
    "dim_employee": ("date_of_birth", "hire_date", "exit_date", "valid_from", "valid_to"),
    "fact_attendance_monthly": ("period_end_date",),
    "fact_leave": ("start_date", "end_date", "applied_on", "decided_on"),
    "fact_payroll": ("period_end_date",),
}
TABLE_COLUMNS = {
    "dim_date": (
        "date", "date_key", "year", "quarter", "month_number", "month_name", "year_month",
        "week_of_year", "day_of_week", "is_weekend", "is_holiday", "is_working_day",
        "financial_year", "fiscal_quarter", "fiscal_month_number", "is_month_end",
        "month_start_date", "month_end_date",
    ),
    "dim_department": ("department_id", "department_name", "cost_center", "description", "is_leadership"),
    "dim_leave_type": (
        "leave_type_code", "leave_type_name", "pay_type", "annual_quota_days",
        "prorated_for_joiners", "carry_forward_allowed", "max_carry_forward_days", "policy_note",
    ),
    "dim_location": ("work_location", "state", "city_tier", "tier_number"),
    "dim_role": (
        "role_id", "department_id", "role_title", "level", "grade", "band_min_annual",
        "band_mid_annual", "band_max_annual", "overtime_eligible", "band_reference_year",
    ),
    "dim_employee": (
        "employee_key", "employee_id", "full_name", "gender", "date_of_birth", "hire_date",
        "exit_date", "employment_status", "employment_type", "department_id", "role_id",
        "role_title", "job_level", "manager_id", "work_location", "work_mode",
        "highest_education", "valid_from", "valid_to", "is_current", "change_reason",
        "tenure_years",
    ),
    "fact_attendance_monthly": (
        "attendance_fact_id", "employee_id", "date_key", "employee_key", "department_id",
        "scheduled_days", "present_days", "wfh_days", "half_days_unpaid", "paid_leave_days",
        "unpaid_absence_days", "late_arrivals", "avg_hours_worked", "overtime_hours",
        "attendance_rate", "period_end_date",
    ),
    "fact_leave": (
        "leave_request_id", "employee_id", "employee_key", "leave_type_code", "start_date_key",
        "end_date_key", "start_date", "end_date", "days", "status", "applied_on", "decided_on",
        "approver_id", "reason",
    ),
    "fact_payroll": (
        "payroll_item_id", "run_id", "employee_id", "date_key", "employee_key", "period_end_date",
        "department_id", "role_id", "payable_days", "lop_days", "overtime_hours_paid",
        "variable_pay_type", "monthly_fixed_gross", "basic", "hra", "conveyance", "medical",
        "special_allowance", "earned_fixed_pay", "overtime_pay", "variable_pay", "gross_pay",
        "pf_employee", "esi_employee", "professional_tax", "tds", "other_deductions",
        "total_deductions", "net_pay", "pf_employer", "esi_employer", "employer_cost",
        "headcount_flag",
    ),
}


def connect(database_url: str | None = None) -> psycopg.Connection:
    dsn = database_url or os.getenv(
        "WAREHOUSE_DATABASE_URL",
        "postgresql://pulseiq:pulseiq_local_only@localhost:5432/pulseiq_warehouse",
    )
    return psycopg.connect(dsn)


def apply_migrations(conn: psycopg.Connection, migrations_dir: Path = MIGRATIONS_DIR) -> None:
    with conn.transaction():
        conn.execute("SELECT pg_advisory_xact_lock(81420261013)")
        conn.execute(
            "CREATE TABLE IF NOT EXISTS schema_migrations "
            "(version text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())"
        )
        applied = {
            row["version"] if isinstance(row, Mapping) else row[0]
            for row in conn.execute("SELECT version FROM schema_migrations").fetchall()
        }
        for migration in sorted(migrations_dir.glob("*.sql")):
            if migration.name in applied:
                continue
            conn.execute(migration.read_text(encoding="utf-8"))
            conn.execute("INSERT INTO schema_migrations(version) VALUES (%s)", (migration.name,))


def _read_csv(path: Path, table: str) -> pd.DataFrame:
    if not path.is_file():
        raise FileNotFoundError(f"Required {table} CSV not found: {path}")
    frame = pd.read_csv(path, dtype="string", keep_default_na=False)
    expected = set(TABLE_COLUMNS[table])
    if table == "dim_employee":
        expected.add("tenure_years")
    actual = set(frame.columns)
    if actual != expected:
        raise ValueError(
            f"Schema mismatch for {table}: missing={sorted(expected - actual)}; "
            f"unexpected={sorted(actual - expected)}"
        )
    return frame


def _text(value: Any) -> str | None:
    if value is None or pd.isna(value) or str(value).strip() == "":
        return None
    return str(value).strip()


def _date(value: Any) -> date | None:
    text = _text(value)
    return date.fromisoformat(text[:10]) if text else None


def _integer(value: Any) -> int | None:
    text = _text(value)
    if text is None:
        return None
    return int(Decimal(text))


def _decimal(value: Any) -> Decimal | None:
    text = _text(value)
    return Decimal(text) if text else None


def _boolean(value: Any) -> bool | None:
    text = _text(value)
    if text in ("Y", "True", "true", "1"):
        return True
    if text in ("N", "False", "false", "0"):
        return False
    if text is None:
        return None
    raise ValueError(f"Expected Y/N boolean, received {text!r}")


def _value(row: pd.Series, column: str) -> Any:
    value = row[column]
    if column in DATE_COLUMNS.get(row.name, ()):
        return _date(value)
    return _text(value)


def _date_key(value: Any) -> int:
    parsed = _date(value)
    if parsed is None:
        raise ValueError("Required date value is blank")
    return int(parsed.strftime("%Y%m%d"))


def _rows(frame: pd.DataFrame) -> list[pd.Series]:
    return [row for _, row in frame.iterrows()]


def _upsert(conn: psycopg.Connection, table: str, columns: tuple[str, ...], values: list[tuple[Any, ...]], key: str) -> None:
    if not values:
        return
    updates = ", ".join(f"{column} = EXCLUDED.{column}" for column in columns if column != key)
    sql = (
        f"INSERT INTO {table} ({', '.join(columns)}) VALUES "
        f"({', '.join(['%s'] * len(columns))}) ON CONFLICT ({key}) DO UPDATE SET {updates}"
    )
    with conn.cursor() as cursor:
        cursor.executemany(sql, values)


def _insert_batch(conn: psycopg.Connection, source_dir: Path, source_counts: dict[str, int]) -> int:
    row = conn.execute(
        "INSERT INTO warehouse_batch(status, source_directory, source_counts) "
        "VALUES ('running', %s, %s) RETURNING batch_id",
        (str(source_dir), Jsonb(source_counts)),
    ).fetchone()
    return int(row["batch_id"] if isinstance(row, Mapping) else row[0])


def _prepare_employee_rows(frame: pd.DataFrame) -> list[dict[str, Any]]:
    source_rows = frame.to_dict(orient="records")
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in source_rows:
        grouped.setdefault(str(row["employee_id"]), []).append(row)
    for versions in grouped.values():
        versions.sort(key=lambda row: str(row["valid_from"]))

    prepared: list[dict[str, Any]] = []
    for employee_id, versions in grouped.items():
        for index, row in enumerate(versions):
            start = _date(row["valid_from"])
            source_end = _date(row["valid_to"])
            effective_end = source_end
            if index + 1 < len(versions):
                next_start = _date(versions[index + 1]["valid_from"])
                if effective_end is not None and next_start is not None and effective_end >= next_start:
                    effective_end = next_start - timedelta(days=1)
            if start is None or effective_end is None or effective_end < start:
                raise ValueError(f"Invalid effective SCD dates for {employee_id}, key={row['employee_key']}")
            prepared.append(
                {
                    **row,
                    "_employee_id": employee_id,
                    "_valid_from": start,
                    "_valid_to": effective_end,
                    "_source_valid_to": source_end,
                }
            )
    return prepared


def _load_dimensions(conn: psycopg.Connection, frames: dict[str, pd.DataFrame]) -> dict[int, int]:
    date_values = []
    for row in _rows(frames["dim_date"]):
        date_value = _date(row["date"])
        date_values.append((
            _integer(row["date_key"]), date_value, _integer(row["year"]), _integer(row["quarter"]),
            _integer(row["month_number"]), _text(row["month_name"]), _text(row["year_month"]),
            _integer(row["week_of_year"]), _text(row["day_of_week"]), _boolean(row["is_weekend"]),
            _boolean(row["is_holiday"]), _boolean(row["is_working_day"]), _text(row["financial_year"]),
            _text(row["fiscal_quarter"]), _integer(row["fiscal_month_number"]),
            _boolean(row["is_month_end"]), _date(row["month_start_date"]), _date(row["month_end_date"]),
        ))
    _upsert(conn, "dim_date", (
        "date_key", "date_value", "calendar_year", "quarter", "month_number", "month_name",
        "year_month", "week_of_year", "day_of_week", "is_weekend", "is_holiday", "is_working_day",
        "financial_year", "fiscal_quarter", "fiscal_month_number", "is_month_end", "month_start_date",
        "month_end_date",
    ), date_values, "date_key")

    department_values = [(
        _text(row.department_id), _text(row.department_name), _text(row.cost_center),
        _text(row.description), _boolean(row.is_leadership),
    ) for row in _rows(frames["dim_department"])]
    _upsert(conn, "dim_department", ("department_id", "department_name", "cost_center", "description", "is_leadership"), department_values, "department_id")

    location_values = [(
        _text(row.work_location), _text(row.state), _text(row.city_tier), _integer(row.tier_number),
    ) for row in _rows(frames["dim_location"])]
    _upsert(conn, "dim_location", ("work_location", "state", "city_tier", "tier_number"), location_values, "work_location")

    leave_type_values = [(
        _text(row.leave_type_code), _text(row.leave_type_name), _text(row.pay_type),
        _decimal(row.annual_quota_days), _boolean(row.prorated_for_joiners),
        _boolean(row.carry_forward_allowed), _decimal(row.max_carry_forward_days), _text(row.policy_note),
    ) for row in _rows(frames["dim_leave_type"])]
    _upsert(conn, "dim_leave_type", (
        "leave_type_code", "leave_type_name", "pay_type", "annual_quota_days", "prorated_for_joiners",
        "carry_forward_allowed", "max_carry_forward_days", "policy_note",
    ), leave_type_values, "leave_type_code")

    role_values = [(
        _text(row.role_id), _text(row.department_id), _text(row.role_title), _integer(row.level),
        _text(row.grade), _decimal(row.band_min_annual), _decimal(row.band_mid_annual),
        _decimal(row.band_max_annual), _boolean(row.overtime_eligible), _integer(row.band_reference_year),
    ) for row in _rows(frames["dim_role"])]
    _upsert(conn, "dim_role", (
        "role_id", "department_id", "role_title", "level", "grade", "band_min_annual",
        "band_mid_annual", "band_max_annual", "overtime_eligible", "band_reference_year",
    ), role_values, "role_id")

    employee_rows = _prepare_employee_rows(frames["dim_employee"])
    employee_ids = sorted({row["_employee_id"] for row in employee_rows})
    if employee_ids:
        conn.execute(
            "UPDATE dim_employee SET is_current = false WHERE employee_id = ANY(%s)",
            (employee_ids,),
        )
    employee_values = []
    for row in employee_rows:
        employee_values.append((
            _integer(row["employee_key"]), row["_employee_id"], _text(row["full_name"]), _text(row["gender"]),
            _date(row["date_of_birth"]), _date(row["hire_date"]), _date(row["exit_date"]),
            _text(row["employment_status"]), _text(row["employment_type"]), _text(row["department_id"]),
            _text(row["role_id"]), _text(row["role_title"]), _integer(row["job_level"]),
            _text(row["manager_id"]), _text(row["work_location"]), _text(row["work_mode"]),
            _text(row["highest_education"]), row["_valid_from"], row["_valid_to"],
            row["_source_valid_to"], _boolean(row["is_current"]), _text(row["change_reason"]),
            _decimal(row["tenure_years"]),
        ))
    employee_columns = (
        "source_employee_key", "employee_id", "full_name", "gender", "date_of_birth", "hire_date",
        "exit_date", "employment_status", "employment_type", "department_id", "role_id", "role_title",
        "job_level", "manager_employee_id", "work_location", "work_mode", "highest_education", "valid_from",
        "valid_to", "source_valid_to", "is_current", "change_reason", "tenure_years",
    )
    updates = ", ".join(f"{column} = EXCLUDED.{column}" for column in employee_columns if column != "source_employee_key")
    if employee_values:
        with conn.cursor() as cursor:
            cursor.executemany(
                f"INSERT INTO dim_employee ({', '.join(employee_columns)}) VALUES "
                f"({', '.join(['%s'] * len(employee_columns))}) "
                f"ON CONFLICT (source_employee_key) DO UPDATE SET {updates}",
                employee_values,
            )
    sk_rows = conn.execute("SELECT source_employee_key, employee_sk FROM dim_employee").fetchall()
    employee_sk = {
        int(row["source_employee_key"] if isinstance(row, Mapping) else row[0]):
        int(row["employee_sk"] if isinstance(row, Mapping) else row[1])
        for row in sk_rows
    }
    conn.execute("UPDATE dim_employee SET manager_sk = NULL")
    conn.execute(
        "UPDATE dim_employee AS employee SET manager_sk = manager.employee_sk "
        "FROM dim_employee AS manager "
        "WHERE employee.manager_employee_id = manager.employee_id AND manager.is_current"
    )
    return employee_sk


def _load_facts(
    conn: psycopg.Connection,
    frames: dict[str, pd.DataFrame],
    employee_sk: dict[int, int],
    batch_id: int,
) -> dict[str, int]:
    accepted: dict[str, int] = {}
    attendance_values = []
    for row in _rows(frames["fact_attendance_monthly"]):
        key = _integer(row.employee_key)
        attendance_values.append((
            _text(row.attendance_fact_id), _text(row.employee_id), employee_sk[key], _integer(row.date_key),
            _text(row.department_id), _integer(row.scheduled_days), _integer(row.present_days),
            _decimal(row.wfh_days), _decimal(row.half_days_unpaid), _decimal(row.paid_leave_days),
            _decimal(row.unpaid_absence_days), _integer(row.late_arrivals), _decimal(row.avg_hours_worked),
            _decimal(row.overtime_hours), _decimal(row.attendance_rate), _date(row.period_end_date), batch_id,
        ))
    attendance_columns = (
        "attendance_fact_id", "employee_id", "employee_sk", "date_key", "department_id", "scheduled_days",
        "present_days", "wfh_days", "half_days_unpaid", "paid_leave_days", "unpaid_absence_days",
        "late_arrivals", "avg_hours_worked", "overtime_hours", "attendance_rate", "period_end_date", "batch_id",
    )
    _upsert(conn, "fact_attendance_monthly", attendance_columns, attendance_values, "attendance_fact_id")
    accepted["fact_attendance_monthly"] = len(attendance_values)

    payroll_columns = (
        "payroll_item_id", "run_id", "employee_id", "employee_sk", "date_key", "period_end_date",
        "department_id", "role_id", "payable_days", "lop_days", "overtime_hours_paid", "variable_pay_type",
        "monthly_fixed_gross", "basic", "hra", "conveyance", "medical", "special_allowance",
        "earned_fixed_pay", "overtime_pay", "variable_pay", "gross_pay", "pf_employee", "esi_employee",
        "professional_tax", "tds", "other_deductions", "total_deductions", "net_pay", "pf_employer",
        "esi_employer", "employer_cost", "headcount_flag", "batch_id",
    )
    payroll_values = []
    for row in _rows(frames["fact_payroll"]):
        key = _integer(row.employee_key)
        values: list[Any] = [
            _text(row.payroll_item_id), _text(row.run_id), _text(row.employee_id), employee_sk[key],
            _integer(row.date_key), _date(row.period_end_date), _text(row.department_id), _text(row.role_id),
        ]
        values.extend(_decimal(row[column]) for column in ("payable_days", "lop_days", "overtime_hours_paid"))
        values.append(_text(row.variable_pay_type))
        values.extend(_decimal(row[column]) for column in (
            "monthly_fixed_gross",
            "basic", "hra", "conveyance", "medical", "special_allowance", "earned_fixed_pay", "overtime_pay",
            "variable_pay", "gross_pay", "pf_employee", "esi_employee", "professional_tax", "tds",
            "other_deductions", "total_deductions", "net_pay", "pf_employer", "esi_employer", "employer_cost",
        ))
        values.extend((_integer(row.headcount_flag), batch_id))
        payroll_values.append(tuple(values))
    _upsert(conn, "fact_payroll", payroll_columns, payroll_values, "payroll_item_id")
    accepted["fact_payroll"] = len(payroll_values)

    leave_columns = (
        "leave_request_id", "employee_id", "employee_sk", "leave_type_code", "start_date_key", "end_date_key",
        "start_date", "end_date", "days", "status", "applied_on", "decided_on", "approver_id", "reason", "batch_id",
    )
    leave_values = []
    for row in _rows(frames["fact_leave"]):
        key = _integer(row.employee_key)
        leave_values.append((
            _text(row.leave_request_id), _text(row.employee_id), employee_sk[key], _text(row.leave_type_code),
            _integer(row.start_date_key), _integer(row.end_date_key), _date(row.start_date), _date(row.end_date),
            _decimal(row.days), _text(row.status), _date(row.applied_on), _date(row.decided_on),
            _text(row.approver_id), _text(row.reason), batch_id,
        ))
    _upsert(conn, "fact_leave", leave_columns, leave_values, "leave_request_id")
    accepted["fact_leave"] = len(leave_values)

    accepted["fact_headcount_monthly"] = _upsert_headcount(conn, frames, batch_id)
    return accepted


def _upsert_headcount(conn: psycopg.Connection, frames: dict[str, pd.DataFrame], batch_id: int) -> int:
    month_ends = sorted({_date(value) for value in frames["fact_payroll"]["period_end_date"] if _date(value) is not None})
    if not month_ends:
        month_ends = sorted({_date(value) for value in frames["fact_attendance_monthly"]["period_end_date"] if _date(value) is not None})
    employees = _prepare_employee_rows(frames["dim_employee"])
    departments = [
        row["department_id"] if isinstance(row, Mapping) else row[0]
        for row in conn.execute("SELECT department_id FROM dim_department ORDER BY department_id").fetchall()
    ]
    values = []
    for month_end in month_ends:
        month_start = month_end.replace(day=1)
        counts = {department: 0 for department in departments}
        hires = {department: set() for department in departments}
        attrition = {department: set() for department in departments}
        for row in employees:
            employee_id = row["_employee_id"]
            start = row["_valid_from"]
            end = row["_valid_to"]
            hire_date = _date(row["hire_date"])
            exit_date = _date(row["exit_date"])
            department = str(row["department_id"])
            if (
                start <= month_end <= end
                and str(row["employment_status"]) == "Active"
                and hire_date is not None and hire_date <= month_end
                and (exit_date is None or exit_date >= month_end)
            ):
                counts[department] += 1
            if hire_date is not None and month_start <= hire_date <= month_end:
                hires[department].add(employee_id)
            if exit_date is not None and month_start <= exit_date <= month_end and start <= exit_date <= end:
                attrition[department].add(employee_id)
        values.extend((month_end, department, counts[department], len(hires[department]), len(attrition[department]), batch_id) for department in departments)
    _upsert(conn, "fact_headcount_monthly", (
        "month_end_date", "department_id", "employee_count", "new_hires", "attrition_count", "batch_id",
    ), values, "month_end_date, department_id")
    return len(values)


def _load_rejections(conn: psycopg.Connection, rejected_dir: Path, batch_id: int) -> dict[str, int]:
    rejected_counts: dict[str, int] = {}
    for table in DATASETS:
        path = rejected_dir / f"{table}_rejected.csv"
        if not path.is_file():
            rejected_counts[table] = 0
            continue
        frame = pd.read_csv(path, dtype="string", keep_default_na=False)
        rejected_counts[table] = len(frame)
        key_column = PRIMARY_KEYS[table]
        for row in _rows(frame):
            record = {str(key): _text(value) for key, value in row.to_dict().items()}
            conn.execute(
                "INSERT INTO warehouse_rejection(batch_id, source_table, source_key, rejection_reason, source_record) "
                "VALUES (%s, %s, %s, %s, %s)",
                (batch_id, table, _text(row.get(key_column)), _text(row.get("rejection_reason")) or "unspecified", Jsonb(record)),
            )
    return rejected_counts


def _reconcile(conn: psycopg.Connection, frames: dict[str, pd.DataFrame]) -> dict[str, str | int]:
    expected = {
        "attendance_rows": len(frames["fact_attendance_monthly"]),
        "payroll_rows": len(frames["fact_payroll"]),
        "payroll_gross_pay": sum((_decimal(value) or Decimal(0) for value in frames["fact_payroll"]["gross_pay"]), Decimal(0)),
        "payroll_employer_cost": sum((_decimal(value) or Decimal(0) for value in frames["fact_payroll"]["employer_cost"]), Decimal(0)),
        "attendance_present_days": sum((_integer(value) or 0 for value in frames["fact_attendance_monthly"]["present_days"]), 0),
        "attendance_scheduled_days": sum((_integer(value) or 0 for value in frames["fact_attendance_monthly"]["scheduled_days"]), 0),
        "approved_leave_days": sum((_decimal(row.days) or Decimal(0) for row in _rows(frames["fact_leave"]) if row.status == "Approved"), Decimal(0)),
    }
    loaded = conn.execute(
        "SELECT (SELECT count(*) FROM fact_attendance_monthly) AS attendance_rows, "
        "(SELECT count(*) FROM fact_payroll) AS payroll_rows, "
        "(SELECT COALESCE(sum(gross_pay), 0) FROM fact_payroll) AS payroll_gross_pay, "
        "(SELECT COALESCE(sum(employer_cost), 0) FROM fact_payroll) AS payroll_employer_cost, "
        "(SELECT COALESCE(sum(present_days), 0) FROM fact_attendance_monthly) AS attendance_present_days, "
        "(SELECT COALESCE(sum(scheduled_days), 0) FROM fact_attendance_monthly) AS attendance_scheduled_days, "
        "(SELECT COALESCE(sum(days), 0) FROM fact_leave WHERE status = 'Approved') AS approved_leave_days"
    ).fetchone()
    actual = (
        {key: loaded[key] for key in expected}
        if isinstance(loaded, Mapping)
        else dict(zip(expected, loaded, strict=True))
    )
    if any(Decimal(str(actual[key])) != Decimal(str(value)) for key, value in expected.items()):
        raise ValueError(f"Warehouse reconciliation mismatch: expected={expected}; loaded={actual}")
    if expected["attendance_scheduled_days"]:
        expected["attendance_rate"] = str(
            Decimal(expected["attendance_present_days"]) / Decimal(expected["attendance_scheduled_days"])
        )
    payroll = frames["fact_payroll"]
    payroll_business_columns = [column for column in payroll.columns if column != "payroll_item_id"]
    duplicate_extras = payroll.loc[payroll.duplicated(subset=payroll_business_columns, keep="first")]
    duplicate_mask = payroll.duplicated(subset=payroll_business_columns, keep=False)
    duplicate_groups = payroll.loc[duplicate_mask, ["run_id", "employee_id", "employee_key", "period_end_date"]].drop_duplicates()
    expected.update({
        "payroll_duplicate_candidate_groups": int(len(duplicate_groups)),
        "payroll_duplicate_candidate_rows": int(duplicate_mask.sum()),
        "payroll_duplicate_extra_rows": int(len(duplicate_extras)),
        "payroll_duplicate_extra_gross_pay": str(
            sum((_decimal(value) or Decimal(0) for value in duplicate_extras["gross_pay"]), Decimal(0))
        ),
        "payroll_duplicate_extra_employer_cost": str(
            sum((_decimal(value) or Decimal(0) for value in duplicate_extras["employer_cost"]), Decimal(0))
        ),
    })
    return {key: str(value) for key, value in expected.items()}


def load_warehouse(
    conn: psycopg.Connection,
    clean_dir: Path = ROOT / "data" / "clean",
    rejected_dir: Path = ROOT / "rejected",
    raw_dir: Path = ROOT / "data" / "raw",
) -> dict[str, Any]:
    frames = {table: _read_csv(clean_dir / f"{table}.csv", table) for table in DATASETS}
    source_counts = {
        table: len(pd.read_csv(raw_dir / f"{table}.csv", dtype="string", keep_default_na=False))
        if (raw_dir / f"{table}.csv").is_file() else len(frames[table])
        for table in DATASETS
    }
    try:
        with conn.transaction():
            conn.execute("SELECT pg_advisory_xact_lock(81420261014)")
            batch_id = _insert_batch(conn, clean_dir, source_counts)
            employee_sk = _load_dimensions(conn, frames)
            accepted_counts = {
                table: len(frames[table])
                for table in ("dim_date", "dim_department", "dim_leave_type", "dim_location", "dim_role", "dim_employee")
            }
            accepted_counts.update(_load_facts(conn, frames, employee_sk, batch_id))
            rejected_counts = _load_rejections(conn, rejected_dir, batch_id)
            reconciliation = _reconcile(conn, frames)
            conn.execute(
                "UPDATE warehouse_batch SET status = 'success', finished_at = now(), "
                "accepted_counts = %s, rejected_counts = %s, reconciliation = %s WHERE batch_id = %s",
                (Jsonb(accepted_counts), Jsonb(rejected_counts), Jsonb(reconciliation), batch_id),
            )
        return {
            "batch_id": batch_id,
            "status": "success",
            "source_counts": source_counts,
            "accepted_counts": accepted_counts,
            "rejected_counts": rejected_counts,
            "reconciliation": reconciliation,
        }
    except Exception as exc:
        with conn.transaction():
            failed = _insert_batch(conn, clean_dir, source_counts)
            conn.execute(
                "UPDATE warehouse_batch SET status = 'failed', finished_at = now(), error_message = %s, "
                "source_counts = %s WHERE batch_id = %s",
                (f"{type(exc).__name__}: {exc}", Jsonb(source_counts), failed),
            )
        raise


def main() -> int:
    parser = argparse.ArgumentParser(description="Load validated PulseIQ CSV outputs into PostgreSQL.")
    parser.add_argument("--clean-dir", type=Path, default=Path(os.getenv("CLEAN_DIR", "data/clean")))
    parser.add_argument("--rejected-dir", type=Path, default=Path(os.getenv("REJECTED_DIR", "rejected")))
    parser.add_argument("--raw-dir", type=Path, default=Path(os.getenv("RAW_DIR", "data/raw")))
    args = parser.parse_args()
    with connect() as conn:
        apply_migrations(conn)
        result = load_warehouse(conn, args.clean_dir, args.rejected_dir, args.raw_dir)
    print(json.dumps(result, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())