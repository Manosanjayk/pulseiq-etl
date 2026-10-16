from __future__ import annotations

import importlib.util
import json
import os
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from uuid import uuid4

import pandas as pd
import pandas.testing as pdt
import pandera as pa
import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
ETL_PATH = REPO_ROOT / "src" / "etl.py"
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


def build_frames() -> dict[str, pd.DataFrame]:
    dates = []
    for day in (29, 30, 31):
        date_value = f"2025-01-{day:02d}"
        dates.append(
            {
                "date": date_value,
                "date_key": f"202501{day:02d}",
                "year": "2025",
                "quarter": "1",
                "month_number": "1",
                "month_name": "January",
                "year_month": "2025-01",
                "week_of_year": "5",
                "day_of_week": "Wednesday" if day == 29 else "Thursday" if day == 30 else "Friday",
                "is_weekend": "N",
                "is_holiday": "N",
                "is_working_day": "Y",
                "financial_year": "FY2024-25",
                "fiscal_quarter": "Q4",
                "fiscal_month_number": "10",
                "is_month_end": "Y" if day == 31 else "N",
                "month_start_date": "2025-01-01",
                "month_end_date": "2025-01-31",
            }
        )
    rows = {
        "dim_date": dates,
        "dim_department": [
            {"department_id": "D01", "department_name": "Engineering", "cost_center": "CC-100", "description": "Engineering team", "is_leadership": "N"},
            {"department_id": "D02", "department_name": "Product", "cost_center": "CC-200", "description": "Product team", "is_leadership": "N"},
        ],
        "dim_leave_type": [
            {"leave_type_code": "CL", "leave_type_name": "Casual Leave", "pay_type": "Paid", "annual_quota_days": "12", "prorated_for_joiners": "Y", "carry_forward_allowed": "N", "max_carry_forward_days": "0", "policy_note": "Short notice"}
        ],
        "dim_location": [
            {"work_location": "Pune", "state": "Maharashtra", "city_tier": "Tier 1", "tier_number": "1"}
        ],
        "dim_role": [
            {"role_id": "R002", "department_id": "D01", "role_title": "Engineer", "level": "2", "grade": "L2", "band_min_annual": "720000", "band_mid_annual": "900000", "band_max_annual": "1170000", "overtime_eligible": "N", "band_reference_year": "2026"}
        ],
        "dim_employee": [
            {"employee_key": "100000", "employee_id": "EMP-0001", "full_name": "Ada Sample", "gender": "Female", "date_of_birth": "1990-01-01", "hire_date": "2020-01-01", "exit_date": "", "employment_status": "Active", "employment_type": "Permanent", "department_id": "D01", "role_id": "R002", "role_title": "Engineer", "job_level": "2", "manager_id": "", "work_location": "Pune", "work_mode": "Hybrid", "highest_education": "B.E.", "valid_from": "2020-01-01", "valid_to": "9999-12-31 00:00:00", "is_current": "Y", "change_reason": "Joining"}
        ],
        "fact_attendance_monthly": [
            {"attendance_fact_id": "FA-1", "employee_id": "EMP-0001", "date_key": "20250131", "employee_key": "100000", "department_id": "D01", "scheduled_days": "23", "present_days": "22", "wfh_days": "10", "half_days_unpaid": "0", "paid_leave_days": "1", "unpaid_absence_days": "0", "late_arrivals": "1", "avg_hours_worked": "8.5", "overtime_hours": "0", "attendance_rate": "0.9565", "period_end_date": "2025-01-31"}
        ],
        "fact_leave": [
            {"leave_request_id": "LR-1", "employee_id": "EMP-0001", "employee_key": "100000", "leave_type_code": "CL", "start_date_key": "20250130", "end_date_key": "20250131", "start_date": "2025-01-30", "end_date": "2025-01-31", "days": "2", "status": "Approved", "applied_on": "2025-01-29", "decided_on": "2025-01-30", "approver_id": "", "reason": "Personal appointment"}
        ],
        "fact_payroll": [
            {"payroll_item_id": "PI-1", "run_id": "PR-2025-01", "employee_id": "EMP-0001", "date_key": "20250131", "employee_key": "100000", "period_end_date": "2025-01-31", "department_id": "D01", "role_id": "R002", "payable_days": "31", "lop_days": "0", "overtime_hours_paid": "0", "variable_pay_type": "", "monthly_fixed_gross": "75000", "basic": "30000", "hra": "15000", "conveyance": "1600", "medical": "1250", "special_allowance": "27150", "earned_fixed_pay": "75000", "overtime_pay": "0", "variable_pay": "0", "gross_pay": "75000", "pf_employee": "1800", "esi_employee": "0", "professional_tax": "200", "tds": "5000", "other_deductions": "0", "total_deductions": "7000", "net_pay": "68000", "pf_employer": "1800", "esi_employer": "0", "employer_cost": "76800", "headcount_flag": "1"}
        ],
    }
    return {name: pd.DataFrame(values) for name, values in rows.items()}


@pytest.fixture(scope="session")
def etl_module(tmp_path_factory: pytest.TempPathFactory):
    isolated = tmp_path_factory.mktemp("etl-import")
    env_values = {
        "RAW_DIR": isolated / "raw",
        "CLEAN_DIR": isolated / "clean",
        "REJECTED_DIR": isolated / "rejected",
        "LOG_DIR": isolated / "logs",
        "OUTPUT_FORMAT": "csv",
        "AS_OF_DATE": "2026-10-12",
    }
    previous = {key: os.environ.get(key) for key in env_values}
    os.environ.update({key: str(value) for key, value in env_values.items()})
    module_name = f"pulseiq_etl_test_{uuid4().hex}"
    spec = importlib.util.spec_from_file_location(module_name, ETL_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    try:
        yield module
    finally:
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        sys.modules.pop(module_name, None)


@pytest.fixture

def synthetic_frames() -> dict[str, pd.DataFrame]:
    return build_frames()


def write_inputs(raw_dir: Path, frames: dict[str, pd.DataFrame]) -> dict[str, bytes]:
    raw_dir.mkdir(parents=True, exist_ok=True)
    source_bytes = {}
    for dataset, frame in frames.items():
        path = raw_dir / f"{dataset}.csv"
        frame.to_csv(path, index=False)
        source_bytes[dataset] = path.read_bytes()
    return source_bytes


def run_pipeline(
    raw_dir: Path,
    output_dir: Path,
    *,
    output_format: str = "csv",
    as_of_date: str = "2026-10-12",
    clear_config: bool = False,
) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    for key in ("RAW_DIR", "CLEAN_DIR", "REJECTED_DIR", "LOG_DIR", "OUTPUT_FORMAT", "AS_OF_DATE"):
        env.pop(key, None)
    if not clear_config:
        env.update(
            {
                "RAW_DIR": str(raw_dir),
                "CLEAN_DIR": str(output_dir / "clean"),
                "REJECTED_DIR": str(output_dir / "rejected"),
                "LOG_DIR": str(output_dir / "logs"),
                "OUTPUT_FORMAT": output_format,
                "AS_OF_DATE": as_of_date,
            }
        )
    return subprocess.run(
        [sys.executable, str(ETL_PATH)],
        cwd=output_dir if clear_config else REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )


def read_summary(output_dir: Path) -> dict:
    summaries = sorted(
        (output_dir / "logs").glob("run_summary_*.json"),
        key=lambda path: path.stat().st_mtime,
    )
    assert summaries
    return json.loads(summaries[-1].read_text(encoding="utf-8"))


def test_extracts_all_nine_star_schema_tables_and_preserves_sources(tmp_path, synthetic_frames) -> None:
    raw_dir = tmp_path / "raw"
    source_bytes = write_inputs(raw_dir, synthetic_frames)
    output_dir = tmp_path / "out"

    result = run_pipeline(raw_dir, output_dir)

    assert result.returncode == 0, result.stdout + result.stderr
    summary = read_summary(output_dir)
    assert summary["status"] == "success"
    assert tuple(summary["datasets"]) == DATASETS
    for dataset, frame in synthetic_frames.items():
        clean_path = output_dir / "clean" / f"{dataset}.csv"
        assert clean_path.is_file()
        assert len(pd.read_csv(clean_path)) == len(frame)
        assert (raw_dir / f"{dataset}.csv").read_bytes() == source_bytes[dataset]
    assert not list((output_dir / "rejected").glob("*.csv"))


def test_missing_dataset_file_fails_run_and_reports_missing_name(tmp_path, synthetic_frames) -> None:
    del synthetic_frames["fact_payroll"]
    write_inputs(tmp_path / "raw", synthetic_frames)

    result = run_pipeline(tmp_path / "raw", tmp_path / "out")

    assert result.returncode == 1
    summary = read_summary(tmp_path / "out")
    assert summary["status"] == "failed"
    assert "fact_payroll" in summary["error"]


def test_empty_csv_is_dataset_failure(tmp_path, synthetic_frames) -> None:
    raw_dir = tmp_path / "raw"
    write_inputs(raw_dir, synthetic_frames)
    (raw_dir / "dim_date.csv").write_text("", encoding="utf-8")

    result = run_pipeline(raw_dir, tmp_path / "out")

    assert result.returncode == 1
    assert "EmptyDataError" in read_summary(tmp_path / "out")["error"]


def test_schema_standardization_trims_values_without_case_mutation(etl_module, synthetic_frames) -> None:
    department = synthetic_frames["dim_department"].iloc[[0]].copy()
    department.columns = [f" {name.upper().replace('_', ' ')} " for name in department.columns]
    department.iloc[0, department.columns.get_loc(" DEPARTMENT NAME ")] = "  Engineering  "

    clean, rejected, _ = etl_module.clean_dataset("dim_department", department)

    assert clean.columns.tolist() == list(etl_module.DATASET_COLUMNS["dim_department"])
    assert clean.loc[0, "department_name"] == "Engineering"
    assert rejected.empty


def test_valid_bad_and_missing_optional_dates(etl_module, synthetic_frames) -> None:
    employee = synthetic_frames["dim_employee"].copy()
    malformed = employee.iloc[0].copy()
    malformed["employee_key"] = "100001"
    malformed["employee_id"] = "EMP-0002"
    malformed["hire_date"] = "not-a-date"
    impossible = employee.iloc[0].copy()
    impossible["employee_key"] = "100003"
    impossible["employee_id"] = "EMP-0004"
    impossible["hire_date"] = "2026-02-30"
    missing_optional = employee.iloc[0].copy()
    missing_optional["employee_key"] = "100002"
    missing_optional["employee_id"] = "EMP-0003"
    employee = pd.concat([employee, pd.DataFrame([malformed, impossible, missing_optional])], ignore_index=True)

    clean, rejected, stats = etl_module.clean_dataset("dim_employee", employee)

    assert clean["employee_id"].tolist() == ["EMP-0001", "EMP-0003"]
    assert pd.isna(clean.iloc[1]["exit_date"])
    assert clean.iloc[0]["valid_to"] == datetime(9999, 12, 31)
    assert rejected["employee_id"].tolist() == ["EMP-0002", "EMP-0004"]
    assert rejected["hire_date"].tolist() == ["not-a-date", "2026-02-30"]
    assert rejected["rejection_reason"].tolist() == [
        "unparseable date value: hire_date",
        "unparseable date value: hire_date",
    ]
    assert stats["input_rows"] == stats["duplicates_removed"] + stats["output_rows"] + stats["rejected_rows"]


def test_exact_duplicates_removed_but_distinct_scd_employee_id_is_valid(etl_module, synthetic_frames) -> None:
    departments = pd.concat([synthetic_frames["dim_department"].iloc[[0]]] * 2, ignore_index=True)
    clean, rejected, stats = etl_module.clean_dataset("dim_department", departments)
    assert len(clean) == 1
    assert rejected.empty
    assert stats["duplicates_removed"] == 1

    employees = synthetic_frames["dim_employee"].copy()
    historical = employees.iloc[0].copy()
    historical["employee_key"] = "100099"
    historical["valid_to"] = "2024-12-31"
    historical["is_current"] = "N"
    employees = pd.concat([employees, pd.DataFrame([historical])], ignore_index=True)
    accepted, rejected, _ = etl_module.clean_dataset("dim_employee", employees)
    etl_module.validate_dataset("dim_employee", accepted)
    assert accepted["employee_id"].tolist() == ["EMP-0001", "EMP-0001"]
    assert rejected.empty


def test_duplicate_primary_key_is_dataset_validation_failure(etl_module, synthetic_frames) -> None:
    departments = pd.concat([synthetic_frames["dim_department"].iloc[[0]]] * 2, ignore_index=True)
    departments.loc[1, "department_name"] = "Different label, same key"

    clean, rejected, _ = etl_module.clean_dataset("dim_department", departments)

    with pytest.raises(ValueError, match="duplicate department_id"):
        etl_module.validate_dataset("dim_department", clean)
    assert rejected.empty


def test_negative_payroll_amount_is_rejected_with_source_value(etl_module, synthetic_frames) -> None:
    payroll = synthetic_frames["fact_payroll"].copy()
    payroll.loc[0, "gross_pay"] = "-75000"

    clean, rejected, stats = etl_module.clean_dataset("fact_payroll", payroll)

    assert clean.empty
    assert rejected.iloc[0]["gross_pay"] == "-75000"
    assert "negative value: gross_pay" in rejected.iloc[0]["rejection_reason"]
    assert stats["input_rows"] == stats["output_rows"] + stats["rejected_rows"]


def test_missing_required_primary_key_is_row_rejection(etl_module, synthetic_frames) -> None:
    departments = synthetic_frames["dim_department"].copy()
    departments.loc[0, "department_id"] = None

    clean, rejected, _ = etl_module.clean_dataset("dim_department", departments)

    assert clean["department_id"].tolist() == ["D02"]
    assert rejected["department_name"].tolist() == ["Engineering"]
    assert pd.isna(rejected.iloc[0]["department_id"])
    assert "missing required value: department_id" in rejected.iloc[0]["rejection_reason"]


def test_reversed_leave_dates_are_rejected(etl_module, synthetic_frames) -> None:
    leave = synthetic_frames["fact_leave"].copy()
    leave.loc[0, "start_date"] = "2025-02-01"
    leave.loc[0, "end_date"] = "2025-01-31"

    clean, rejected, _ = etl_module.clean_dataset("fact_leave", leave)

    assert clean.empty
    assert rejected.iloc[0]["rejection_reason"] == "start_date is after end_date"


def test_tenure_and_calendar_fields_use_fixed_values(etl_module, synthetic_frames, monkeypatch) -> None:
    monkeypatch.setattr(etl_module, "AS_OF_DATE", pd.Timestamp("2026-01-01"))
    employees, _, _ = etl_module.clean_dataset("dim_employee", synthetic_frames["dim_employee"])
    dates, _, _ = etl_module.clean_dataset("dim_date", synthetic_frames["dim_date"])

    assert employees.loc[0, "tenure_years"] == 6.0
    assert dates["year"].tolist() == [2025, 2025, 2025]
    assert dates["month_number"].tolist() == [1, 1, 1]


def test_unknown_employee_key_sentinel_rejects_actual_payroll_ids(tmp_path, synthetic_frames) -> None:
    payroll = synthetic_frames["fact_payroll"].copy()
    orphan_rows = []
    for payroll_item_id, employee_id in (("PI-X0003", "EMP-9001"), ("PI-X0004", "EMP-9002")):
        orphan = payroll.iloc[0].copy()
        orphan["payroll_item_id"] = payroll_item_id
        orphan["employee_id"] = employee_id
        orphan["employee_key"] = "-1"
        orphan_rows.append(orphan)
    synthetic_frames["fact_payroll"] = pd.concat([payroll, pd.DataFrame(orphan_rows)], ignore_index=True)
    write_inputs(tmp_path / "raw", synthetic_frames)

    result = run_pipeline(tmp_path / "raw", tmp_path / "out")

    assert result.returncode == 0, result.stdout + result.stderr
    clean = pd.read_csv(tmp_path / "out" / "clean" / "fact_payroll.csv", dtype={"employee_key": "string"})
    rejected = pd.read_csv(tmp_path / "out" / "rejected" / "fact_payroll_rejected.csv", dtype={"employee_key": "string"})
    assert clean["payroll_item_id"].tolist() == ["PI-1"]
    assert rejected["payroll_item_id"].tolist() == ["PI-X0003", "PI-X0004"]
    assert rejected["employee_id"].tolist() == ["EMP-9001", "EMP-9002"]
    assert rejected["employee_key"].astype(str).tolist() == ["-1", "-1"]
    assert rejected["rejection_reason"].tolist() == [
        "unknown employee_key reference",
        "unknown employee_key reference",
    ]
    stats = read_summary(tmp_path / "out")["datasets"]["fact_payroll"]
    assert stats["input_rows"] == stats["duplicates_removed"] + stats["output_rows"] + stats["rejected_rows"]


def test_fact_employee_id_must_match_historical_employee_key(etl_module, synthetic_frames) -> None:
    payroll = synthetic_frames["fact_payroll"].copy()
    payroll.loc[0, "employee_id"] = "EMP-OTHER"
    clean, rejected, stats = etl_module.clean_dataset("fact_payroll", payroll)
    processed = {"dim_employee": synthetic_frames["dim_employee"], "dim_date": synthetic_frames["dim_date"], "dim_department": synthetic_frames["dim_department"], "dim_role": synthetic_frames["dim_role"]}

    accepted, rejected = etl_module.apply_relationship_checks("fact_payroll", clean, rejected, stats, processed)

    assert accepted.empty
    assert "employee_id does not match employee_key" in rejected.iloc[0]["rejection_reason"]


def test_rejected_output_retains_reasons_and_rows_reconcile(tmp_path, synthetic_frames) -> None:
    leave = synthetic_frames["fact_leave"].copy()
    invalid = leave.iloc[0].copy()
    invalid["leave_request_id"] = "LR-BAD"
    invalid["start_date"] = "2025-02-01"
    leave = pd.concat([leave, pd.DataFrame([invalid])], ignore_index=True)
    synthetic_frames["fact_leave"] = leave
    write_inputs(tmp_path / "raw", synthetic_frames)

    result = run_pipeline(tmp_path / "raw", tmp_path / "out")

    assert result.returncode == 0, result.stdout + result.stderr
    rejected = pd.read_csv(tmp_path / "out" / "rejected" / "fact_leave_rejected.csv")
    assert rejected["leave_request_id"].tolist() == ["LR-BAD"]
    assert rejected.iloc[0]["start_date"] == "2025-02-01"
    assert "start_date is after end_date" in rejected.iloc[0]["rejection_reason"]
    summary = read_summary(tmp_path / "out")
    for dataset, stats in summary["datasets"].items():
        assert stats["input_rows"] == stats["duplicates_removed"] + stats["output_rows"] + stats["rejected_rows"], dataset


def test_repeated_runs_are_deterministic(tmp_path, synthetic_frames) -> None:
    write_inputs(tmp_path / "raw", synthetic_frames)
    first = run_pipeline(tmp_path / "raw", tmp_path / "first")
    second = run_pipeline(tmp_path / "raw", tmp_path / "second")

    assert first.returncode == second.returncode == 0, first.stdout + first.stderr + second.stdout + second.stderr
    for dataset in DATASETS:
        left = pd.read_csv(tmp_path / "first" / "clean" / f"{dataset}.csv")
        right = pd.read_csv(tmp_path / "second" / "clean" / f"{dataset}.csv")
        pdt.assert_frame_equal(left, right, check_dtype=False)


@pytest.mark.parametrize("output_format,suffix", [("csv", ".csv"), ("parquet", ".parquet")])
def test_writes_all_datasets_in_configured_format(tmp_path, synthetic_frames, output_format, suffix) -> None:
    write_inputs(tmp_path / "raw", synthetic_frames)

    result = run_pipeline(tmp_path / "raw", tmp_path / "out", output_format=output_format)

    assert result.returncode == 0, result.stdout + result.stderr
    for dataset in DATASETS:
        path = tmp_path / "out" / "clean" / f"{dataset}{suffix}"
        assert path.is_file()
        loaded = pd.read_csv(path) if suffix == ".csv" else pd.read_parquet(path)
        assert len(loaded) == len(synthetic_frames[dataset])


def test_clears_previous_rejected_file_when_latest_run_has_no_rejections(tmp_path, synthetic_frames) -> None:
    invalid_frames = {dataset: frame.copy() for dataset, frame in synthetic_frames.items()}
    invalid_frames["fact_leave"].loc[0, "start_date"] = "2025-02-01"
    invalid_frames["fact_leave"].loc[0, "end_date"] = "2025-01-31"
    raw_dir = tmp_path / "raw"
    output_dir = tmp_path / "out"
    write_inputs(raw_dir, invalid_frames)

    first = run_pipeline(raw_dir, output_dir)
    rejected_path = output_dir / "rejected" / "fact_leave_rejected.csv"
    assert first.returncode == 0, first.stdout + first.stderr
    assert rejected_path.is_file()

    write_inputs(raw_dir, synthetic_frames)
    second = run_pipeline(raw_dir, output_dir)

    assert second.returncode == 0, second.stdout + second.stderr
    assert not rejected_path.exists()
    leave_stats = read_summary(output_dir)["datasets"]["fact_leave"]
    assert leave_stats["rejected_rows"] == 0
    assert leave_stats["rejected_file"] is None


def test_unsupported_output_format_fails_with_configuration_error(tmp_path, synthetic_frames) -> None:
    write_inputs(tmp_path / "raw", synthetic_frames)

    result = run_pipeline(tmp_path / "raw", tmp_path / "out", output_format="json")

    assert result.returncode == 1
    summary = read_summary(tmp_path / "out")
    assert summary["status"] == "failed"
    assert summary["error"] == "ValueError: Invalid OUTPUT_FORMAT 'json'; expected 'csv' or 'parquet'"


def test_pandera_amount_validation_has_actionable_error(etl_module, synthetic_frames) -> None:
    payroll = synthetic_frames["fact_payroll"].copy()
    payroll["gross_pay"] = -1.0
    with pytest.raises(pa.errors.SchemaErrors, match="gross_pay|greater|ge"):
        etl_module.validate_dataset("fact_payroll", payroll)


def test_unknown_or_missing_columns_are_dataset_failures(etl_module, synthetic_frames) -> None:
    department = synthetic_frames["dim_department"].drop(columns="description")
    with pytest.raises(ValueError, match="missing columns=.*description"):
        etl_module.clean_dataset("dim_department", department)
    department = synthetic_frames["dim_department"].assign(unexpected="value")
    with pytest.raises(ValueError, match="unexpected columns=.*unexpected"):
        etl_module.clean_dataset("dim_department", department)


def test_missing_configuration_uses_default_relative_paths(tmp_path, synthetic_frames) -> None:
    raw_dir = tmp_path / "data" / "raw"
    write_inputs(raw_dir, synthetic_frames)

    result = run_pipeline(tmp_path / "unused", tmp_path, clear_config=True)

    assert result.returncode == 0, result.stdout + result.stderr
    assert all((tmp_path / "data" / "clean" / f"{dataset}.csv").is_file() for dataset in DATASETS)
    assert read_summary(tmp_path)["status"] == "success"


def test_missing_source_inventory_returns_nonzero(tmp_path) -> None:
    (tmp_path / "raw").mkdir()
    result = run_pipeline(tmp_path / "raw", tmp_path / "out")

    assert result.returncode == 1
    assert "Input inventory mismatch" in read_summary(tmp_path / "out")["error"]


def test_nine_table_contract_declares_primary_keys_and_exact_schemas(etl_module) -> None:
    assert etl_module.DATASETS == DATASETS
    assert set(etl_module.DATASET_COLUMNS) == set(DATASETS)
    for dataset in DATASETS:
        assert etl_module.PRIMARY_KEYS[dataset] in etl_module.DATASET_COLUMNS[dataset]
        assert len(etl_module.DATASET_COLUMNS[dataset]) == len(set(etl_module.DATASET_COLUMNS[dataset]))


def test_duplicate_payroll_business_key_is_row_rejection(etl_module, synthetic_frames) -> None:
    payroll = synthetic_frames["fact_payroll"].copy()
    duplicate_item = payroll.iloc[0].copy()
    duplicate_item["payroll_item_id"] = "PI-DUP"
    duplicate_item["gross_pay"] = "99999"
    combined = pd.concat([payroll, pd.DataFrame([duplicate_item])], ignore_index=True)

    clean, rejected, stats = etl_module.clean_dataset("fact_payroll", combined)

    assert len(clean) == 1
    assert clean.iloc[0]["payroll_item_id"] == "PI-1"
    assert len(rejected) == 1
    assert rejected.iloc[0]["payroll_item_id"] == "PI-DUP"
    assert "duplicate (employee_id, run_id)" in rejected.iloc[0]["rejection_reason"]
    assert stats["input_rows"] == 2
    assert stats["output_rows"] == 1
    assert stats["rejected_rows"] == 1


def test_attendance_after_exit_month_is_rejected(etl_module, synthetic_frames) -> None:
    employees = synthetic_frames["dim_employee"].copy()
    # EMP-0001 exited in January 2025
    employees.loc[0, "exit_date"] = "2025-01-15"

    attendance = synthetic_frames["fact_attendance_monthly"].copy()
    late_att = attendance.iloc[0].copy()
    late_att["attendance_fact_id"] = "FA-LATE"
    late_att["date_key"] = "20250228"
    late_att["period_end_date"] = "2025-02-28"

    feb_date = pd.DataFrame([{
        "date": "2025-02-28", "date_key": "20250228", "year": "2025", "quarter": "1",
        "month_number": "2", "month_name": "February", "year_month": "2025-02",
        "week_of_year": "9", "day_of_week": "Friday", "is_weekend": "N", "is_holiday": "N",
        "is_working_day": "Y", "financial_year": "FY2024-25", "fiscal_quarter": "Q4",
        "fiscal_month_number": "11", "is_month_end": "Y", "month_start_date": "2025-02-01",
        "month_end_date": "2025-02-28"
    }])
    dates = pd.concat([synthetic_frames["dim_date"], feb_date], ignore_index=True)

    clean, rejected, stats = etl_module.clean_dataset("fact_attendance_monthly", pd.DataFrame([late_att]))
    processed = {
        "dim_employee": employees,
        "dim_date": dates,
        "dim_department": synthetic_frames["dim_department"],
    }
    clean, rejected = etl_module.apply_relationship_checks("fact_attendance_monthly", clean, rejected, stats, processed)


    assert clean.empty
    assert len(rejected) == 1
    assert rejected.iloc[0]["attendance_fact_id"] == "FA-LATE"
    assert "attendance recorded after employee exit month" in rejected.iloc[0]["rejection_reason"]


def test_payroll_duplicate_business_key_end_to_end_pipeline(tmp_path, synthetic_frames) -> None:
    """End-to-end regression: duplicate (employee_id, run_id) with a different payroll_item_id
    must be rejected through the full ETL pipeline (mimics the PI-X0001 / PI-X0002 pattern).
    """
    frames = {dataset: frame.copy() for dataset, frame in synthetic_frames.items()}
    payroll = frames["fact_payroll"].copy()

    # First duplicate: same (employee_id, run_id) as PI-1 but a different payroll_item_id.
    dup1 = payroll.iloc[0].copy()
    dup1["payroll_item_id"] = "PI-XDUP1"
    dup1["gross_pay"] = "99999"  # differs, so it is NOT an exact-row duplicate

    # Second duplicate pair: same employee, a different run duplicated.
    original2 = payroll.iloc[0].copy()
    original2["payroll_item_id"] = "PI-ORIG2"
    original2["run_id"] = "PR-2025-02"  # distinct run from PI-1

    dup2 = original2.copy()
    dup2["payroll_item_id"] = "PI-XDUP2"
    dup2["gross_pay"] = "88888"

    frames["fact_payroll"] = pd.concat(
        [payroll, pd.DataFrame([dup1, original2, dup2])], ignore_index=True
    )
    write_inputs(tmp_path / "raw", frames)

    result = run_pipeline(tmp_path / "raw", tmp_path / "out")

    assert result.returncode == 0, result.stdout + result.stderr
    clean = pd.read_csv(tmp_path / "out" / "clean" / "fact_payroll.csv", dtype=str)
    rejected = pd.read_csv(tmp_path / "out" / "rejected" / "fact_payroll_rejected.csv", dtype=str)

    clean_ids = clean["payroll_item_id"].tolist()
    rejected_ids = rejected["payroll_item_id"].tolist()

    # Originals survive in clean output.
    assert "PI-1" in clean_ids
    assert "PI-ORIG2" in clean_ids

    # Duplicates are absent from clean output.
    assert "PI-XDUP1" not in clean_ids
    assert "PI-XDUP2" not in clean_ids

    # Duplicates appear in rejected output with the correct reason.
    assert "PI-XDUP1" in rejected_ids
    assert "PI-XDUP2" in rejected_ids
    for dup_id in ("PI-XDUP1", "PI-XDUP2"):
        reason = rejected.loc[rejected["payroll_item_id"] == dup_id, "rejection_reason"].values[0]
        assert "duplicate (employee_id, run_id) payroll record" in reason, (
            f"{dup_id}: unexpected rejection reason: {reason!r}"
        )

    # No duplicate (employee_id, run_id) remain in the clean output.
    dup_key_mask = clean.duplicated(subset=["employee_id", "run_id"], keep=False)
    assert not dup_key_mask.any(), (
        "Duplicate business keys still in clean: "
        + str(clean.loc[dup_key_mask, ["payroll_item_id", "employee_id", "run_id"]].to_dict("records"))
    )

    # Row-count reconciliation: raw = duplicates_removed + clean + rejected.
    stats = read_summary(tmp_path / "out")["datasets"]["fact_payroll"]
    assert stats["input_rows"] == stats["duplicates_removed"] + stats["output_rows"] + stats["rejected_rows"]


def test_actual_payroll_outputs_reject_pi_x001_and_pi_x002() -> None:
    """Regression: after ETL execution the real output files must reject PI-X0001 and PI-X0002
    as duplicate-business-key records while keeping PI-004059 and PI-000761 in the clean output.
    PI-X0003 and PI-X0004 must remain rejected for unknown employee_key reference.
    """
    clean_path = REPO_ROOT / "data" / "clean" / "fact_payroll.csv"
    rejected_path = REPO_ROOT / "rejected" / "fact_payroll_rejected.csv"
    assert clean_path.is_file(), (
        f"Clean payroll CSV not found at {clean_path}. "
        "Run the ETL pipeline first: docker compose -f compose.yaml run --rm etl"
    )
    assert rejected_path.is_file(), (
        f"Rejected payroll CSV not found at {rejected_path}. "
        "Run the ETL pipeline first."
    )

    clean = pd.read_csv(clean_path, dtype=str)
    rejected = pd.read_csv(rejected_path, dtype=str)

    clean_ids = set(clean["payroll_item_id"].tolist())
    rejected_ids = set(rejected["payroll_item_id"].tolist())

    # Duplicate-pair extras must NOT be in the clean output.
    assert "PI-X0001" not in clean_ids, "PI-X0001 (duplicate of PI-004059) must not be in clean output"
    assert "PI-X0002" not in clean_ids, "PI-X0002 (duplicate of PI-000761) must not be in clean output"

    # Original records of those pairs must be in the clean output.
    assert "PI-004059" in clean_ids, "PI-004059 (original) must be in clean output"
    assert "PI-000761" in clean_ids, "PI-000761 (original) must be in clean output"

    # No duplicate (employee_id, run_id) keys in clean output.
    dup_mask = clean.duplicated(subset=["employee_id", "run_id"], keep=False)
    assert not dup_mask.any(), (
        "Duplicate (employee_id, run_id) keys found in clean CSV: "
        + str(clean.loc[dup_mask, ["payroll_item_id", "employee_id", "run_id"]].to_dict("records"))
    )

    # PI-X0001 and PI-X0002 must appear in rejected output with the duplicate reason.
    assert "PI-X0001" in rejected_ids, "PI-X0001 must appear in rejected output"
    assert "PI-X0002" in rejected_ids, "PI-X0002 must appear in rejected output"
    for pid in ("PI-X0001", "PI-X0002"):
        reason = rejected.loc[rejected["payroll_item_id"] == pid, "rejection_reason"].values[0]
        assert "duplicate (employee_id, run_id) payroll record" in reason, (
            f"{pid}: expected duplicate rejection reason, got: {reason!r}"
        )

    # PI-X0003 and PI-X0004 must remain rejected for unknown employee_key reference.
    assert "PI-X0003" in rejected_ids, "PI-X0003 must appear in rejected output"
    assert "PI-X0004" in rejected_ids, "PI-X0004 must appear in rejected output"
    for pid in ("PI-X0003", "PI-X0004"):
        reason = rejected.loc[rejected["payroll_item_id"] == pid, "rejection_reason"].values[0]
        assert "unknown employee_key reference" in reason, (
            f"{pid}: expected unknown employee_key rejection, got: {reason!r}"
        )
