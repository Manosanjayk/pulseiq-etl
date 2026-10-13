from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import pandera as pa
from dotenv import load_dotenv

load_dotenv()
RAW_DIR = Path(os.getenv("RAW_DIR", "data/raw"))
CLEAN_DIR = Path(os.getenv("CLEAN_DIR", "data/clean"))
REJECTED_DIR = Path(os.getenv("REJECTED_DIR", "rejected"))
LOG_DIR = Path(os.getenv("LOG_DIR", "logs"))
OUTPUT_FORMAT = os.getenv("OUTPUT_FORMAT", "csv").strip().lower()
AS_OF_DATE = pd.Timestamp(os.getenv("AS_OF_DATE", "2026-10-12")).normalize()
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
DATASET_COLUMNS = {
    "dim_date": ("date", "date_key", "year", "quarter", "month_number", "month_name", "year_month", "week_of_year", "day_of_week", "is_weekend", "is_holiday", "is_working_day", "financial_year", "fiscal_quarter", "fiscal_month_number", "is_month_end", "month_start_date", "month_end_date"),
    "dim_department": ("department_id", "department_name", "cost_center", "description", "is_leadership"),
    "dim_leave_type": ("leave_type_code", "leave_type_name", "pay_type", "annual_quota_days", "prorated_for_joiners", "carry_forward_allowed", "max_carry_forward_days", "policy_note"),
    "dim_location": ("work_location", "state", "city_tier", "tier_number"),
    "dim_role": ("role_id", "department_id", "role_title", "level", "grade", "band_min_annual", "band_mid_annual", "band_max_annual", "overtime_eligible", "band_reference_year"),
    "dim_employee": ("employee_key", "employee_id", "full_name", "gender", "date_of_birth", "hire_date", "exit_date", "employment_status", "employment_type", "department_id", "role_id", "role_title", "job_level", "manager_id", "work_location", "work_mode", "highest_education", "valid_from", "valid_to", "is_current", "change_reason"),
    "fact_attendance_monthly": ("attendance_fact_id", "employee_id", "date_key", "employee_key", "department_id", "scheduled_days", "present_days", "wfh_days", "half_days_unpaid", "paid_leave_days", "unpaid_absence_days", "late_arrivals", "avg_hours_worked", "overtime_hours", "attendance_rate", "period_end_date"),
    "fact_leave": ("leave_request_id", "employee_id", "employee_key", "leave_type_code", "start_date_key", "end_date_key", "start_date", "end_date", "days", "status", "applied_on", "decided_on", "approver_id", "reason"),
    "fact_payroll": ("payroll_item_id", "run_id", "employee_id", "date_key", "employee_key", "period_end_date", "department_id", "role_id", "payable_days", "lop_days", "overtime_hours_paid", "variable_pay_type", "monthly_fixed_gross", "basic", "hra", "conveyance", "medical", "special_allowance", "earned_fixed_pay", "overtime_pay", "variable_pay", "gross_pay", "pf_employee", "esi_employee", "professional_tax", "tds", "other_deductions", "total_deductions", "net_pay", "pf_employer", "esi_employer", "employer_cost", "headcount_flag"),
}
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
NUMERIC_COLUMNS = {
    "dim_date": ("year", "quarter", "month_number", "week_of_year", "fiscal_month_number"),
    "dim_leave_type": ("annual_quota_days", "max_carry_forward_days"),
    "dim_location": ("tier_number",),
    "dim_role": ("level", "band_min_annual", "band_mid_annual", "band_max_annual", "band_reference_year"),
    "dim_employee": ("job_level",),
    "fact_attendance_monthly": ("scheduled_days", "present_days", "wfh_days", "half_days_unpaid", "paid_leave_days", "unpaid_absence_days", "late_arrivals", "avg_hours_worked", "overtime_hours", "attendance_rate"),
    "fact_leave": ("days",),
    "fact_payroll": ("payable_days", "lop_days", "overtime_hours_paid", "monthly_fixed_gross", "basic", "hra", "conveyance", "medical", "special_allowance", "earned_fixed_pay", "overtime_pay", "variable_pay", "gross_pay", "pf_employee", "esi_employee", "professional_tax", "tds", "other_deductions", "total_deductions", "net_pay", "pf_employer", "esi_employer", "employer_cost", "headcount_flag"),
}
OPTIONAL_COLUMNS = {
    "dim_employee": ("exit_date", "manager_id"),
    "fact_leave": ("decided_on", "approver_id"),
    "fact_payroll": ("variable_pay_type",),
}
ID_COLUMNS = {
    "date_key", "department_id", "leave_type_code", "work_location", "role_id",
    "employee_key", "employee_id", "manager_id", "attendance_fact_id",
    "leave_request_id", "approver_id", "start_date_key", "end_date_key",
    "payroll_item_id", "run_id",
}
FOREIGN_KEYS = {
    "dim_role": (("department_id", "dim_department", "department_id", False),),
    "dim_employee": (
        ("department_id", "dim_department", "department_id", False),
        ("role_id", "dim_role", "role_id", False),
        ("work_location", "dim_location", "work_location", False),
        ("manager_id", "dim_employee", "employee_id", True),
    ),
    "fact_attendance_monthly": (
        ("employee_key", "dim_employee", "employee_key", False),
        ("date_key", "dim_date", "date_key", False),
        ("department_id", "dim_department", "department_id", False),
    ),
    "fact_leave": (
        ("employee_key", "dim_employee", "employee_key", False),
        ("leave_type_code", "dim_leave_type", "leave_type_code", False),
        ("start_date_key", "dim_date", "date_key", False),
        ("end_date_key", "dim_date", "date_key", False),
        ("approver_id", "dim_employee", "employee_id", True),
    ),
    "fact_payroll": (
        ("employee_key", "dim_employee", "employee_key", False),
        ("date_key", "dim_date", "date_key", False),
        ("department_id", "dim_department", "department_id", False),
        ("role_id", "dim_role", "role_id", False),
    ),
}

for directory in (CLEAN_DIR, REJECTED_DIR, LOG_DIR):
    directory.mkdir(parents=True, exist_ok=True)

run_stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
log_path = LOG_DIR / f"etl_{run_stamp}.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)sZ %(levelname)s %(message)s",
    handlers=[logging.FileHandler(log_path, encoding="utf-8"), logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("pulseiq_etl")


def find_input(dataset: str) -> Path:
    """Find the required CSV input for a dataset."""
    path = RAW_DIR / f"{dataset}.csv"
    if not path.is_file():
        raise FileNotFoundError(f"Missing input for '{dataset}' in {RAW_DIR}. Expected {path.name}")
    return path


def read_file(path: Path) -> pd.DataFrame:
    if path.suffix.lower() != ".csv":
        raise ValueError(f"Unsupported file format for PulseIQ sources: {path.suffix}")
    return pd.read_csv(path, dtype="string")


def standardise_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip().lower().replace(" ", "_").replace("-", "_") for c in df.columns]
    return df


def clean_dataset(dataset: str, raw: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    """Parse a declared PulseIQ table and return accepted, rejected, and row counts."""
    if dataset not in DATASET_COLUMNS:
        raise ValueError(f"Unknown dataset: {dataset}")
    df = standardise_columns(raw)
    input_rows = len(df)
    expected_columns = set(DATASET_COLUMNS[dataset])
    actual_columns = set(df.columns)
    missing_columns = sorted(expected_columns - actual_columns)
    unexpected_columns = sorted(actual_columns - expected_columns)
    if missing_columns or unexpected_columns:
        raise ValueError(
            f"Schema mismatch for {dataset}: missing columns={missing_columns}; "
            f"unexpected columns={unexpected_columns}"
        )

    for col in df.select_dtypes(include=["object", "string"]).columns:
        df[col] = df[col].astype("string").str.strip().replace({"": pd.NA, "nan": pd.NA, "None": pd.NA, "NULL": pd.NA})

    df = df.drop_duplicates().copy()
    duplicate_rows = input_rows - len(df)
    df = df.reset_index(drop=True)

    original_values: dict[str, pd.Series] = {}
    reasons = pd.Series("", index=df.index, dtype="string")

    def add_reason(mask: pd.Series, message: pd.Series | str) -> None:
        if isinstance(message, str):
            addition = pd.Series(message, index=df.index, dtype="string")
        else:
            addition = message.astype("string")
        reasons.loc[mask] = reasons.loc[mask].where(
            reasons.loc[mask].eq(""), reasons.loc[mask] + "; "
        ) + addition.loc[mask]

    required = [column for column in DATASET_COLUMNS[dataset] if column not in OPTIONAL_COLUMNS.get(dataset, ())]
    for col in required:
        missing = df[col].isna()
        add_reason(missing, f"missing required value: {col}")

    for col in DATE_COLUMNS.get(dataset, ()):
        original_values[col] = df[col].copy()
        parsed = pd.to_datetime(df[col], errors="coerce", format="mixed")
        invalid = df[col].notna() & parsed.isna()
        if invalid.any():
            parsed = parsed.astype("object")
            for index in df.index[invalid]:
                value = str(df.at[index, col])
                try:
                    parsed.at[index] = datetime.fromisoformat(value.replace("Z", "+00:00"))
                    invalid.at[index] = False
                except ValueError:
                    pass
        add_reason(invalid, f"unparseable date value: {col}")
        df[col] = parsed

    for col in NUMERIC_COLUMNS.get(dataset, ()):
        original_values[col] = df[col].copy()
        parsed = pd.to_numeric(df[col], errors="coerce")
        invalid = df[col].notna() & parsed.isna()
        add_reason(invalid, f"unparseable numeric value: {col}")
        df[col] = parsed
        negative = parsed.notna() & (parsed < 0)
        add_reason(negative, f"negative value: {col}")

    date_ranges = {
        "dim_date": (("month_start_date", "month_end_date", "month_start_date is after month_end_date"),),
        "dim_employee": (
            ("hire_date", "exit_date", "hire_date is after exit_date"),
            ("valid_from", "valid_to", "valid_from is after valid_to"),
        ),
        "fact_leave": (("start_date", "end_date", "start_date is after end_date"),),
    }
    for start_col, end_col, message in date_ranges.get(dataset, ()):
        invalid_range = df[start_col].notna() & df[end_col].notna() & (df[start_col] > df[end_col])
        add_reason(invalid_range, message)

    if dataset == "fact_attendance_monthly":
        invalid_rate = df["attendance_rate"].notna() & ~df["attendance_rate"].between(0, 1)
        add_reason(invalid_rate, "attendance_rate must be between 0 and 1")

    rejected_mask = reasons.ne("")
    rejected = df.loc[rejected_mask].copy()
    for col, original in original_values.items():
        if col in rejected.columns:
            rejected[col] = rejected[col].astype("object")
            rejected.loc[rejected_mask, col] = original.loc[rejected_mask]
    if not rejected.empty:
        rejected["rejection_reason"] = reasons.loc[rejected_mask]
    df = df.loc[~rejected_mask].copy()

    if dataset == "dim_employee":
        hire_date = pd.to_datetime(df["hire_date"], errors="coerce")
        exit_date = pd.to_datetime(df["exit_date"], errors="coerce").fillna(AS_OF_DATE)
        df["tenure_years"] = ((exit_date - hire_date).dt.days / 365.25).round(2).astype("Float64")

    stats = {
        "input_rows": int(input_rows),
        "duplicates_removed": int(duplicate_rows),
        "output_rows": int(len(df)),
        "rejected_rows": int(len(rejected)),
    }
    return df, rejected, stats


def validate_dataset(dataset: str, df: pd.DataFrame) -> None:
    """Validate table shape, primary key uniqueness, and typed business measures."""
    expected = set(DATASET_COLUMNS[dataset])
    if dataset == "dim_employee" and "tenure_years" in df.columns:
        expected.add("tenure_years")
    if set(df.columns) != expected:
        raise ValueError(f"Output schema mismatch for {dataset}")

    primary_key = PRIMARY_KEYS[dataset]
    if df[primary_key].duplicated().any():
        duplicate_values = df.loc[df[primary_key].duplicated(keep=False), primary_key].astype(str).unique().tolist()[:10]
        raise ValueError(f"Validation failed: duplicate {primary_key} values in {dataset}: {duplicate_values}")

    columns: dict[str, Any] = {}
    for col in ID_COLUMNS.intersection(df.columns):
        columns[col] = pa.Column(
            str,
            nullable=col in OPTIONAL_COLUMNS.get(dataset, ()),
            required=False,
            coerce=True,
        )
    for col in NUMERIC_COLUMNS.get(dataset, ()):
        if col in df.columns:
            checks = [pa.Check.ge(0)]
            if col == "attendance_rate":
                checks.append(pa.Check.le(1))
            columns[col] = pa.Column(float, checks=checks, nullable=True, required=False, coerce=True)
    schema = pa.DataFrameSchema(columns=columns, strict=False, coerce=False)
    schema.validate(df, lazy=True)


def write_frame(df: pd.DataFrame, path_without_suffix: Path) -> str:
    if OUTPUT_FORMAT == "parquet":
        path = path_without_suffix.with_suffix(".parquet")
        df.to_parquet(path, index=False)
    else:
        path = path_without_suffix.with_suffix(".csv")
        df.to_csv(path, index=False, date_format="%Y-%m-%d")
    return str(path)


def validate_configuration() -> None:
    if OUTPUT_FORMAT not in {"csv", "parquet"}:
        raise ValueError(
            f"Invalid OUTPUT_FORMAT '{OUTPUT_FORMAT}'; expected 'csv' or 'parquet'"
        )


def apply_relationship_checks(
    dataset: str,
    clean: pd.DataFrame,
    rejected: pd.DataFrame,
    stats: dict[str, Any],
    processed: dict[str, pd.DataFrame],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Reject rows with broken declared foreign keys or dimensional key mismatches."""
    reasons = pd.Series("", index=clean.index, dtype="string")

    def add_reason(mask: pd.Series, messages: pd.Series | str) -> None:
        if isinstance(messages, str):
            messages = pd.Series(messages, index=clean.index, dtype="string")
        existing = reasons.loc[mask]
        additions = messages.loc[mask]
        reasons.loc[mask] = existing.where(existing.eq(""), existing + "; ") + additions

    for column, ref_dataset, ref_column, nullable in FOREIGN_KEYS.get(dataset, ()):
        reference = clean if ref_dataset == dataset else processed.get(ref_dataset)
        if reference is None:
            raise ValueError(f"Required reference dataset was not processed: {ref_dataset}")
        valid_values = set(reference[ref_column].dropna().astype("string"))
        values = clean[column].astype("string")
        invalid = values.notna() & ~values.isin(valid_values)
        if not nullable:
            invalid |= values.isna()
        add_reason(invalid, f"unknown {column} reference")

    if dataset in {"fact_attendance_monthly", "fact_leave", "fact_payroll"}:
        employees = processed["dim_employee"].set_index("employee_key")
        known_key = clean["employee_key"].astype("string").isin(employees.index.astype("string"))
        employee_ids = clean["employee_key"].map(employees["employee_id"])
        id_mismatch = known_key & employee_ids.astype("string").ne(clean["employee_id"].astype("string"))
        add_reason(id_mismatch, "employee_id does not match employee_key")

        if dataset == "fact_attendance_monthly":
            employee_department = clean["employee_key"].map(employees["department_id"])
            add_reason(employee_department.notna() & employee_department.astype("string").ne(clean["department_id"].astype("string")), "department_id does not match employee_key")
            date_values = processed["dim_date"].set_index("date_key")["date"]
            fact_date = clean["date_key"].map(date_values)
            add_reason(fact_date.notna() & fact_date.ne(clean["period_end_date"]), "period_end_date does not match date_key")
        elif dataset == "fact_payroll":
            employee_department = clean["employee_key"].map(employees["department_id"])
            employee_role = clean["employee_key"].map(employees["role_id"])
            add_reason(employee_department.notna() & employee_department.astype("string").ne(clean["department_id"].astype("string")), "department_id does not match employee_key")
            add_reason(employee_role.notna() & employee_role.astype("string").ne(clean["role_id"].astype("string")), "role_id does not match employee_key")
            date_values = processed["dim_date"].set_index("date_key")["date"]
            fact_date = clean["date_key"].map(date_values)
            add_reason(fact_date.notna() & fact_date.ne(clean["period_end_date"]), "period_end_date does not match date_key")
        else:
            date_values = processed["dim_date"].set_index("date_key")["date"]
            start_date = clean["start_date_key"].map(date_values)
            end_date = clean["end_date_key"].map(date_values)
            add_reason(start_date.notna() & start_date.ne(clean["start_date"]), "start_date does not match start_date_key")
            add_reason(end_date.notna() & end_date.ne(clean["end_date"]), "end_date does not match end_date_key")

    rejected_mask = reasons.ne("")
    rejected_rows = clean.loc[rejected_mask].copy()
    if not rejected_rows.empty:
        rejected_rows["rejection_reason"] = reasons.loc[rejected_mask]
        rejected = rejected_rows if rejected.empty else pd.concat([rejected, rejected_rows], ignore_index=True)
    clean = clean.loc[~rejected_mask].copy()
    stats["output_rows"] = int(len(clean))
    stats["rejected_rows"] = int(len(rejected))
    return clean, rejected


def main() -> int:
    started = datetime.now(timezone.utc).isoformat()
    logger.info("ETL run started: %s", started)
    run_summary: dict[str, Any] = {"run_started_utc": started, "datasets": {}, "status": "success"}
    try:
        validate_configuration()
        actual_sources = {path.stem for path in RAW_DIR.glob("*.csv")}
        expected_sources = set(DATASETS)
        if actual_sources != expected_sources:
            raise ValueError(
                f"Input inventory mismatch in {RAW_DIR}: missing={sorted(expected_sources - actual_sources)}; "
                f"unexpected={sorted(actual_sources - expected_sources)}"
            )
        processed: dict[str, pd.DataFrame] = {}
        for dataset in DATASETS:
            path = find_input(dataset)
            raw = read_file(path)
            logger.info("Extracted %s rows from %s", len(raw), path)
            clean, rejected, stats = clean_dataset(dataset, raw)
            validate_dataset(dataset, clean)
            if dataset in FOREIGN_KEYS:
                clean, rejected = apply_relationship_checks(dataset, clean, rejected, stats, processed)
                validate_dataset(dataset, clean)
            processed[dataset] = clean
            clean_path = write_frame(clean, CLEAN_DIR / dataset)
            rejected_base = REJECTED_DIR / f"{dataset}_rejected"
            for suffix in (".csv", ".parquet"):
                rejected_output = rejected_base.with_suffix(suffix)
                if rejected_output.exists():
                    rejected_output.unlink()
            rejected_path = None
            if not rejected.empty:
                rejected_path = write_frame(rejected, rejected_base)
            stats.update({"source_file": str(path), "clean_file": clean_path, "rejected_file": rejected_path})
            run_summary["datasets"][dataset] = stats
            logger.info("Dataset %s: %s", dataset, json.dumps(stats, default=str))
        logger.info("ETL run completed successfully")
    except Exception as exc:
        run_summary["status"] = "failed"
        run_summary["error"] = f"{type(exc).__name__}: {exc}"
        logger.exception("ETL run failed: %s", exc)
    finally:
        run_summary["run_finished_utc"] = datetime.now(timezone.utc).isoformat()
        summary_path = LOG_DIR / f"run_summary_{run_stamp}.json"
        summary_path.write_text(json.dumps(run_summary, indent=2, default=str), encoding="utf-8")
        logger.info("Run summary written to %s", summary_path)
    return 0 if run_summary["status"] == "success" else 1


if __name__ == "__main__":
    raise SystemExit(main())
