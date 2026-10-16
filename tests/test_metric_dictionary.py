from __future__ import annotations

import csv
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
DICTIONARY_PATH = REPO_ROOT / "metric_dictionary.csv"
EXPECTED_COLUMNS = [
    "metric_id",
    "metric_name",
    "business_definition",
    "formula",
    "source_table",
    "grain",
    "time_period",
    "owner_team",
    "unit",
    "good_direction",
    "caveat",
    "minimum_group_size",
]
EXPECTED_METRIC_IDS = {f"M{i:02d}" for i in range(1, 29)}


def load_metrics_by_id() -> dict[str, dict[str, str]]:
    with DICTIONARY_PATH.open(newline="", encoding="utf-8") as dictionary_file:
        rows = list(csv.DictReader(dictionary_file))
    return {row["metric_id"]: row for row in rows}


def load_metrics_by_name() -> dict[str, dict[str, str]]:
    with DICTIONARY_PATH.open(newline="", encoding="utf-8") as dictionary_file:
        rows = list(csv.DictReader(dictionary_file))
    return {row["metric_name"]: row for row in rows}


def test_dictionary_has_all_28_official_metrics_and_declared_columns() -> None:
    with DICTIONARY_PATH.open(newline="", encoding="utf-8") as dictionary_file:
        reader = csv.DictReader(dictionary_file)
        assert reader.fieldnames == EXPECTED_COLUMNS
        rows = list(reader)

    assert len(rows) == 28
    assert {row["metric_id"] for row in rows} == EXPECTED_METRIC_IDS
    for row in rows:
        # Mandatory fields must not be empty
        for key in (
            "metric_id", "metric_name", "business_definition", "formula",
            "source_table", "grain", "time_period", "owner_team", "unit",
            "good_direction", "minimum_group_size"
        ):
            assert row[key].strip() != "", f"Empty field {key} in metric {row['metric_id']}"


def test_headcount_and_attrition_metric_definitions() -> None:
    metrics = load_metrics_by_id()

    headcount = metrics["M01"]
    assert headcount["metric_name"] == "Headcount"
    assert "COUNT(DISTINCT employee_id)" in headcount["formula"]
    assert "hire_date <= month_end" in headcount["formula"]
    assert "dim_employee" in headcount["source_table"]

    hires = metrics["M02"]
    assert hires["metric_name"] == "Hires"
    assert "Transfers are not hires" in hires["caveat"]

    attrition = metrics["M04"]
    assert attrition["metric_name"] == "Attrition rate (monthly)"
    assert "Exits / headcount_start" in attrition["formula"]


def test_payroll_and_finance_metric_definitions() -> None:
    metrics = load_metrics_by_id()

    gross_pay = metrics["M07"]
    assert gross_pay["metric_name"] == "Gross pay"
    assert gross_pay["formula"] == "SUM(gross_pay)"
    assert gross_pay["source_table"] == "fact_payroll"

    net_pay = metrics["M08"]
    assert net_pay["metric_name"] == "Net pay"
    assert net_pay["formula"] == "SUM(net_pay)"

    employer_cost = metrics["M09"]
    assert employer_cost["metric_name"] == "Payroll cost (employer cost)"
    assert "5001-5005" in employer_cost["caveat"]


def test_attendance_and_productivity_metric_definitions() -> None:
    metrics = load_metrics_by_id()

    attendance = metrics["M13"]
    assert attendance["metric_name"] == "Attendance rate"
    assert "SUM(present_days)" in attendance["formula"]
    assert "SUM(scheduled_days)" in attendance["formula"]
    assert attendance["unit"] == "ratio"

    task_completion = metrics["M22"]
    assert task_completion["metric_name"] == "Task completion rate"
    assert "Done" in task_completion["formula"]

