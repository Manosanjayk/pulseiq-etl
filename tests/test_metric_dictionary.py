from __future__ import annotations

import csv
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
DICTIONARY_PATH = REPO_ROOT / "metric_dictionary.csv"
EXPECTED_METRICS = {
    "active_employee_count",
    "payroll_headcount",
    "payroll_total",
    "attendance_rate",
    "approved_leave_days",
}


def load_metrics() -> dict[str, dict[str, str]]:
    with DICTIONARY_PATH.open(newline="", encoding="utf-8") as dictionary_file:
        rows = list(csv.DictReader(dictionary_file))
    return {row["metric_name"]: row for row in rows}


def test_dictionary_has_exact_current_metric_set_and_complete_columns() -> None:
    with DICTIONARY_PATH.open(newline="", encoding="utf-8") as dictionary_file:
        reader = csv.DictReader(dictionary_file)
        assert reader.fieldnames == ["metric_name", "definition", "grain", "calculation", "source", "notes"]
        rows = list(reader)

    assert {row["metric_name"] for row in rows} == EXPECTED_METRICS
    assert all(all(value.strip() for value in row.values()) for row in rows)


def test_active_employee_definition_is_as_of_and_scd_aware() -> None:
    metric = load_metrics()["active_employee_count"]
    calculation = metric["calculation"]

    assert metric["source"] == "dim_employee; AS_OF_DATE"
    for required_rule in (
        "employment_status = Active",
        "hire_date <= AS_OF_DATE",
        "exit_date is null or exit_date >= AS_OF_DATE",
        "valid_from <= AS_OF_DATE <= valid_to",
        "Count distinct employee_id",
    ):
        assert required_rule in calculation
    assert "SCD validity" in metric["notes"]


def test_payroll_metrics_distinguish_headcount_from_gross_amount() -> None:
    metrics = load_metrics()

    assert metrics["payroll_headcount"]["calculation"] == "Sum headcount_flag over accepted payroll rows"
    assert metrics["payroll_total"]["calculation"] == "Sum gross_pay over accepted payroll rows"
    assert "run_id" in metrics["payroll_total"]["grain"]
    assert "net_pay" in metrics["payroll_total"]["notes"]


def test_attendance_formula_defines_period_and_zero_denominator_handling() -> None:
    metric = load_metrics()["attendance_rate"]

    assert metric["calculation"] == "Sum present_days / sum scheduled_days"
    assert "month" in metric["grain"]
    assert "period_end_date" in metric["grain"]
    assert "sum scheduled_days = 0" in metric["notes"]
    assert "exclude rejected rows" in metric["notes"].lower()


def test_leave_formula_defines_status_type_and_month_attribution() -> None:
    metric = load_metrics()["approved_leave_days"]

    assert metric["calculation"] == "Sum days where status = Approved"
    assert metric["source"] == "fact_leave; dim_leave_type"
    assert "start_date" in metric["grain"]
    assert "Includes paid and unpaid" in metric["notes"]
    assert "full request duration to its start month" in metric["notes"]
    assert "rejected rows are excluded" in metric["notes"].lower()
