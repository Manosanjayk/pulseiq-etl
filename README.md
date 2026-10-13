# PulseIQ ETL Pipeline

A repeatable pandas ETL for the supplied nine-table PulseIQ HR star-schema export.

## Source Tables

Place exactly one CSV for each table in `data/raw/`:

| Table | Primary key | Role |
| --- | --- | --- |
| `dim_date` | `date_key` | Calendar dates and fiscal/calendar attributes |
| `dim_department` | `department_id` | Department and cost-center lookup |
| `dim_leave_type` | `leave_type_code` | Leave policy lookup |
| `dim_location` | `work_location` | Location lookup |
| `dim_role` | `role_id` | Roles, levels, and salary bands |
| `dim_employee` | `employee_key` | Employee history / SCD dimension |
| `fact_attendance_monthly` | `attendance_fact_id` | Employee-month attendance measures |
| `fact_leave` | `leave_request_id` | Leave requests and decisions |
| `fact_payroll` | `payroll_item_id` | Employee payroll components and deductions |

`dim_employee.employee_id` is a business identifier and intentionally repeats across history rows. `employee_key` identifies each historical version and is the dimension primary key used by facts.

## Run with Docker

Run the production ETL from the repository root:

```bash
docker compose run --build --rm etl
```

The ETL checks that all nine expected CSV files are present and there are no extra CSV inputs. Source files are read only. Accepted tables are written to `data/clean/`, row-level rejects to `rejected/`, and logs plus a run summary to `logs/`.

To run the functional tests and process the real source tables with a single command, use:

```bash
docker compose run --build --rm etl-verify
```

The command stops before processing source data if any functional test fails. This is a finite batch job: Docker exits normally when processing and writing the summary are complete; it is not intended to remain running as a server.

## Validation and Outputs

- The ETL checks exact declared column sets, required values, date/numeric parsing, nonnegative measures, attendance-rate bounds, and primary-key uniqueness. Schema mismatches and duplicate primary keys fail the dataset/run; duplicate `employee_id` values in the SCD employee dimension are allowed.
- Foreign keys are checked against accepted dimension rows: employee department, role, location, manager; role department; and fact employee/date/department/role/leave-type references. Fact `employee_id` must agree with its `employee_key`; fact period/date keys must agree with calendar dates.
- Row-level data issues are written to the rejected output with `rejection_reason`; other valid rows continue through the run. The run summary reports input, duplicate, accepted, and rejected counts. Missing exit dates, manager IDs, leave decisions/approvers, and payroll variable-pay types are allowed because they occur in the supplied data.
- The supplied files contain payroll rows `PI-X0003`/`EMP-9001` and `PI-X0004`/`EMP-9002` with `employee_key=-1`. Neither employee ID appears in `dim_employee`, and the dimension has no `employee_key=-1` member, so these are unknown-employee fact rows, not failed employee-ID lookups; they are rejected by the declared foreign-key rule. The source also has two leave rows whose start date is after their end date; those rows are rejected without modifying the source files.
- `dim_employee.tenure_years` uses `exit_date` when present and otherwise `AS_OF_DATE` (default `2026-10-12`).
- `OUTPUT_FORMAT` supports `csv` and `parquet`. `RAW_DIR`, `CLEAN_DIR`, `REJECTED_DIR`, `LOG_DIR`, and `AS_OF_DATE` can be overridden through environment variables or `.env`.

## Metric Definitions

`metric_dictionary.csv` defines metrics against the nine-table model. The following business rules are agreed and encoded there:

- `active_employee_count` is a distinct `employee_id` snapshot at `AS_OF_DATE`. A qualifying SCD row has `employment_status = Active`, `hire_date` on or before the snapshot, no `exit_date` before the snapshot, and an SCD validity interval containing the snapshot (`valid_from <= AS_OF_DATE <= valid_to`).
- `payroll_headcount` is the sum of `fact_payroll.headcount_flag` per `run_id` and `period_end_date`.
- `payroll_total` is gross payroll: the sum of `fact_payroll.gross_pay` per `run_id` and `period_end_date`. It is not net pay or employer cost.
- `attendance_rate` is aggregated as total `present_days` divided by total `scheduled_days` per calendar month derived from `period_end_date`. Groups with a zero scheduled-day denominator are excluded.
- `approved_leave_days` sums `fact_leave.days` for `status = Approved`, grouped by the calendar month of `start_date`. Paid and unpaid leave types are included, and the entire request is attributed to its start month.

All metric calculations exclude rows rejected by ETL validation. Update the dictionary and this section together if these business definitions change.

Latest successful Docker verification recorded during this submission review: `logs/run_summary_20261012T165619Z.json` (2026-10-12 16:56 UTC). It completed the ETL run with status `success`; the combined verification command ran the functional suites before processing the source tables.

## Tests

The functional tests use synthetic schema-complete data and do not require real employee records:

```bash
docker compose run --build --rm etl-test
```

The separate Docker smoke test builds and runs the production image for both successful processing and missing-source failure. Run it from a host with Docker CLI and daemon access:

```bash
python -m pytest tests/test_docker.py -q
```

The tests import pandas and Pandera directly. A missing/broken runtime dependency fails test collection; functional tests are not skipped. Install dependencies with `python -m pip install -r requirements.txt` when package access is available.
