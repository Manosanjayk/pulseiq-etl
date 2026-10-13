# PulseIQ Analytics Warehouse

## Source basis and data-quality decisions

The warehouse is loaded from the nine accepted CSV outputs in `data/clean/`, produced by Task 1. The matching raw exports and Task 1 rejection CSVs are also available. No Excel workbook was present in the workspace or under `D:\mano`, so the workbook-only `data_dictionary`, `data_contracts`, reconciliation answer key, department-month reference, and known-issues sheets could not be inspected. This implementation does not claim reconciliation to those absent sheets.

The supplied export has 2,557 dates, 11 departments, 6 leave types, 9 locations, 58 roles, 1,606 employee versions for 745 employee IDs, 7,343 monthly attendance rows, 3,439 accepted leave requests, and 7,342 accepted payroll items. The raw payroll file has no exact duplicate rows and no duplicate `payroll_item_id`, but it has two business-grain collisions: `PI-000761`/`PI-X0002` for `PR-2025-05`/`EMP-0598` and `PI-004059`/`PI-X0001` for `PR-2026-02`/`EMP-0194`. Within each pair every source field except `payroll_item_id` is identical. They are flagged as duplicate-transaction candidates in batch reconciliation, but both source rows remain loaded because the absent workbook answer key prevents a definitive business disposition. Current clean-source totals therefore include candidate excesses of 213,025.04 gross pay and 216,625.04 employer cost (currency is not specified in the CSV contract). Task 1 rejects two payroll rows (`PI-X0003`, `PI-X0004`) because `employee_key=-1` is not a dimension member. Task 1 rejects two leave rows because start date is after end date. The warehouse stores these source rejects in `warehouse_rejection` with their original fields and reasons; it does not silently load them into facts.

The source employee histories have 745 current versions and 441 `9999-12-31` end-date sentinels. Dates are parsed from their ISO text into Python/PostgreSQL `date` values, not pandas timestamps, so year 9999 is safe. There is one overlapping pair of source intervals for `EMP-0001`; both rows have the same department and role. To make the warehouse's inclusive validity intervals non-overlapping, the earlier warehouse `valid_to` is clipped to the day before the next version's `valid_from`. The original end date is retained in `source_valid_to`. No department transfer is inferred or added to the production data.

All 1,606 employee versions reference existing departments and roles, and every role's department matches its employee-version department. Every accepted fact references an existing source employee-version key. However, 85 attendance rows and 82 payroll rows have a month-end later than that referenced version's source `valid_to`; all accepted leave start dates fall within their referenced version. These month-end mismatches are retained, not remapped or discarded. Facts use their exact source version key for traceability, and the discrepancy remains visible through `source_valid_to` and fact-period dates.

No `fact_headcount_monthly.csv` exists. The warehouse derives monthly snapshots only for payroll period-end dates, from the supplied SCD employee versions. Active headcount uses the version effective at period end, `employment_status = Active`, hire date on or before period end, and exit date null or on/after period end. New hires and attrition are counted by their source hire/exit dates. This is derived reporting data, not an input source table.

## Grains and keys

| Table | Grain | Natural key | Warehouse key |
| --- | --- | --- | --- |
| `dim_date` | One calendar date | `date_key` | `date_key` |
| `dim_department` | One source department | `department_id` | `department_id` |
| `dim_role` | One source role | `role_id` | `role_id` |
| `dim_location` | One source work location | `work_location` | `work_location` |
| `dim_leave_type` | One source leave policy/code | `leave_type_code` | `leave_type_code` |
| `dim_employee` | One employee history version | `employee_key` | `employee_sk` identity; `source_employee_key` uniquely maps the source version key |
| `fact_attendance_monthly` | One employee-version attendance record per source month | `attendance_fact_id` | `attendance_fact_id` |
| `fact_payroll` | One employee payroll item in a payroll run | `payroll_item_id` | `payroll_item_id` |
| `fact_leave` | One leave request | `leave_request_id` | `leave_request_id` |
| `fact_headcount_monthly` | One department at one payroll month end | `(month_end_date, department_id)` | Same composite key |

Attendance remains monthly. The source has no daily attendance facts, so the warehouse does not generate daily attendance records.

## Relationships and mappings

- `dim_role.department_id`, `dim_employee.department_id`, and all fact department IDs reference `dim_department`.
- Employee role and location keys reference `dim_role` and `dim_location`.
- Attendance/payroll use `date_key` and `period_end_date`; leave uses start/end date keys and dates. Both key/date pairs remain available for reconciliation.
- Each fact maps its source `employee_key` to exactly one `dim_employee.employee_sk`. The source employee ID is preserved on facts. The source version key is authoritative for fact lineage, even for the 167 month-end/source-validity mismatches described above; department and role values on facts remain the source-time attributes, not a join to today's employee version.
- Payroll monetary fields are `NUMERIC(18,2)`. Precision-sensitive measures and leave/day values use `NUMERIC`; true/false flags use `BOOLEAN`; counts and keys use integer types.
- Nullable employee manager references are mapped to the current manager version when present. Missing managers remain null.

## SCD Type 2 behavior

`source_employee_key` preserves a stable source version mapping, while `employee_sk` is the warehouse surrogate key referenced by facts. The loader retains every source version and upserts on `source_employee_key`; it does not infer a new production version from a changed source export with no new source version key. Department and role changes represented as source history rows remain separate employee versions. A partial unique index permits only one `is_current` row per business `employee_id`; a GiST exclusion constraint prevents overlapping inclusive date ranges. The loader serializes batches with a PostgreSQL advisory transaction lock.

The source's current-version sentinel remains valid as a PostgreSQL `DATE`. `source_valid_to` records the source boundary independently from effective `valid_to`, allowing the single observed overlap to be resolved transparently and reproducibly.

## Reconciliation

Every successful `warehouse_batch` records source, accepted, and rejected counts plus independent input totals for payroll gross pay, employer cost, attendance present/scheduled days, and approved leave days. The loader compares those input totals with database totals before commit. The verified clean-source baseline is:

| Measure | Clean CSV baseline |
| --- | ---: |
| Payroll gross pay | 849,839,985.31 |
| Payroll employer cost | 861,803,017.46 |
| Attendance present days | 141,195 |
| Attendance scheduled days | 150,215 |
| Attendance aggregate rate | 0.9399527344 |
| Approved leave days | 7,274 |

These are independently calculated from the accepted clean CSVs. They are not represented as workbook answer-key matches.

## Test-data isolation

Warehouse integration tests require `WAREHOUSE_TEST_DATABASE_URL` to point to a database whose name ends in `_test`. Compose uses a dedicated `pulseiq_warehouse_test` database on a separate PostgreSQL service backed by ephemeral `tmpfs`, never the persistent application database. Department-transfer test rows are written only to temporary copied CSVs used by the test and discarded afterward.

## Migrations and operation

`sql/migrations/001_initial_warehouse.sql` is the versioned initial schema. `src/warehouse.py` tracks and applies unapplied migrations in `schema_migrations`; `sql/warehouse_schema.sql` is a standalone bootstrap copy for manual inspection or setup. The loader's fact upserts use each source natural key, making repeat loads idempotent for the same complete source snapshot. A failed data transaction rolls back all dimensions, facts, and success audit data; a separate failed batch record is retained with the error.