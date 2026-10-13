-- 1. Monthly gross payroll and employer cost by department.
SELECT date_trunc('month', p.period_end_date)::date AS payroll_month,
       d.department_id,
       d.department_name,
       SUM(p.gross_pay) AS gross_pay,
       SUM(p.employer_cost) AS employer_cost
FROM fact_payroll AS p
JOIN dim_department AS d USING (department_id)
WHERE p.period_end_date >= DATE '2025-01-01'
  AND p.period_end_date < DATE '2027-01-01'
GROUP BY 1, 2, 3
ORDER BY 1, 2;

-- 2. Monthly attendance rate, calculated from total days rather than row rates.
SELECT date_trunc('month', a.period_end_date)::date AS attendance_month,
       d.department_id,
       d.department_name,
       SUM(a.present_days)::numeric / NULLIF(SUM(a.scheduled_days), 0) AS attendance_rate
FROM fact_attendance_monthly AS a
JOIN dim_department AS d USING (department_id)
WHERE a.period_end_date >= DATE '2025-01-01'
  AND a.period_end_date < DATE '2027-01-01'
GROUP BY 1, 2, 3
HAVING SUM(a.scheduled_days) > 0
ORDER BY 1, 2;

-- 3. Approved leave days by request-start month and leave type.
SELECT date_trunc('month', l.start_date)::date AS leave_month,
       t.leave_type_code,
       t.leave_type_name,
       SUM(l.days) AS approved_leave_days
FROM fact_leave AS l
JOIN dim_leave_type AS t USING (leave_type_code)
WHERE l.status = 'Approved'
  AND l.start_date >= DATE '2025-01-01'
  AND l.start_date < DATE '2027-01-01'
GROUP BY 1, 2, 3
ORDER BY 1, 2;

-- 4. Department headcount, new hires, and attrition at observed payroll month ends.
SELECT h.month_end_date,
       d.department_id,
       d.department_name,
       h.employee_count,
       h.new_hires,
       h.attrition_count
FROM fact_headcount_monthly AS h
JOIN dim_department AS d USING (department_id)
WHERE h.month_end_date >= DATE '2025-01-01'
  AND h.month_end_date < DATE '2027-01-01'
ORDER BY h.month_end_date, d.department_id;

-- 5. Department assignment for each employee at an explicit historical date.
-- This uses the inclusive SCD validity interval and does not join facts whose
-- supplied month-end/version combinations are known to be inconsistent.
WITH parameters AS (
    SELECT DATE '2025-06-30' AS as_of_date
)
SELECT p.as_of_date,
       e.employee_id,
       e.full_name,
       e.department_id,
       d.department_name,
       e.role_id,
       e.valid_from,
       e.valid_to
FROM parameters AS p
JOIN dim_employee AS e
  ON p.as_of_date BETWEEN e.valid_from AND e.valid_to
JOIN dim_department AS d
  ON d.department_id = e.department_id
ORDER BY e.employee_id;