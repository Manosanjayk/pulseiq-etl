import os
from datetime import datetime
from flask import Flask, request, jsonify, abort
import psycopg
from psycopg.rows import dict_row

app = Flask(__name__)

def get_db_url() -> str:
    return (
        os.getenv("WAREHOUSE_DATABASE_URL")
        or os.getenv("WAREHOUSE_TEST_DATABASE_URL")
        or "postgresql://pulseiq:pulseiq_local_only@localhost:5432/pulseiq_warehouse"
    )

# Helper to execute query with params and return list of dicts
def run_query(sql_text: str, params: tuple):
    try:
        with psycopg.connect(get_db_url(), row_factory=dict_row) as conn:
            rows = conn.execute(sql_text, params).fetchall()
            return [dict(row) for row in rows]
    except Exception as e:
        app.logger.error(f"Database error: {e}")
        abort(500, description="Internal server error")

def parse_iso(date_str: str):
    try:
        return datetime.fromisoformat(date_str).date()
    except Exception:
        abort(400, description="Invalid ISO date format; expected YYYY-MM-DD")

@app.route('/payroll_by_dept')
def payroll_by_dept():
    # Accept both explicit and short parameter names
    start_raw = request.args.get('start_date') or request.args.get('start')
    end_raw = request.args.get('end_date') or request.args.get('end')
    start = parse_iso(start_raw)
    end = parse_iso(end_raw)
    dept = request.args.get('department_id')
    sql = """
        SELECT date_trunc('month', p.period_end_date)::date AS month,
               d.department_id,
               SUM(p.gross_pay) AS gross_pay,
               SUM(p.employer_cost) AS employer_cost,
               COUNT(*) AS employees_paid
        FROM fact_payroll AS p
        JOIN dim_department AS d USING (department_id)
        WHERE p.period_end_date >= %s AND p.period_end_date < %s
    """
    params = (start, end)
    if dept:
        sql += " AND d.department_id = %s"
        params += (dept,)
    sql += " GROUP BY 1,2 ORDER BY 1,2"
    result = run_query(sql, params)
    return jsonify(result)

@app.route('/attendance_rate')
def attendance_rate():
    start_raw = request.args.get('start_date') or request.args.get('start')
    end_raw = request.args.get('end_date') or request.args.get('end')
    start = parse_iso(start_raw)
    end = parse_iso(end_raw)
    dept = request.args.get('department_id')
    sql = """
        SELECT date_trunc('month', a.period_end_date)::date AS month,
               d.department_id,
               SUM(a.present_days)::numeric / NULLIF(SUM(a.scheduled_days),0) AS attendance_rate,
               SUM(a.present_days) AS present_days,
               SUM(a.scheduled_days) AS scheduled_days
        FROM fact_attendance_monthly AS a
        JOIN dim_department AS d USING (department_id)
        WHERE a.period_end_date >= %s AND a.period_end_date < %s
    """
    params = (start, end)
    if dept:
        sql += " AND d.department_id = %s"
        params += (dept,)
    sql += " GROUP BY 1,2 HAVING SUM(a.scheduled_days) > 0 ORDER BY 1,2"
    result = run_query(sql, params)
    return jsonify(result)

@app.route('/approved_leave')
def approved_leave():
    start_raw = request.args.get('start_date') or request.args.get('start')
    end_raw = request.args.get('end_date') or request.args.get('end')
    start = parse_iso(start_raw)
    end = parse_iso(end_raw)
    sql = """
        SELECT date_trunc('month', l.start_date)::date AS month,
               SUM(l.days) AS approved_leave_days
        FROM fact_leave AS l
        WHERE l.status = 'Approved' AND l.start_date >= %s AND l.start_date < %s
        GROUP BY 1 ORDER BY 1
    """
    params = (start, end)
    result = run_query(sql, params)
    return jsonify(result)

@app.route('/health')
def health():
    return jsonify({"status": "ok"})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)
