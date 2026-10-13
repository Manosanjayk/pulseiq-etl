from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path
from uuid import uuid4

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
DATASETS = {
    "dim_date": "date,date_key,year,quarter,month_number,month_name,year_month,week_of_year,day_of_week,is_weekend,is_holiday,is_working_day,financial_year,fiscal_quarter,fiscal_month_number,is_month_end,month_start_date,month_end_date\n2025-01-29,20250129,2025,1,1,January,2025-01,5,Wednesday,N,N,Y,FY2024-25,Q4,10,N,2025-01-01,2025-01-31\n2025-01-30,20250130,2025,1,1,January,2025-01,5,Thursday,N,N,Y,FY2024-25,Q4,10,N,2025-01-01,2025-01-31\n2025-01-31,20250131,2025,1,1,January,2025-01,5,Friday,N,N,Y,FY2024-25,Q4,10,Y,2025-01-01,2025-01-31\n",
    "dim_department": "department_id,department_name,cost_center,description,is_leadership\nD01,Engineering,CC-100,Engineering team,N\n",
    "dim_leave_type": "leave_type_code,leave_type_name,pay_type,annual_quota_days,prorated_for_joiners,carry_forward_allowed,max_carry_forward_days,policy_note\nCL,Casual Leave,Paid,12,Y,N,0,Short notice\n",
    "dim_location": "work_location,state,city_tier,tier_number\nPune,Maharashtra,Tier 1,1\n",
    "dim_role": "role_id,department_id,role_title,level,grade,band_min_annual,band_mid_annual,band_max_annual,overtime_eligible,band_reference_year\nR002,D01,Engineer,2,L2,720000,900000,1170000,N,2026\n",
    "dim_employee": "employee_key,employee_id,full_name,gender,date_of_birth,hire_date,exit_date,employment_status,employment_type,department_id,role_id,role_title,job_level,manager_id,work_location,work_mode,highest_education,valid_from,valid_to,is_current,change_reason\n100000,EMP-0001,Ada Sample,Female,1990-01-01,2020-01-01,,Active,Permanent,D01,R002,Engineer,2,,Pune,Hybrid,B.E.,2020-01-01,2026-12-31,Y,Joining\n",
    "fact_attendance_monthly": "attendance_fact_id,employee_id,date_key,employee_key,department_id,scheduled_days,present_days,wfh_days,half_days_unpaid,paid_leave_days,unpaid_absence_days,late_arrivals,avg_hours_worked,overtime_hours,attendance_rate,period_end_date\nFA-1,EMP-0001,20250131,100000,D01,23,22,10,0,1,0,1,8.5,0,0.9565,2025-01-31\n",
    "fact_leave": "leave_request_id,employee_id,employee_key,leave_type_code,start_date_key,end_date_key,start_date,end_date,days,status,applied_on,decided_on,approver_id,reason\nLR-1,EMP-0001,100000,CL,20250130,20250131,2025-01-30,2025-01-31,2,Approved,2025-01-29,2025-01-30,,Personal appointment\n",
    "fact_payroll": "payroll_item_id,run_id,employee_id,date_key,employee_key,period_end_date,department_id,role_id,payable_days,lop_days,overtime_hours_paid,variable_pay_type,monthly_fixed_gross,basic,hra,conveyance,medical,special_allowance,earned_fixed_pay,overtime_pay,variable_pay,gross_pay,pf_employee,esi_employee,professional_tax,tds,other_deductions,total_deductions,net_pay,pf_employer,esi_employer,employer_cost,headcount_flag\nPI-1,PR-2025-01,EMP-0001,20250131,100000,2025-01-31,D01,R002,31,0,0,,75000,30000,15000,1600,1250,27150,75000,0,0,75000,1800,0,200,5000,0,7000,68000,1800,0,76800,1\n",
}


def resolve_docker_cli() -> str | None:
    docker = shutil.which("docker")
    if docker is None and os.name == "nt" and os.environ.get("LOCALAPPDATA"):
        candidate = Path(os.environ["LOCALAPPDATA"]) / "Programs" / "DockerDesktop" / "resources" / "bin" / "docker.exe"
        if candidate.is_file():
            docker = str(candidate)
    return docker


def write_synthetic_inputs(raw_dir: Path) -> None:
    raw_dir.mkdir(parents=True, exist_ok=True)
    for dataset, contents in DATASETS.items():
        (raw_dir / f"{dataset}.csv").write_text(contents, encoding="utf-8")


def test_docker_executes_success_and_returns_nonzero_for_missing_sources(tmp_path: Path) -> None:
    docker = resolve_docker_cli()
    if docker is None:
        pytest.skip("Docker CLI is not installed")
    available = subprocess.run([docker, "info"], capture_output=True, text=True, check=False)
    if available.returncode != 0:
        pytest.skip("Docker daemon is not available")

    tag = f"pulseiq-etl-tests:{uuid4().hex}"
    built = subprocess.run(
        [docker, "build", "--tag", tag, str(REPO_ROOT)], capture_output=True, text=True, check=False
    )
    assert built.returncode == 0, built.stderr
    try:
        good_root = tmp_path / "good"
        write_synthetic_inputs(good_root / "raw")
        for folder in ("clean", "rejected", "logs"):
            (good_root / folder).mkdir()
        successful = subprocess.run(
            [
                docker,
                "run",
                "--rm",
                "-v",
                f"{good_root / 'raw'}:/app/data/raw",
                "-v",
                f"{good_root / 'clean'}:/app/data/clean",
                "-v",
                f"{good_root / 'rejected'}:/app/rejected",
                "-v",
                f"{good_root / 'logs'}:/app/logs",
                tag,
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        assert successful.returncode == 0, successful.stderr
        assert all((good_root / "clean" / f"{dataset}.csv").is_file() for dataset in DATASETS)

        bad_root = tmp_path / "bad"
        for folder in ("raw", "clean", "rejected", "logs"):
            (bad_root / folder).mkdir(parents=True)
        failed = subprocess.run(
            [
                docker,
                "run",
                "--rm",
                "-v",
                f"{bad_root / 'raw'}:/app/data/raw",
                "-v",
                f"{bad_root / 'clean'}:/app/data/clean",
                "-v",
                f"{bad_root / 'rejected'}:/app/rejected",
                "-v",
                f"{bad_root / 'logs'}:/app/logs",
                tag,
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        assert failed.returncode != 0
        summary_files = list((bad_root / "logs").glob("run_summary_*.json"))
        assert len(summary_files) == 1
        assert '"status": "failed"' in summary_files[0].read_text(encoding="utf-8")
    finally:
        subprocess.run([docker, "image", "rm", "--force", tag], capture_output=True, check=False)
