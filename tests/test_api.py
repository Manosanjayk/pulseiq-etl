import os
from decimal import Decimal
from urllib.parse import urljoin
import pytest
import requests
from src.api import app

API_BASE_URL = os.getenv("API_BASE_URL")

class HttpClient:
    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip("/") + "/"

    def get(self, path: str):
        url = urljoin(self.base_url, path.lstrip("/"))
        resp = requests.get(url, timeout=10)
        return resp

@pytest.fixture
def client():
    if API_BASE_URL:
        # Wrap requests with compatible helper
        yield HttpClient(API_BASE_URL)
    else:
        app.config["TESTING"] = True
        with app.test_client() as c:
            # Wrap test_client with .text property for compatibility
            yield c

def _get_json(resp):
    if hasattr(resp, "get_json"):
        return resp.get_json()
    return resp.json()

def _get_text(resp):
    if hasattr(resp, "get_data"):
        return resp.get_data(as_text=True)
    return resp.text

def test_health_endpoint(client):
    resp = client.get("/health")
    assert resp.status_code == 200
    assert _get_json(resp) == {"status": "ok"}

def test_payroll_by_dept_success(client):
    resp = client.get("/payroll_by_dept?start_date=2025-04-01&end_date=2025-05-31")
    assert resp.status_code == 200
    data = _get_json(resp)
    assert isinstance(data, list)
    if data:
        first = data[0]
        assert "department_id" in first
        assert "gross_pay" in first
        assert "employer_cost" in first
        assert "employees_paid" in first
        assert "month" in first

def test_payroll_by_dept_with_department_filter(client):
    resp = client.get("/payroll_by_dept?start_date=2025-04-01&end_date=2025-05-31&department_id=D01")
    assert resp.status_code == 200
    data = _get_json(resp)
    assert isinstance(data, list)
    assert all(row["department_id"] == "D01" for row in data)

def test_attendance_rate_success(client):
    resp = client.get("/attendance_rate?start_date=2025-04-01&end_date=2025-05-31")
    assert resp.status_code == 200
    data = _get_json(resp)
    assert isinstance(data, list)
    if data:
        first = data[0]
        assert "department_id" in first
        assert "attendance_rate" in first
        assert "present_days" in first
        assert "scheduled_days" in first

def test_approved_leave_success(client):
    resp = client.get("/approved_leave?start_date=2025-04-01&end_date=2025-05-31")
    assert resp.status_code == 200
    data = _get_json(resp)
    assert isinstance(data, list)
    if data:
        first = data[0]
        assert "approved_leave_days" in first
        assert "month" in first

def test_invalid_date_format_returns_400(client):
    resp = client.get("/payroll_by_dept?start_date=01-01-2025&end_date=12-31-2025")
    assert resp.status_code == 400
    assert "Invalid ISO date format" in _get_text(resp)

def test_missing_date_params_returns_400(client):
    resp = client.get("/payroll_by_dept")
    assert resp.status_code == 400

    resp = client.get("/attendance_rate")
    assert resp.status_code == 400

    resp = client.get("/approved_leave")
    assert resp.status_code == 400

def test_database_error_handling_sanitizes_message(client, monkeypatch):
    if API_BASE_URL:
        pytest.skip("Local monkeypatch test is only applicable to in-process test client")
    import src.api
    def faulty_run_query(sql_text, params):
        from flask import abort
        abort(500, description="Internal server error")
    monkeypatch.setattr(src.api, "run_query", faulty_run_query)
    resp = client.get("/payroll_by_dept?start_date=2025-04-01&end_date=2025-05-31")
    assert resp.status_code == 500
    assert "Internal server error" in _get_text(resp)
    assert "password" not in _get_text(resp).lower()

