"""Unit tests of the DAG helpers (no Airflow needed): pytest dags/tests"""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dbt_airflow_helpers import (  # noqa: E402
    anomaly_email,
    failure_email,
    failures_by_team,
    parse_dbt_show_json,
    recipients,
    schedules_in_use,
)

MANIFEST = {
    "exposures": {
        "exposure.p.a": {"tags": ["use_case_1", "daily"]},
        "exposure.p.b": {"tags": ["use_case_2", "monthly"]},
    },
    "nodes": {
        "model.p.gold": {"name": "gold_model", "config": {"meta": {"notify": "team_uc", "criticality": "high"}}},
        "model.p.silver": {"name": "silver_model", "config": {"meta": {}}},
        "test.p.gold_test": {"name": "not_null_gold", "attached_node": "model.p.gold", "config": {"meta": {}}},
    },
}


def test_schedules_in_use_keeps_only_known_schedules_in_order():
    assert schedules_in_use(MANIFEST) == ["daily", "monthly"]


def test_failures_are_grouped_by_the_team_of_the_model():
    run_results = {"results": [
        {"unique_id": "model.p.gold", "status": "error", "message": "boom"},
        {"unique_id": "test.p.gold_test", "status": "fail", "message": "1 row"},
        {"unique_id": "model.p.silver", "status": "error", "message": "bad"},
        {"unique_id": "model.p.silver", "status": "success", "message": ""},
    ]}
    by_team = failures_by_team(run_results, MANIFEST)
    assert [f["name"] for f in by_team["team_uc"]] == ["gold_model", "not_null_gold"]
    assert [f["name"] for f in by_team["data_platform"]] == ["silver_model"]


def test_failure_email_flags_high_criticality():
    subject, body = failure_email("team_uc", [{"name": "m", "status": "error", "message": "x", "criticality": "high"}], "daily")
    assert subject.startswith("[HIGH] ")
    assert "team_uc" in body


def test_parse_dbt_show_json_reads_the_show_node_event():
    rows = [{"event_date": "2026-01-10", "source_database": "source_database_1", "z_score": 91.8}]
    output = "\n".join([
        json.dumps({"info": {"name": "MainReportVersion"}, "data": {}}),
        json.dumps({"info": {"name": "ShowNode"}, "data": {"node_name": "m", "preview": json.dumps(rows)}}),
    ])
    assert parse_dbt_show_json(output) == rows


def test_anomaly_email_is_none_without_rows():
    assert anomaly_email([]) is None
    subject, _ = anomaly_email([{"event_date": "2026-01-10", "z_score": 91.8}])
    assert "2026-01-10" in subject


def test_recipients_fall_back_to_the_data_platform_team():
    notify_map = {"team_uc": ["uc@company.com"], "data_platform": ["data@company.com"]}
    assert recipients("team_uc", notify_map) == ["uc@company.com"]
    assert recipients("unknown_team", notify_map) == ["data@company.com"]
