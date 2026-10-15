"""Unit tests of the DAG helpers (no Airflow needed): pytest dags/tests"""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dbt_airflow_helpers import (  # noqa: E402
    alert_email,
    dag_specs,
    exposures_affected_by,
    failure_email,
    failures_by_team,
    not_ready_email,
    parse_dbt_show_json,
    ready_by_groups,
    recipients,
)

# silver -> gold_daily -> export (daily, high)      silver -> gold_monthly -> committee (custom)
#                     -> alert_view -> alert (daily)
MANIFEST = {
    "exposures": {
        "exposure.p.export": {
            "name": "export", "tags": ["uc", "daily"],
            "config": {"meta": {"notify": "team_export", "criticality": "high", "ready_by": "07:30"}},
            "depends_on": {"nodes": ["model.p.gold_daily"]},
        },
        "exposure.p.alert": {
            "name": "alert", "tags": ["uc", "daily"],
            "config": {"meta": {"notify": "team_uc", "criticality": "medium", "ready_by": "08:00", "consumer": "alert"}},
            "depends_on": {"nodes": ["model.p.alert_view"]},
        },
        "exposure.p.committee": {
            "name": "committee", "tags": ["uc", "custom"],
            "config": {"meta": {"notify": "team_uc", "criticality": "medium", "ready_by": "09:00", "cron": "0 6 * * 2#3"}},
            "depends_on": {"nodes": ["model.p.gold_monthly"]},
        },
    },
    "nodes": {
        "model.p.silver": {"name": "silver", "config": {"meta": {}}},
        "model.p.gold_daily": {"name": "gold_daily", "config": {"meta": {"notify": "team_uc"}}},
        "model.p.gold_monthly": {"name": "gold_monthly", "config": {"meta": {"notify": "team_uc"}}},
        "model.p.alert_view": {"name": "alert_view", "config": {"meta": {}}},
        "model.p.orphan": {"name": "orphan", "config": {"meta": {}}},
        "test.p.gold_daily_test": {"name": "not_null_gold_daily", "attached_node": "model.p.gold_daily", "config": {"meta": {}}},
    },
    "child_map": {
        "model.p.silver": ["model.p.gold_daily", "model.p.gold_monthly", "model.p.alert_view"],
        "model.p.gold_daily": ["exposure.p.export", "test.p.gold_daily_test"],
        "model.p.gold_monthly": ["exposure.p.committee"],
        "model.p.alert_view": ["exposure.p.alert"],
    },
}


def specs_by_id():
    return {spec["dag_id"]: spec for spec in dag_specs(MANIFEST)}


def test_shared_schedule_and_custom_exposures_get_their_own_dags():
    specs = specs_by_id()
    assert sorted(specs) == ["poc_dbt__committee", "poc_dbt__daily"]
    assert specs["poc_dbt__daily"]["select"] == "+tag:daily" and specs["poc_dbt__daily"]["snapshot"]
    assert specs["poc_dbt__committee"]["cron"] == "0 6 * * 2#3"
    assert specs["poc_dbt__committee"]["select"] == "+exposure:committee"
    assert not specs["poc_dbt__committee"]["snapshot"]


def test_alert_exposures_become_alert_tasks_on_their_model():
    alerts = specs_by_id()["poc_dbt__daily"]["alerts"]
    assert [(a["exposure"]["name"], a["model"]) for a in alerts] == [("alert", "alert_view")]


def test_exposures_affected_follow_the_lineage_and_tests_follow_their_model():
    assert exposures_affected_by("model.p.silver", MANIFEST) == {
        "exposure.p.export", "exposure.p.alert", "exposure.p.committee"}
    assert exposures_affected_by("test.p.gold_daily_test", MANIFEST) == {"exposure.p.export"}


def test_failures_go_to_the_teams_of_the_affected_exposures_of_the_dag():
    run_results = {"results": [
        {"unique_id": "model.p.silver", "status": "error", "message": "boom"},
        {"unique_id": "model.p.orphan", "status": "error", "message": "bad"},
        {"unique_id": "model.p.gold_daily", "status": "success", "message": ""},
    ]}
    by_team = failures_by_team(run_results, MANIFEST, specs_by_id()["poc_dbt__daily"]["exposures"])
    assert by_team["team_export"][0]["criticality"] == "high"
    assert by_team["team_export"][0]["exposures"] == ["export"]
    # committee is affected too, but it is not part of the daily DAG
    assert by_team["team_uc"][0]["exposures"] == ["alert"]
    assert [f["name"] for f in by_team["data_platform"]] == ["orphan"]


def test_failure_email_flags_high_criticality():
    subject, body = failure_email("team_export", [{"name": "m", "status": "error", "message": "x",
                                                   "criticality": "high", "exposures": ["export"]}], "poc_dbt__daily")
    assert subject.startswith("[HIGH] ") and "export" in body


def test_ready_by_groups_one_deadline_check_per_distinct_time():
    groups = ready_by_groups(specs_by_id()["poc_dbt__daily"]["exposures"])
    assert [(ready_by, [e["name"] for e in members]) for ready_by, members in groups] == [
        ("07:30", ["export"]), ("08:00", ["alert"])]


def test_not_ready_email_reports_the_build_state():
    export = specs_by_id()["poc_dbt__daily"]["exposures"][0]
    subject, body = not_ready_email("team_export", [export], "poc_dbt__daily", "07:30", "running")
    assert subject == "[HIGH] poc_dbt__daily: data not ready by 07:30 UTC for export"
    assert "running" in body
    assert "not started" in not_ready_email("team_export", [export], "poc_dbt__daily", "07:30", None)[1]


def test_parse_dbt_show_json_reads_the_show_node_event():
    rows = [{"event_date": "2026-01-10", "source_database": "source_database_1", "z_score": 91.8}]
    output = "\n".join([
        json.dumps({"info": {"name": "MainReportVersion"}, "data": {}}),
        json.dumps({"info": {"name": "ShowNode"}, "data": {"node_name": "m", "preview": json.dumps(rows)}}),
    ])
    assert parse_dbt_show_json(output) == rows


def test_alert_email_is_none_without_rows():
    exposure = specs_by_id()["poc_dbt__daily"]["alerts"][0]["exposure"]
    assert alert_email(exposure, []) is None
    subject, _ = alert_email(exposure, [{"event_date": "2026-01-10", "z_score": 91.8}])
    assert subject == "alert: 1 row(s) to review"


def test_recipients_fall_back_to_the_data_platform_team():
    notify_map = {"team_uc": ["uc@company.com"], "data_platform": ["data@company.com"]}
    assert recipients("team_uc", notify_map) == ["uc@company.com"]
    assert recipients("unknown_team", notify_map) == ["data@company.com"]
