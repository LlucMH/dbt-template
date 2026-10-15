"""Pure helpers for the poc_dbt Airflow DAGs: no Airflow import, so they are unit-tested
with pytest in the CI (dags/tests/).

They read dbt's artifacts (target/manifest.json, target/run_results.json) and the output
of `dbt show`, so the DAGs work the same on every warehouse: dbt talks to the warehouse,
the DAG only talks to dbt.
"""
import json
from collections import defaultdict
from pathlib import Path

# Schedules the DAGs understand: exposure tag -> cron expression (UTC).
# An exposure tagged with one of them gets its parents built by that DAG.
SCHEDULES = {
    "daily": "0 6 * * *",
    "weekly": "0 6 * * 1",
    "monthly": "0 7 1 * *",
}

FAILED_STATUSES = {"error", "fail", "runtime error"}


def load_json(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def schedules_in_use(manifest):
    """Schedules that at least one enabled exposure is tagged with, in SCHEDULES order."""
    tags = set()
    for exposure in manifest.get("exposures", {}).values():
        tags.update(exposure.get("tags", []))
    return [schedule for schedule in SCHEDULES if schedule in tags]


def failures_by_team(run_results, manifest):
    """Failed nodes of a dbt run, grouped by the team in their meta.notify.

    Returns {team: [{"name", "status", "message", "criticality"}, ...]}. Nodes without
    meta.notify (bronze, silver, tests on them...) go to the "data_platform" team.
    A test is attributed to the team of the model it tests.
    """
    nodes = manifest.get("nodes", {})
    by_team = defaultdict(list)
    for result in run_results.get("results", []):
        if str(result.get("status", "")).lower() not in FAILED_STATUSES:
            continue
        node = nodes.get(result["unique_id"], {})
        owner_node = nodes.get(node.get("attached_node") or "", node)
        meta = (owner_node.get("config") or {}).get("meta") or owner_node.get("meta") or {}
        by_team[meta.get("notify", "data_platform")].append({
            "name": node.get("name", result["unique_id"]),
            "status": result.get("status"),
            "message": (result.get("message") or "").strip(),
            "criticality": meta.get("criticality", "medium"),
        })
    return dict(by_team)


def failure_email(team, failures, schedule):
    """Subject and HTML body of the failure e-mail for one team."""
    critical = any(f["criticality"] == "high" for f in failures)
    subject = f"{'[HIGH] ' if critical else ''}poc_dbt {schedule} run: {len(failures)} failed node(s) for {team}"
    rows = "".join(
        f"<tr><td>{f['name']}</td><td>{f['status']}</td><td>{f['criticality']}</td><td>{f['message']}</td></tr>"
        for f in failures
    )
    body = (
        f"<p>The {schedule} dbt run finished with failures owned by <b>{team}</b>.</p>"
        "<table border='1' cellpadding='4'><tr><th>Node</th><th>Status</th><th>Criticality</th><th>Message</th></tr>"
        f"{rows}</table>"
    )
    return subject, body


def parse_dbt_show_json(output):
    """Rows of `dbt show --output json --log-format json` (one JSON event per line)."""
    for line in output.splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        event = json.loads(line)
        preview = event.get("data", {}).get("preview")
        if preview is not None:
            return json.loads(preview) if isinstance(preview, str) else preview
    return []


def anomaly_email(rows):
    """Subject and HTML body of the anomaly alert, or None when there is nothing to report."""
    if not rows:
        return None
    columns = list(rows[0].keys())
    header = "".join(f"<th>{column}</th>" for column in columns)
    lines = "".join("<tr>" + "".join(f"<td>{row.get(c)}</td>" for c in columns) + "</tr>" for row in rows)
    subject = f"poc_dbt: {len(rows)} amount anomaly(ies) on {rows[0].get('event_date')}"
    body = (
        "<p>The latest loaded day has amounts far from their usual level "
        "(more than 3 standard deviations from the previous 7 days).</p>"
        f"<table border='1' cellpadding='4'><tr>{header}</tr>{lines}</table>"
    )
    return subject, body


def recipients(team, notify_map):
    """E-mail addresses of a team, from the Airflow Variable dbt_notify_recipients.

    Addresses live in Airflow, never in the repository: meta.notify only holds team ids.
    """
    return notify_map.get(team) or notify_map.get("data_platform") or []
