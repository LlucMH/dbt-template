"""Pure helpers for the poc_dbt Airflow DAGs: no Airflow import, so they are unit-tested
with pytest in the CI (dags/tests/).

They read dbt's artifacts (target/manifest.json, target/run_results.json) and the output
of `dbt show`, so the DAGs work the same on every warehouse: dbt talks to the warehouse,
the DAG only talks to dbt.

The exposures are the scheduling contract (see the header of any _<use_case>__exposures.yml,
checked by macros/validate_exposures.sql):
  tags            one schedule: daily | weekly | monthly (shared DAG) or custom (own DAG)
  meta.cron       custom only: cron expression (UTC)
  meta.ready_by   "HH:MM" UTC by which the consumer needs the data
  meta.notify     team alerted when something the exposure needs fails or is late
  meta.criticality  low | medium | high
"""
import json
from collections import defaultdict, deque
from pathlib import Path

# Shared schedules: tag -> cron expression (UTC). Every exposure with the tag is
# built by the same DAG, poc_dbt__<schedule>.
SCHEDULES = {
    "daily": "0 6 * * *",
    "weekly": "0 6 * * 1",
    "monthly": "0 7 1 * *",
}
CUSTOM = "custom"
# Snapshots capture history once a day: only the daily DAG runs them.
SNAPSHOT_SCHEDULE = "daily"

FAILED_STATUSES = {"error", "fail", "runtime error"}


def load_json(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _meta(node):
    return (node.get("config") or {}).get("meta") or node.get("meta") or {}


def _exposure(exposure_id, exposure):
    meta = _meta(exposure)
    return {
        "id": exposure_id,
        "name": exposure["name"],
        "notify": meta.get("notify", "data_platform"),
        "criticality": meta.get("criticality", "medium"),
        "ready_by": meta.get("ready_by"),
        "consumer": meta.get("consumer"),
        "depends_on": (exposure.get("depends_on") or {}).get("nodes", []),
    }


def dag_specs(manifest):
    """One spec per DAG to create, from the exposures of the manifest.

    Shared schedules (daily, weekly, monthly) give one DAG for all their exposures,
    selected by tag; every custom exposure gives its own DAG at its meta.cron,
    selected by exposure. Alerts are the exposures with meta.consumer: alert.
    """
    exposures = manifest.get("exposures", {})
    specs = []
    for schedule, cron in SCHEDULES.items():
        members = [_exposure(i, e) for i, e in exposures.items() if schedule in e.get("tags", [])]
        if members:
            specs.append({
                "dag_id": f"poc_dbt__{schedule}",
                "cron": cron,
                "select": f"+tag:{schedule}",
                "snapshot": schedule == SNAPSHOT_SCHEDULE,
                "exposures": members,
            })
    for exposure_id, exposure in exposures.items():
        if CUSTOM in exposure.get("tags", []):
            member = _exposure(exposure_id, exposure)
            specs.append({
                "dag_id": f"poc_dbt__{member['name']}",
                "cron": _meta(exposure)["cron"],
                "select": f"+exposure:{member['name']}",
                "snapshot": False,
                "exposures": [member],
            })
    nodes = manifest.get("nodes", {})
    for spec in specs:
        spec["alerts"] = [
            {"exposure": e, "model": nodes[e["depends_on"][0]]["name"]}
            for e in spec["exposures"]
            if e["consumer"] == "alert" and e["depends_on"] and e["depends_on"][0] in nodes
        ]
    return specs


def exposures_affected_by(node_id, manifest):
    """Ids of the exposures downstream of a node, following manifest.child_map.

    A test is traced from the model it tests: if a test fails, every consumer of
    that model is affected.
    """
    node = manifest.get("nodes", {}).get(node_id, {})
    start = node.get("attached_node") or node_id
    child_map = manifest.get("child_map", {})
    seen, queue, affected = {start}, deque([start]), set()
    while queue:
        for child in child_map.get(queue.popleft(), []):
            if child in seen:
                continue
            seen.add(child)
            if child.startswith("exposure."):
                affected.add(child)
            else:
                queue.append(child)
    return affected


def failures_by_team(run_results, manifest, exposures):
    """Failed nodes of a dbt run, grouped by the team that must hear about them.

    Each failure goes to the meta.notify of every exposure of this DAG it affects,
    with that exposure's criticality. A failure that affects none of them goes to
    the node's own meta.notify, or to "data_platform".
    Returns {team: [{"name", "status", "message", "criticality", "exposures"}, ...]}.
    """
    nodes = manifest.get("nodes", {})
    by_id = {e["id"]: e for e in exposures}
    by_team = defaultdict(list)
    for result in run_results.get("results", []):
        if str(result.get("status", "")).lower() not in FAILED_STATUSES:
            continue
        node_id = result["unique_id"]
        node = nodes.get(node_id, {})
        failure = {
            "name": node.get("name", node_id),
            "status": result.get("status"),
            "message": (result.get("message") or "").strip(),
        }
        affected = [by_id[i] for i in sorted(exposures_affected_by(node_id, manifest)) if i in by_id]
        if affected:
            teams = defaultdict(list)
            for exposure in affected:
                teams[exposure["notify"]].append(exposure)
            for team, team_exposures in teams.items():
                critical = any(e["criticality"] == "high" for e in team_exposures)
                by_team[team].append({**failure, "criticality": "high" if critical else team_exposures[0]["criticality"],
                                      "exposures": [e["name"] for e in team_exposures]})
        else:
            owner = nodes.get(node.get("attached_node") or "", node)
            by_team[_meta(owner).get("notify", "data_platform")].append(
                {**failure, "criticality": _meta(owner).get("criticality", "medium"), "exposures": []})
    return dict(by_team)


def failure_email(team, failures, dag_id):
    """Subject and HTML body of the failure e-mail for one team."""
    critical = any(f["criticality"] == "high" for f in failures)
    subject = f"{'[HIGH] ' if critical else ''}{dag_id}: {len(failures)} failed node(s) for {team}"
    rows = "".join(
        f"<tr><td>{f['name']}</td><td>{f['status']}</td><td>{', '.join(f['exposures']) or '-'}</td>"
        f"<td>{f['criticality']}</td><td>{f['message']}</td></tr>"
        for f in failures
    )
    body = (
        f"<p>{dag_id} finished with failures that concern <b>{team}</b>.</p>"
        "<table border='1' cellpadding='4'><tr><th>Node</th><th>Status</th><th>Affected exposures</th>"
        f"<th>Criticality</th><th>Message</th></tr>{rows}</table>"
    )
    return subject, body


def ready_by_groups(exposures):
    """Exposures with a ready_by, grouped by it and sorted: [(ready_by, [exposures]), ...].

    The DAG creates one deadline check per distinct ready_by.
    """
    groups = defaultdict(list)
    for exposure in exposures:
        if exposure["ready_by"]:
            groups[exposure["ready_by"]].append(exposure)
    return sorted(groups.items())


def not_ready_email(team, exposures, dag_id, ready_by, build_state):
    """Subject and HTML body of the e-mail sent when the data is not ready by ready_by (UTC)."""
    critical = any(e["criticality"] == "high" for e in exposures)
    names = ", ".join(e["name"] for e in exposures)
    subject = f"{'[HIGH] ' if critical else ''}{dag_id}: data not ready by {ready_by} UTC for {names}"
    body = (
        f"<p>At {ready_by} UTC the dbt build of {dag_id} was <b>{build_state or 'not started'}</b>, "
        f"so these consumers (owned by <b>{team}</b>) do not have their data yet:</p>"
        "<ul>" + "".join(f"<li>{e['name']}</li>" for e in exposures) + "</ul>"
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


def alert_email(exposure, rows):
    """Subject and HTML body of an alert exposure, or None when there is nothing to report."""
    if not rows:
        return None
    columns = list(rows[0].keys())
    header = "".join(f"<th>{column}</th>" for column in columns)
    lines = "".join("<tr>" + "".join(f"<td>{row.get(c)}</td>" for c in columns) + "</tr>" for row in rows)
    prefix = "[HIGH] " if exposure["criticality"] == "high" else ""
    subject = f"{prefix}{exposure['name']}: {len(rows)} row(s) to review"
    body = (
        f"<p>The alert <b>{exposure['name']}</b> has rows to review.</p>"
        f"<table border='1' cellpadding='4'><tr>{header}</tr>{lines}</table>"
    )
    return subject, body


def recipients(team, notify_map):
    """E-mail addresses of a team, from the Airflow Variable dbt_notify_recipients.

    Addresses live in Airflow, never in the repository: meta.notify only holds team ids.
    """
    return notify_map.get(team) or notify_map.get("data_platform") or []
