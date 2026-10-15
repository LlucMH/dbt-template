"""Airflow DAGs of poc_dbt: one DAG per schedule used by the exposures.

Generated from target/manifest.json (built by Cloud Build with the deployment target):
every schedule tag found on an exposure (daily, weekly, monthly; see SCHEDULES in
dbt_airflow_helpers.py) becomes a DAG `poc_dbt__<schedule>` that:

  1. source_freshness  dbt source freshness of the sources it needs. A source past its
                       error_after fails the DAG before building on stale data.
  2. snapshot          (daily only) dbt snapshot: history is captured once a day.
  3. build             dbt build --select +tag:<schedule>: every model, test and seed
                       the exposures of that schedule need.
  4. notify_failures   always runs: e-mails each team (meta.notify) the nodes it owns
                       that failed, from target/run_results.json.
  5. send_anomaly_alert (daily only, when the alerts model exists): e-mails the anomalies
                       of the latest day if the alerts view returns rows.

dbt prepares the data; this DAG decides when to run it and what to send. Warehouse-agnostic:
the DAG only calls dbt and reads its artifacts.

Environment (Composer environment variables): DBT_ADAPTER, DBT_TARGET (prod) and the
connection variables of the warehouse (see .env.example). Optional: DBT_BIN (dbt executable,
e.g. a dedicated virtualenv, to keep dbt's dependencies apart from Airflow's) and
DBT_RUNS_DIR (where each run writes its artifacts; must be shared by every worker).
Airflow Variable dbt_notify_recipients: JSON {team: [e-mails]}, e.g.
{"team_use_case_2": ["uc2@company.com"], "data_platform": ["data@company.com"]}.
"""
import os
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from dbt_airflow_helpers import (  # noqa: E402
    SCHEDULES,
    anomaly_email,
    failure_email,
    failures_by_team,
    load_json,
    parse_dbt_show_json,
    recipients,
    schedules_in_use,
)

from airflow import DAG  # noqa: E402
from airflow.models import Variable  # noqa: E402

try:  # Airflow 3
    from airflow.providers.standard.operators.bash import BashOperator
    from airflow.providers.standard.operators.python import PythonOperator
except ImportError:  # Airflow 2
    from airflow.operators.bash import BashOperator
    from airflow.operators.python import PythonOperator

from airflow.utils.email import send_email  # noqa: E402

PROJECT_DIR = Path(__file__).resolve().parents[1]
MANIFEST_PATH = PROJECT_DIR / "target" / "manifest.json"
ALERTS_MODEL = "vw_gld_l0_use_case_2__amount_anomaly_alerts"

DBT_BIN = os.environ.get("DBT_BIN", "dbt")
# Composer: /home/airflow/gcs/data is shared by every worker (unlike /tmp).
RUNS_DIR = os.environ.get("DBT_RUNS_DIR", "/home/airflow/gcs/data/dbt_runs")

DEFAULT_ARGS = {
    "owner": "data_platform",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}


def run_dir(context):
    """Artifacts folder of one DAG run: target/ and logs/ never go to the dags bucket."""
    return f"{RUNS_DIR}/{context['dag'].dag_id}/{context['run_id']}".replace(":", "_")


def dbt_env():
    return {
        **os.environ,
        "DBT_PROFILES_DIR": str(PROJECT_DIR),
        "DBT_TARGET": os.environ.get("DBT_TARGET", "prod"),
        "DBT_TARGET_PATH": "{{ ti.xcom_pull(task_ids='prepare') }}/target",
        "DBT_LOG_PATH": "{{ ti.xcom_pull(task_ids='prepare') }}/logs",
    }


def dbt_task(task_id, command, **kwargs):
    return BashOperator(
        task_id=task_id,
        bash_command=f"cd '{PROJECT_DIR}' && {DBT_BIN} {command}",
        env=dbt_env(),
        append_env=False,
        **kwargs,
    )


def prepare(**context):
    path = run_dir(context)
    Path(path).mkdir(parents=True, exist_ok=True)
    return path


def notify_failures(schedule, **context):
    run_results_path = Path(context["ti"].xcom_pull(task_ids="prepare")) / "target" / "run_results.json"
    if not run_results_path.exists():
        return  # dbt did not get to run (e.g. freshness failed): Airflow's own alerting covers it
    notify_map = Variable.get("dbt_notify_recipients", default_var={}, deserialize_json=True)
    for team, failures in failures_by_team(load_json(run_results_path), load_json(MANIFEST_PATH)).items():
        subject, body = failure_email(team, failures, schedule)
        to = recipients(team, notify_map)
        if to:
            send_email(to=to, subject=subject, html_content=body)


def send_anomaly_alert(**context):
    path = context["ti"].xcom_pull(task_ids="prepare")
    output = subprocess.run(
        [DBT_BIN, "show", "--select", ALERTS_MODEL, "--limit", "500", "--output", "json", "--log-format", "json"],
        cwd=PROJECT_DIR,
        env={
            **os.environ,
            "DBT_PROFILES_DIR": str(PROJECT_DIR),
            "DBT_TARGET": os.environ.get("DBT_TARGET", "prod"),
            "DBT_TARGET_PATH": f"{path}/target_show",
            "DBT_LOG_PATH": f"{path}/logs",
        },
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    email = anomaly_email(parse_dbt_show_json(output))
    if email is None:
        return  # no anomalies on the latest day: nothing to send
    notify_map = Variable.get("dbt_notify_recipients", default_var={}, deserialize_json=True)
    to = recipients("team_use_case_2", notify_map)
    if to:
        send_email(to=to, subject=email[0], html_content=email[1])


def build_dag(schedule, manifest):
    alerts_enabled = any(node.get("name") == ALERTS_MODEL for node in manifest.get("nodes", {}).values())
    with DAG(
        dag_id=f"poc_dbt__{schedule}",
        description=f"dbt build of everything the {schedule} exposures of poc_dbt need",
        schedule=SCHEDULES[schedule],
        start_date=datetime(2026, 1, 1),
        catchup=False,
        max_active_runs=1,
        default_args=DEFAULT_ARGS,
        tags=["dbt", "poc_dbt", schedule],
    ) as dag:
        prepare_task = PythonOperator(task_id="prepare", python_callable=prepare)
        freshness = dbt_task("source_freshness", f"source freshness --select +tag:{schedule}")
        build = dbt_task("build", f"build --select +tag:{schedule} --exclude resource_type:snapshot")
        notify = PythonOperator(
            task_id="notify_failures",
            python_callable=notify_failures,
            op_kwargs={"schedule": schedule},
            trigger_rule="all_done",
        )

        if schedule == "daily":
            snapshot = dbt_task("snapshot", "snapshot")
            prepare_task >> freshness >> snapshot >> build >> notify
        else:
            prepare_task >> freshness >> build >> notify

        if schedule == "daily" and alerts_enabled:
            alert = PythonOperator(task_id="send_anomaly_alert", python_callable=send_anomaly_alert)
            build >> alert
    return dag


if MANIFEST_PATH.exists():
    _manifest = load_json(MANIFEST_PATH)
    for _schedule in schedules_in_use(_manifest):
        globals()[f"poc_dbt__{_schedule}"] = build_dag(_schedule, _manifest)
