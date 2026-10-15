"""Airflow DAGs of poc_dbt, generated from the exposures of target/manifest.json.

The exposures are the scheduling contract (see dbt_airflow_helpers.py and the header of
any _<use_case>__exposures.yml):

  daily / weekly / monthly tag  -> shared DAG poc_dbt__<schedule> (dbt build --select +tag:<schedule>)
  custom tag + meta.cron        -> own DAG poc_dbt__<exposure>     (dbt build --select +exposure:<name>)

Every DAG runs:

  1. source_freshness  dbt source freshness of the sources it needs. A source past its
                       error_after stops the DAG before building on stale data.
  2. snapshot          (daily DAG only) dbt snapshot: history is captured once a day.
  3. build             dbt build of everything its exposures need.
  4. notify_failures   always: e-mails the meta.notify team of every exposure affected by
                       a failed node (following the lineage), with its criticality.
  5. check_ready_by    always: e-mails the meta.notify team of every exposure whose
                       meta.ready_by (UTC) passed before the run finished.
  6. alert__<exposure> for exposures with meta.consumer: alert: e-mails the rows of the
                       model they read, only when it returns rows.

dbt prepares the data; this DAG decides when to run it and whom to tell. Warehouse-agnostic:
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
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from dbt_airflow_helpers import (  # noqa: E402
    alert_email,
    dag_specs,
    failure_email,
    failures_by_team,
    late_email,
    late_exposures,
    load_json,
    parse_dbt_show_json,
    recipients,
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


def dbt_env(target_subdir="target"):
    return {
        **os.environ,
        "DBT_PROFILES_DIR": str(PROJECT_DIR),
        "DBT_TARGET": os.environ.get("DBT_TARGET", "prod"),
        "DBT_TARGET_PATH": "{{ ti.xcom_pull(task_ids='prepare') }}/" + target_subdir,
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


def notify_map():
    return Variable.get("dbt_notify_recipients", default_var={}, deserialize_json=True)


def prepare(**context):
    path = run_dir(context)
    Path(path).mkdir(parents=True, exist_ok=True)
    return path


def notify_failures(spec, **context):
    run_results_path = Path(context["ti"].xcom_pull(task_ids="prepare")) / "target" / "run_results.json"
    if not run_results_path.exists():
        return  # dbt did not get to run (e.g. freshness failed): Airflow's own alerting covers it
    teams = notify_map()
    failures = failures_by_team(load_json(run_results_path), load_json(MANIFEST_PATH), spec["exposures"])
    for team, team_failures in failures.items():
        to = recipients(team, teams)
        if to:
            subject, body = failure_email(team, team_failures, spec["dag_id"])
            send_email(to=to, subject=subject, html_content=body)


def check_ready_by(spec, **context):
    finished_at = datetime.now(timezone.utc)
    run_date = context["data_interval_end"].date()
    late = late_exposures(spec["exposures"], run_date, finished_at)
    teams = notify_map()
    for team in sorted({e["notify"] for e in late}):
        team_late = [e for e in late if e["notify"] == team]
        to = recipients(team, teams)
        if to:
            subject, body = late_email(team, team_late, spec["dag_id"], finished_at)
            send_email(to=to, subject=subject, html_content=body)


def send_alert(alert, **context):
    path = context["ti"].xcom_pull(task_ids="prepare")
    output = subprocess.run(
        [DBT_BIN, "show", "--select", alert["model"], "--limit", "500", "--output", "json", "--log-format", "json"],
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
    email = alert_email(alert["exposure"], parse_dbt_show_json(output))
    if email is None:
        return  # nothing to report
    to = recipients(alert["exposure"]["notify"], notify_map())
    if to:
        send_email(to=to, subject=email[0], html_content=email[1])


def build_dag(spec):
    with DAG(
        dag_id=spec["dag_id"],
        description=f"dbt build of everything {', '.join(e['name'] for e in spec['exposures'])} need(s)",
        schedule=spec["cron"],
        start_date=datetime(2026, 1, 1),
        catchup=False,
        max_active_runs=1,
        default_args=DEFAULT_ARGS,
        tags=["dbt", "poc_dbt"],
    ) as dag:
        prepare_task = PythonOperator(task_id="prepare", python_callable=prepare)
        freshness = dbt_task("source_freshness", f"source freshness --select {spec['select']}")
        build = dbt_task("build", f"build --select {spec['select']} --exclude resource_type:snapshot")
        notify = PythonOperator(task_id="notify_failures", python_callable=notify_failures,
                                op_kwargs={"spec": spec}, trigger_rule="all_done")
        ready = PythonOperator(task_id="check_ready_by", python_callable=check_ready_by,
                               op_kwargs={"spec": spec}, trigger_rule="all_done")

        if spec["snapshot"]:
            snapshot = dbt_task("snapshot", "snapshot")
            prepare_task >> freshness >> snapshot >> build
        else:
            prepare_task >> freshness >> build
        build >> [notify, ready]

        for alert in spec["alerts"]:
            build >> PythonOperator(task_id=f"alert__{alert['exposure']['name']}", python_callable=send_alert,
                                    op_kwargs={"alert": alert})
    return dag


if MANIFEST_PATH.exists():
    for _spec in dag_specs(load_json(MANIFEST_PATH)):
        globals()[_spec["dag_id"]] = build_dag(_spec)
