"""Daily amount of table_3 per source database, flagged when it is anomalous.

l3: reads only from the l2 daily aggregate of its own layer.

Python model: a rolling z-score over a calendar window is a few lines of pandas and
awkward in portable SQL (window frames by date range differ between warehouses).
A day is anomalous when its amount is more than Z_SCORE_THRESHOLD standard deviations
away from the mean of the previous WINDOW_DAYS calendar days of the same source
database. Days without enough history (MIN_DAYS_WITH_DATA) are never flagged.

Python models in this template:
  - Run on BigQuery (Dataproc), Snowflake (Snowpark), Databricks and DuckDB. They are
    disabled on Postgres and Redshift, which do not support them (see the YAML).
  - Work on small, aggregated inputs: to_pandas() collects the input in memory.
    For large inputs, use the warehouse's native DataFrame API instead.
  - Are always materialized as tables (dbt has no Python views): hence the tb_ prefix.
"""
import pandas as pd

WINDOW_DAYS = 7
MIN_DAYS_WITH_DATA = 3
Z_SCORE_THRESHOLD = 3.0


def to_pandas(df):
    """Converts the DataFrame returned by dbt.ref() to pandas, whatever the warehouse."""
    for method in ("to_pandas", "toPandas", "df"):  # Snowpark, PySpark (BigQuery, Databricks), DuckDB
        if hasattr(df, method):
            return getattr(df, method)()
    return df


def finalize(df, adapter_type):
    """Snowflake stores lowercase pandas columns as quoted, case-sensitive names."""
    if adapter_type == "snowflake":
        df.columns = [column.upper() for column in df.columns]
    return df


def add_baseline(group):
    """Mean and standard deviation of the previous WINDOW_DAYS calendar days (current day excluded)."""
    amounts = group.set_index("event_date")["total_amount"]
    window = amounts.rolling(f"{WINDOW_DAYS}D", closed="left", min_periods=MIN_DAYS_WITH_DATA)
    group = group.copy()
    group["baseline_mean"] = window.mean().to_numpy()
    group["baseline_std"] = window.std().to_numpy()
    return group


def model(dbt, session):
    dbt.config(materialized="table", packages=["pandas"])
    adapter_type = dbt.config.get("meta")["adapter_type"]

    daily = to_pandas(dbt.ref("vw_slv_l2_business_entity_2__agg_table_3_daily"))
    daily.columns = [column.lower() for column in daily.columns]
    daily = daily[["event_date", "source_database", "record_count", "total_amount"]].copy()
    daily["event_date"] = pd.to_datetime(daily["event_date"])
    daily["total_amount"] = daily["total_amount"].astype(float)
    daily = daily.sort_values(["source_database", "event_date"])

    daily = pd.concat(
        [add_baseline(group) for _, group in daily.groupby("source_database")],
        ignore_index=True,
    )

    # A zero standard deviation (flat history) gives no z-score instead of a division by zero.
    daily["z_score"] = (daily["total_amount"] - daily["baseline_mean"]) / daily["baseline_std"].where(
        daily["baseline_std"] > 0
    )
    daily["is_anomaly"] = daily["z_score"].abs().gt(Z_SCORE_THRESHOLD).fillna(False).astype(bool)

    # Back to DATE: pandas keeps dates as datetime64, which would be written as a timestamp.
    daily["event_date"] = daily["event_date"].dt.date
    # Python models have no access to run_started_at: this is the time the model ran (UTC).
    daily["_dbt_run_at"] = pd.Timestamp.now(tz="UTC").tz_localize(None)

    columns = [
        "event_date",
        "source_database",
        "record_count",
        "total_amount",
        "baseline_mean",
        "baseline_std",
        "z_score",
        "is_anomaly",
        "_dbt_run_at",
    ]
    return finalize(daily[columns], adapter_type)
