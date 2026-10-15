"""Monthly amount of table_3: actual months plus a forecast of the next FORECAST_MONTHS.

l2: reads only from the l1 monthly model of its own layer.

Python model: fits a linear trend (numpy.polyfit) on the monthly total and projects it.
Deliberately simple and explainable; swap in a proper forecasting library (Prophet,
statsmodels...) by adding it to `packages` when the use case needs it.
  - Fewer than MIN_MONTHS_FOR_FORECAST months of history: no forecast rows.
  - The forecast never goes below zero.

Gold exception: dbt does not support contracts on Python models, so the YAML disables
the contract inherited from dbt_project.yml. The columns are still documented and tested.

Python models in this template:
  - Run on BigQuery (Dataproc), Snowflake (Snowpark), Databricks and DuckDB. They are
    disabled on Postgres and Redshift, which do not support them (see the YAML).
  - Work on small, aggregated inputs: to_pandas() collects the input in memory.
  - Are always materialized as tables (dbt has no Python views): hence the tb_ prefix.
"""
import numpy as np
import pandas as pd

FORECAST_MONTHS = 3
MIN_MONTHS_FOR_FORECAST = 2


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


def forecast(actuals):
    """Linear trend on the month index, projected FORECAST_MONTHS months ahead."""
    if len(actuals) < MIN_MONTHS_FOR_FORECAST:
        return actuals.iloc[0:0]

    month_index = np.arange(len(actuals))
    slope, intercept = np.polyfit(month_index, actuals["total_amount"].to_numpy(), deg=1)

    future_index = np.arange(len(actuals), len(actuals) + FORECAST_MONTHS)
    last_month = actuals["month_start_date"].iloc[-1]
    return pd.DataFrame({
        "month_start_date": [last_month + pd.DateOffset(months=i + 1) for i in range(FORECAST_MONTHS)],
        "total_amount": np.clip(slope * future_index + intercept, 0, None),
        "is_forecast": True,
    })


def model(dbt, session):
    dbt.config(materialized="table", packages=["pandas", "numpy"])
    adapter_type = dbt.config.get("meta")["adapter_type"]

    monthly = to_pandas(dbt.ref("tb_gld_l1_use_case_2__monthly_amount"))
    monthly.columns = [column.lower() for column in monthly.columns]
    actuals = monthly[["month_start_date", "total_amount"]].copy()
    actuals["month_start_date"] = pd.to_datetime(actuals["month_start_date"])
    actuals["total_amount"] = actuals["total_amount"].astype(float)
    actuals = actuals.sort_values("month_start_date").reset_index(drop=True)
    actuals["is_forecast"] = False

    result = pd.concat([actuals, forecast(actuals)], ignore_index=True)
    result["is_forecast"] = result["is_forecast"].astype(bool)

    # Back to DATE: pandas keeps dates as datetime64, which would be written as a timestamp.
    result["month_start_date"] = result["month_start_date"].dt.date
    # Python models have no access to run_started_at: this is the time the model ran (UTC).
    result["_dbt_run_at"] = pd.Timestamp.now(tz="UTC").tz_localize(None)

    return finalize(result[["month_start_date", "total_amount", "is_forecast", "_dbt_run_at"]], adapter_type)
