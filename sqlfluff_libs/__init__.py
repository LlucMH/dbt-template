"""Stubs of the project macros for the SQLFluff jinja templater.

SQLFluff does not run dbt: with the jinja templater it knows neither the project
macros nor dbt's. These functions return representative SQL so that the linter
can analyse the structure of the models. They do not reproduce the real logic
(which depends on the active warehouse): only enough for the rendered SQL to be
valid.

Functions in this file are available at the root of the jinja context
(data_type(), ...); the ones in dbt.py under the dbt namespace (dbt.safe_cast(), ...).
If a model uses a new project macro, add its stub here.
"""


def data_type(name):
    return name.upper()


def dbt_run_at():
    return "CAST('2000-01-01 00:00:00' AS TIMESTAMP)"


def table_config_by_date(column, cluster_by=()):
    return {}


def incremental_config_by_date(column, cluster_by=()):
    return {}
