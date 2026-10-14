"""Stubs of dbt's cross-database macros (dbt.<macro>) for SQLFluff."""


def safe_cast(field, type_):
    return f"CAST({field} AS {type_})"


def dateadd(datepart, interval, from_date_or_timestamp):
    return f"DATEADD({datepart}, {interval}, {from_date_or_timestamp})"


def date_trunc(datepart, date):
    return f"DATE_TRUNC('{datepart}', {date})"


def current_timestamp():
    return "CURRENT_TIMESTAMP"
