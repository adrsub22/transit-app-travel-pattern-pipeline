"""Data access functions.
This module intentionally treats SQL as one possible source for cleaned trip-leg
records.
"""
from __future__ import annotations

import datetime as dt
from pathlib import Path
from typing import Any

import pandas as pd
import pyodbc


def connect_sql_server(db_cfg: dict[str, Any]) -> pyodbc.Connection:
    """Create a SQL Server connection from config placeholders."""
    trusted = db_cfg.get("trusted_connection", True)
    conn_parts = [
        f"DRIVER={{{db_cfg['driver']}}}",
        f"SERVER={db_cfg['server']}",
        f"DATABASE={db_cfg['database']}",
    ]
    if trusted:
        conn_parts.append("Trusted_Connection=yes")
    else:
        conn_parts.append(f"UID={db_cfg['username']}")
        conn_parts.append(f"PWD={db_cfg['password']}")
    cn = pyodbc.connect(";".join(conn_parts) + ";")
    cn.autocommit = True
    return cn


def load_trip_legs(cfg: dict[str, Any], start_date: dt.date, end_exclusive: dt.date) -> pd.DataFrame:
    """Load cleaned trip-leg records from SQL Server or CSV.

    Required columns are documented in ``docs/data_dictionary.md``.
    """
    db_cfg = cfg["database"]
    input_mode = db_cfg.get("input_mode", "sql_server").lower()

    if input_mode == "csv":
        return _load_trip_legs_csv(db_cfg["sample_csv_path"])
    if input_mode == "sql_server":
        with connect_sql_server(db_cfg) as cn:
            return fetch_clean_trip_legs_sql(
                cn,
                db_cfg["trip_leg_table"],
                start_date,
                end_exclusive,
                future_cushion_hours=cfg["safeguards"]["future_cushion_hours"],
            )
    raise ValueError(f"Unsupported database.input_mode: {input_mode}")


def _load_trip_legs_csv(csv_path: str | Path) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    for col in ["start_time_utc", "end_time_utc", "trip_date"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    return df


def fetch_clean_trip_legs_sql(
    cn: pyodbc.Connection,
    trip_leg_table: str,
    start_date: dt.date,
    end_exclusive: dt.date,
    *,
    future_cushion_hours: int,
) -> pd.DataFrame:
    """Fetch cleaned trip-leg records from a user-provided table.

    This query expects one row per trip leg and performs a conservative de-dupe
    using common identifying fields. Replace the table name through config rather
    than hardcoding any internal schema in public code.
    """
    sql = f"""
    ;WITH base AS (
        SELECT
            user_trip_id,
            start_time_utc,
            end_time_utc,
            start_longitude,
            start_latitude,
            end_longitude,
            end_latitude,
            start_stop_name,
            end_stop_name,
            service_name,
            route_short_name,
            mode,
            manhattan_distance_mi,
            Origin_BG,
            Dest_BG,
            trip_date,
            ROW_NUMBER() OVER (
                PARTITION BY
                    user_trip_id, start_time_utc, end_time_utc,
                    start_longitude, start_latitude, end_longitude, end_latitude
                ORDER BY user_trip_id
            ) AS rn
        FROM {trip_leg_table}
        WHERE trip_date >= ?
          AND trip_date <  ?
          AND start_time_utc <= DATEADD(HOUR, ?, SYSUTCDATETIME())
          AND user_trip_id IS NOT NULL
          AND start_time_utc IS NOT NULL
          AND end_time_utc IS NOT NULL
    )
    SELECT
        user_trip_id,
        start_time_utc,
        end_time_utc,
        start_longitude,
        start_latitude,
        end_longitude,
        end_latitude,
        start_stop_name,
        end_stop_name,
        service_name,
        route_short_name,
        mode,
        manhattan_distance_mi,
        Origin_BG,
        Dest_BG,
        trip_date
    FROM base
    WHERE rn = 1
    """
    return pd.read_sql(sql, cn, params=[start_date, end_exclusive, future_cushion_hours])


def load_reference_monthly_ridership(cfg: dict[str, Any]) -> pd.DataFrame:
    """Load optional reference monthly ridership for route-share calculations."""
    ref = cfg.get("ridership_reference", {})
    if not ref.get("enabled", False):
        return pd.DataFrame()

    mode = ref.get("input_mode", "csv").lower()
    if mode == "csv":
        df = pd.read_csv(ref["sample_csv_path"])
    elif mode == "sql_server":
        cn = pyodbc.connect(f"DSN={ref['dsn']};")
        try:
            sql = f"""
                SELECT curr_month, route, route_name, SUM(monthly_ridership) AS ridership
                FROM {ref['table']}
                GROUP BY curr_month, route, route_name
            """
            df = pd.read_sql(sql, cn)
        finally:
            cn.close()
    else:
        raise ValueError(f"Unsupported ridership_reference.input_mode: {mode}")

    df["curr_month"] = pd.to_datetime(df["curr_month"], errors="coerce").dt.date
    df["route"] = df["route"].astype("string").str.strip()
    df["route_name"] = df["route_name"].astype("string")
    df["ridership"] = pd.to_numeric(df["ridership"], errors="coerce").fillna(0.0)
    return df.dropna(subset=["curr_month", "route"])
