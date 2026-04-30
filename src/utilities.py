"""General utilities shared across the pipeline."""
from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


def compute_window(lag_days: int, analysis_days: int) -> tuple[dt.date, dt.date, dt.date]:
    """Compute a UTC-aligned rolling analysis window.

    Returns ``(start_date, end_inclusive, end_exclusive)``.
    """
    today_utc = dt.datetime.utcnow().date()
    end_incl = today_utc - dt.timedelta(days=lag_days)
    start = end_incl - dt.timedelta(days=analysis_days - 1)
    end_excl = end_incl + dt.timedelta(days=1)
    return start, end_incl, end_excl


def date_to_noon_datetime(value: Any) -> dt.datetime | None:
    """Convert a date-like value to noon local datetime for ArcGIS DATE fields.

    Storing dates at noon avoids common web-map display shifts where midnight UTC
    can appear as the prior local calendar day.
    """
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return None
    if isinstance(value, pd.Timestamp):
        if pd.isna(value):
            return None
        value = value.date()
    if isinstance(value, dt.datetime):
        value = value.date()
    if isinstance(value, dt.date):
        return dt.datetime(value.year, value.month, value.day, 12, 0, 0)
    return None


def apply_time_safeguards(
    df: pd.DataFrame,
    start_date: dt.date,
    end_inclusive: dt.date,
    *,
    future_cushion_hours: int,
    max_leg_minutes: int,
) -> pd.DataFrame:
    """Validate timestamps, durations, future records, and analysis-window bounds."""
    if df.empty:
        return df

    out = df.copy()
    out["start_time_utc"] = pd.to_datetime(out["start_time_utc"], errors="coerce", utc=True)
    out["end_time_utc"] = pd.to_datetime(out["end_time_utc"], errors="coerce", utc=True)
    out = out.dropna(subset=["start_time_utc", "end_time_utc"]).copy()

    max_allowed = pd.Timestamp.now(tz="UTC") + pd.Timedelta(hours=future_cushion_hours)
    out = out[out["start_time_utc"] <= max_allowed].copy()

    out["Trip_Date"] = out["start_time_utc"].dt.date
    out = out[(out["Trip_Date"] >= start_date) & (out["Trip_Date"] <= end_inclusive)].copy()

    out["time_min"] = (out["end_time_utc"] - out["start_time_utc"]).dt.total_seconds() / 60.0
    out = out[out["time_min"].notna()].copy()
    out = out[(out["time_min"] >= 0) & (out["time_min"] <= max_leg_minutes)].copy()
    return out


def safe_float(value: Any) -> float:
    """Convert a value to a finite float or ``np.nan``."""
    try:
        if value is None or pd.isna(value):
            return np.nan
        v = float(value)
        if np.isnan(v) or np.isinf(v):
            return np.nan
        return v
    except Exception:
        return np.nan


def haversine_miles(lon1: Any, lat1: Any, lon2: Any, lat2: Any) -> float:
    """Return haversine distance in miles between two WGS84 coordinate pairs."""
    try:
        lon1 = float(lon1); lat1 = float(lat1); lon2 = float(lon2); lat2 = float(lat2)
    except Exception:
        return np.nan
    rlon1, rlat1, rlon2, rlat2 = map(np.radians, [lon1, lat1, lon2, lat2])
    dlon = rlon2 - rlon1
    dlat = rlat2 - rlat1
    a = np.sin(dlat / 2.0) ** 2 + np.cos(rlat1) * np.cos(rlat2) * np.sin(dlon / 2.0) ** 2
    c = 2 * np.arcsin(np.sqrt(a))
    return float(3958.7613 * c)


def read_state(path: str | Path) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {}
    return json.loads(p.read_text(encoding="utf-8"))


def write_state(path: str | Path, state: dict[str, Any]) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(state, indent=2), encoding="utf-8")
