"""Optional monthly route-share table processing."""
from __future__ import annotations

import datetime as dt
import os

import numpy as np
import pandas as pd

try:
    import arcpy
except ImportError:
    arcpy = None

from utils import date_to_noon_datetime


def month_floor(d: dt.date) -> dt.date:
    return dt.date(d.year, d.month, 1)


def add_months(d: dt.date, months: int) -> dt.date:
    y = d.year + (d.month - 1 + months) // 12
    m = (d.month - 1 + months) % 12 + 1
    return dt.date(y, m, 1)


def build_monthly_route_share(trip_legs: pd.DataFrame, reference_ridership: pd.DataFrame, *, min_month: str | None = None) -> pd.DataFrame:
    """Compare trip-app/sample activity against monthly reference ridership.

    This step is optional and exists to show how the publishing workflow can create
    a non-spatial hosted table alongside hosted feature layers.
    """
    if trip_legs.empty or reference_ridership.empty:
        return pd.DataFrame()

    legs = trip_legs.copy()
    legs["trip_date"] = pd.to_datetime(legs["trip_date"], errors="coerce")
    legs["month_start"] = legs["trip_date"].dt.to_period("M").dt.to_timestamp().dt.date
    legs["route"] = legs["route_short_name"].astype("string").str.strip()
    legs = legs[(legs["mode"].astype("string") == "Transit") & legs["route"].notna()].copy()

    monthly_sample = (
        legs.groupby(["month_start", "route"], dropna=False)
        .size()
        .reset_index(name="sample_trips")
    )

    ref = reference_ridership.copy()
    ref["month_start"] = pd.to_datetime(ref["curr_month"], errors="coerce").dt.date
    ref["route"] = ref["route"].astype("string").str.strip()
    ref["ridership"] = pd.to_numeric(ref["ridership"], errors="coerce").fillna(0.0)

    if min_month:
        min_dt = pd.to_datetime(min_month).date()
        monthly_sample = monthly_sample[monthly_sample["month_start"] >= min_dt]
        ref = ref[ref["month_start"] >= min_dt]

    out = monthly_sample.merge(ref[["month_start", "route", "route_name", "ridership"]], on=["month_start", "route"], how="inner")
    if out.empty:
        return out

    out["share_sample_trips"] = np.where(out["ridership"] > 0, out["sample_trips"] / out["ridership"], np.nan)
    out["pct_share_sample_trips"] = out["share_sample_trips"] * 100.0
    out["Year"] = pd.to_datetime(out["month_start"]).dt.year
    out["Month"] = pd.to_datetime(out["month_start"]).dt.month
    out = out.rename(
        columns={
            "month_start": "Month_Start",
            "route": "Route",
            "route_name": "Route_Name",
            "sample_trips": "Sample_Trips",
            "ridership": "Ridership",
            "share_sample_trips": "Share_Sample_Trips",
            "pct_share_sample_trips": "Pct_Share_Sample_Trips",
        }
    )
    return out[["Year", "Month", "Month_Start", "Route", "Route_Name", "Sample_Trips", "Ridership", "Share_Sample_Trips", "Pct_Share_Sample_Trips"]]


def write_monthly_route_share_table(df: pd.DataFrame, out_table: str) -> str:
    _require_arcpy()
    if arcpy.Exists(out_table):
        arcpy.management.Delete(out_table)
    out_gdb, out_name = os.path.dirname(out_table), os.path.basename(out_table)
    arcpy.management.CreateTable(out_gdb, out_name)

    field_defs = [
        ("Year", "LONG", None),
        ("Month", "LONG", None),
        ("Month_Start", "DATE", None),
        ("Route", "TEXT", 50),
        ("Route_Name", "TEXT", 200),
        ("Sample_Trips", "LONG", None),
        ("Ridership", "DOUBLE", None),
        ("Share_Sample_Trips", "DOUBLE", None),
        ("Pct_Share_Sample_Trips", "DOUBLE", None),
    ]
    for name, ftype, length in field_defs:
        if ftype == "TEXT":
            arcpy.management.AddField(out_table, name, ftype, field_length=length)
        else:
            arcpy.management.AddField(out_table, name, ftype)

    fields = [x[0] for x in field_defs]
    with arcpy.da.InsertCursor(out_table, fields) as cur:
        for _, row in df.iterrows():
            cur.insertRow(tuple(_clean(row.get(f), is_date=f == "Month_Start") for f in fields))
    return out_table


def _clean(value, *, is_date=False):
    if is_date:
        return date_to_noon_datetime(value)
    if value is None or pd.isna(value):
        return None
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        if np.isnan(value) or np.isinf(value):
            return None
        return float(value)
    return value.item() if hasattr(value, "item") else value


def _require_arcpy() -> None:
    if arcpy is None:
        raise ImportError("arcpy is required for this operation. Run from the ArcGIS Pro Python environment.")
