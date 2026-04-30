"""Origin-destination processing and ArcGIS feature creation."""
from __future__ import annotations

import os

import numpy as np
import pandas as pd

try:
    import arcpy
except ImportError:  # Allows non-ArcGIS environments to inspect non-ArcPy logic.
    arcpy = None

from utils import date_to_noon_datetime


def build_od_daily_summary(
    df: pd.DataFrame,
    *,
    origin_destination_prefix: str | None,
    fixed_route_service: str,
    on_demand_service: str,
) -> pd.DataFrame:
    """Aggregate trip legs into daily OD records suitable for line features."""
    if df.empty:
        return df

    out = df.copy()
    if origin_destination_prefix:
        like = str(origin_destination_prefix)
        out = out[
            out["Origin_BG"].astype("string").str.startswith(like, na=False)
            & out["Dest_BG"].astype("string").str.startswith(like, na=False)
        ].copy()

    out["Origin_BG"] = out["Origin_BG"].astype("string")
    out["Dest_BG"] = out["Dest_BG"].astype("string")
    out["Mode"] = out["mode"].astype("string").fillna("(UNKNOWN)")
    out["Route_No"] = out["route_short_name"].astype("string").fillna("(NA)")
    out["Service_Name"] = out["service_name"].astype("string").fillna("(UNKNOWN)")

    for c in ["start_longitude", "start_latitude", "end_longitude", "end_latitude", "manhattan_distance_mi"]:
        out[c] = pd.to_numeric(out[c], errors="coerce")

    out = out.dropna(
        subset=[
            "Trip_Date",
            "user_trip_id",
            "Origin_BG",
            "Dest_BG",
            "start_longitude",
            "start_latitude",
            "end_longitude",
            "end_latitude",
        ]
    ).copy()

    anchor_services = {fixed_route_service, on_demand_service}
    out = out.groupby(["user_trip_id", "Trip_Date"], group_keys=False).apply(
        lambda g: _associate_to_anchor_service(g, anchor_services, on_demand_service)
    )

    gcols = [
        "Trip_Date",
        "Origin_BG",
        "Dest_BG",
        "Service_Name",
        "Mode",
        "Route_No",
        "Assoc_Service",
        "Assoc_Route",
    ]
    agg = (
        out.groupby(gcols, dropna=False)
        .agg(
            Trip_Count=("user_trip_id", "nunique"),
            Avg_Man_Dist=("manhattan_distance_mi", "mean"),
            Min_Man_Dist=("manhattan_distance_mi", "min"),
            Max_Man_Dist=("manhattan_distance_mi", "max"),
            Median_Man_Dist=("manhattan_distance_mi", "median"),
            Avg_Time=("time_min", "mean"),
            Min_Time=("time_min", "min"),
            Max_Time=("time_min", "max"),
            Median_Time=("time_min", "median"),
            mean_start_lon=("start_longitude", "mean"),
            mean_start_lat=("start_latitude", "mean"),
            mean_end_lon=("end_longitude", "mean"),
            mean_end_lat=("end_latitude", "mean"),
        )
        .reset_index()
    )
    return agg


def _associate_to_anchor_service(g: pd.DataFrame, anchor_services: set[str], on_demand_service: str) -> pd.DataFrame:
    """Associate non-anchor legs to the nearest fixed/on-demand service leg."""
    g = g.sort_values("start_time_utc").copy()
    is_anchor = g["Service_Name"].isin(anchor_services)

    prev_service = g["Service_Name"].where(is_anchor).ffill()
    prev_route = g["Route_No"].where(is_anchor).ffill()
    prev_end = g["end_time_utc"].where(is_anchor).ffill()

    next_service = g["Service_Name"].where(is_anchor).bfill()
    next_route = g["Route_No"].where(is_anchor).bfill()
    next_start = g["start_time_utc"].where(is_anchor).bfill()

    gap_prev = (g["start_time_utc"] - prev_end).dt.total_seconds() / 60.0
    gap_next = (next_start - g["end_time_utc"]).dt.total_seconds() / 60.0

    assoc_service = pd.Series(["(NONE)"] * len(g), index=g.index, dtype="string")
    assoc_route = pd.Series(["(NA)"] * len(g), index=g.index, dtype="string")

    non_anchor = ~is_anchor
    has_prev = non_anchor & prev_service.notna()
    has_next = non_anchor & next_service.notna()
    both = has_prev & has_next
    only_prev = has_prev & ~has_next
    only_next = has_next & ~has_prev

    gp = gap_prev.where(gap_prev >= 0, np.inf)
    gn = gap_next.where(gap_next >= 0, np.inf)
    choose_prev = both & (gp <= gn)
    choose_next = both & (gn < gp)

    assoc_service.loc[only_prev | choose_prev] = prev_service.loc[only_prev | choose_prev].astype("string")
    assoc_route.loc[only_prev | choose_prev] = prev_route.loc[only_prev | choose_prev].astype("string")
    assoc_service.loc[only_next | choose_next] = next_service.loc[only_next | choose_next].astype("string")
    assoc_route.loc[only_next | choose_next] = next_route.loc[only_next | choose_next].astype("string")

    assoc_service.loc[is_anchor] = g.loc[is_anchor, "Service_Name"].astype("string")
    assoc_route.loc[is_anchor] = g.loc[is_anchor, "Route_No"].astype("string")
    assoc_route.loc[assoc_service == on_demand_service] = on_demand_service

    g["Assoc_Service"] = assoc_service.fillna("(NONE)")
    g["Assoc_Route"] = assoc_route.fillna("(NA)")
    return g


def build_od_destination_summary(od_summary: pd.DataFrame) -> pd.DataFrame:
    """Summarize destination geography activity by origin and destination."""
    if od_summary is None or od_summary.empty:
        return pd.DataFrame()

    df = od_summary.copy()
    df["Trip_Count"] = pd.to_numeric(df["Trip_Count"], errors="coerce").fillna(0).astype(int)
    df["Avg_Time"] = pd.to_numeric(df["Avg_Time"], errors="coerce")
    df["Avg_Man_Dist"] = pd.to_numeric(df["Avg_Man_Dist"], errors="coerce")
    df["wt_time"] = df["Avg_Time"] * df["Trip_Count"]
    df["wt_dist"] = df["Avg_Man_Dist"] * df["Trip_Count"]

    g = (
        df.groupby(["Origin_BG", "Dest_BG"], dropna=False)
        .agg(Trip_Count=("Trip_Count", "sum"), wt_time=("wt_time", "sum"), wt_dist=("wt_dist", "sum"))
        .reset_index()
    )
    g["Avg_Time"] = np.where(g["Trip_Count"] > 0, g["wt_time"] / g["Trip_Count"], np.nan)
    g["Avg_Man_Dist"] = np.where(g["Trip_Count"] > 0, g["wt_dist"] / g["Trip_Count"], np.nan)
    g = g.drop(columns=["wt_time", "wt_dist"])
    total_by_origin = g.groupby("Origin_BG")["Trip_Count"].transform("sum")
    g["Pct_of_Origin"] = np.where(total_by_origin > 0, g["Trip_Count"] / total_by_origin, np.nan)
    g["Dest_Rank"] = g.groupby("Origin_BG")["Trip_Count"].rank(method="dense", ascending=False).astype("Int64")
    return g


def write_od_table(df: pd.DataFrame, out_table: str) -> str:
    _require_arcpy()
    if arcpy.Exists(out_table):
        arcpy.management.Delete(out_table)
    out_gdb, out_name = os.path.dirname(out_table), os.path.basename(out_table)
    arcpy.management.CreateTable(out_gdb, out_name)

    field_defs = [
        ("Trip_Date", "DATE", None),
        ("Origin_BG", "TEXT", 12),
        ("Dest_BG", "TEXT", 12),
        ("Service_Name", "TEXT", 100),
        ("Mode", "TEXT", 50),
        ("Route_No", "TEXT", 50),
        ("Assoc_Service", "TEXT", 100),
        ("Assoc_Route", "TEXT", 50),
        ("Trip_Count", "LONG", None),
    ]
    for f in [
        "Avg_Man_Dist", "Min_Man_Dist", "Max_Man_Dist", "Median_Man_Dist",
        "Avg_Time", "Min_Time", "Max_Time", "Median_Time",
        "mean_start_lon", "mean_start_lat", "mean_end_lon", "mean_end_lat",
    ]:
        field_defs.append((f, "DOUBLE", None))
    _add_fields(out_table, field_defs)
    _insert_dataframe(out_table, df, [x[0] for x in field_defs], date_fields={"Trip_Date"})
    return out_table


def build_lines_fc(in_table: str, out_fc: str) -> str:
    _require_arcpy()
    if arcpy.Exists(out_fc):
        arcpy.management.Delete(out_fc)
    sr_wgs84 = arcpy.SpatialReference(4326)
    arcpy.management.XYToLine(
        in_table=in_table,
        out_featureclass=out_fc,
        startx_field="mean_start_lon",
        starty_field="mean_start_lat",
        endx_field="mean_end_lon",
        endy_field="mean_end_lat",
        line_type="GEODESIC",
        attributes="ATTRIBUTES",
        spatial_reference=sr_wgs84,
    )
    arcpy.management.DefineProjection(out_fc, sr_wgs84)
    return out_fc


def write_destination_summary_table(df: pd.DataFrame, out_table: str) -> str:
    _require_arcpy()
    if arcpy.Exists(out_table):
        arcpy.management.Delete(out_table)
    out_gdb, out_name = os.path.dirname(out_table), os.path.basename(out_table)
    arcpy.management.CreateTable(out_gdb, out_name)
    field_defs = [
        ("Origin_BG", "TEXT", 12),
        ("Dest_BG", "TEXT", 12),
        ("Trip_Count", "LONG", None),
        ("Avg_Time", "DOUBLE", None),
        ("Avg_Man_Dist", "DOUBLE", None),
        ("Pct_of_Origin", "DOUBLE", None),
        ("Dest_Rank", "LONG", None),
    ]
    _add_fields(out_table, field_defs)
    _insert_dataframe(out_table, df, [x[0] for x in field_defs])
    return out_table


def build_destination_polygon_fc(bg_fc: str, bg_id_field: str, summary_table: str, out_fc: str) -> str:
    """Join OD destination summaries to geography polygons and copy a clean FC."""
    _require_arcpy()
    if not arcpy.Exists(bg_fc):
        raise FileNotFoundError(f"Geography feature class not found: {bg_fc}")
    if arcpy.Exists(out_fc):
        arcpy.management.Delete(out_fc)

    bg_lyr = "bg_destination_summary_lyr"
    tb_view = "destination_summary_view"
    arcpy.management.MakeFeatureLayer(bg_fc, bg_lyr)
    arcpy.management.MakeTableView(summary_table, tb_view)
    arcpy.management.AddJoin(bg_lyr, bg_id_field, tb_view, "Dest_BG", join_type="KEEP_COMMON")
    arcpy.management.CopyFeatures(bg_lyr, out_fc)
    try:
        arcpy.management.RemoveJoin(bg_lyr)
        arcpy.management.Delete(bg_lyr)
        arcpy.management.Delete(tb_view)
    except Exception:
        pass
    return out_fc


def _require_arcpy() -> None:
    if arcpy is None:
        raise ImportError("arcpy is required for this operation. Run from the ArcGIS Pro Python environment.")


def _add_fields(table: str, field_defs: list[tuple[str, str, int | None]]) -> None:
    for name, ftype, length in field_defs:
        if ftype == "TEXT":
            arcpy.management.AddField(table, name, ftype, field_length=length)
        else:
            arcpy.management.AddField(table, name, ftype)


def _clean_value(value, *, is_date: bool = False):
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


def _insert_dataframe(table: str, df: pd.DataFrame, fields: list[str], *, date_fields: set[str] | None = None) -> None:
    date_fields = date_fields or set()
    with arcpy.da.InsertCursor(table, fields) as cur:
        for _, row in df.iterrows():
            cur.insertRow(tuple(_clean_value(row.get(f), is_date=f in date_fields) for f in fields))
