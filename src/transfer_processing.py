"""Transfer-chain and transfer-hotspot processing."""
from __future__ import annotations

import os

import numpy as np
import pandas as pd

try:
    import arcpy
except ImportError:
    arcpy = None

from utils import date_to_noon_datetime, haversine_miles, safe_float


def build_transfer_chain_summary(
    df: pd.DataFrame,
    *,
    transfer_prefix: str | None,
    fixed_route_service: str,
    on_demand_service: str,
    max_transfer_gap_minutes: int,
    max_transfer_jump_miles: float,
    same_route_collapse_max_minutes: int,
) -> pd.DataFrame:
    """Build one transfer event per completed chain and aggregate to hotspots."""
    if df.empty:
        return df

    prepared = _prepare_transfer_dataframe(df, on_demand_service)
    anchor_services = {fixed_route_service, on_demand_service}
    events: list[dict] = []

    for (_, trip_date), g in prepared.groupby(["user_trip_id", "Trip_Date"], sort=False):
        anchor = _anchor_legs(g, anchor_services, same_route_collapse_max_minutes, on_demand_service)
        if len(anchor) < 2:
            continue
        if not _valid_chain(anchor, transfer_prefix, max_transfer_gap_minutes, max_transfer_jump_miles):
            continue

        chain_routes = _route_chain(anchor["Route_No"].tolist(), on_demand_service)
        first_leg = anchor.iloc[0]
        second_leg = anchor.iloc[1]
        last_leg = anchor.iloc[-1]
        total_gap = _total_chain_gap(anchor)

        events.append(
            {
                "Trip_Date": trip_date,
                "Route_No": chain_routes[0],
                "From_Route": chain_routes[0],
                "To_Route": chain_routes[-1],
                "Transfer_Pattern": " to ".join(chain_routes),
                "Transfer_Type": _classify_type(first_leg["Service_Name"], last_leg["Service_Name"], fixed_route_service, on_demand_service),
                "Transfer_Stop": _transfer_stop(first_leg, on_demand_service),
                "Leg_Chains": max(len(chain_routes) - 1, 0),
                "Route_Count": len(chain_routes),
                "Transfer_Time": total_gap,
                "Manhattan_Distance": 0.0,
                "Longitude": float(first_leg["end_longitude"]),
                "Latitude": float(first_leg["end_latitude"]),
            }
        )

    if not events:
        return pd.DataFrame()

    ev = pd.DataFrame(events)
    gcols = [
        "Trip_Date", "Route_No", "From_Route", "To_Route", "Transfer_Pattern",
        "Transfer_Type", "Transfer_Stop", "Leg_Chains", "Route_Count",
    ]
    return (
        ev.groupby(gcols, dropna=False)
        .agg(
            Transfer_Count=("Trip_Date", "size"),
            Avg_Transfer_Time=("Transfer_Time", "mean"),
            Min_Transfer_Time=("Transfer_Time", "min"),
            Max_Transfer_Time=("Transfer_Time", "max"),
            Median_Transfer_Time=("Transfer_Time", "median"),
            Avg_Distance=("Manhattan_Distance", "mean"),
            Min_Distance=("Manhattan_Distance", "min"),
            Max_Distance=("Manhattan_Distance", "max"),
            Median_Distance=("Manhattan_Distance", "median"),
            Longitude=("Longitude", "mean"),
            Latitude=("Latitude", "mean"),
        )
        .reset_index()
    )


def build_transfer_unified_summary(
    df: pd.DataFrame,
    *,
    transfer_prefix: str | None,
    fixed_route_service: str,
    on_demand_service: str,
    max_transfer_gap_minutes: int,
    max_transfer_jump_miles: float,
    same_route_collapse_max_minutes: int,
) -> pd.DataFrame:
    """Build one event per transfer step within a chain and aggregate metrics.

    Wait metrics are based on gaps between consecutive anchor legs. Travel metrics
    are based on the actual leg durations/distances, using a distance field when
    available and haversine fallback when needed.
    """
    if df.empty:
        return df

    prepared = _prepare_transfer_dataframe(df, on_demand_service)
    anchor_services = {fixed_route_service, on_demand_service}
    events: list[dict] = []

    for (user_trip_id, trip_date), g in prepared.groupby(["user_trip_id", "Trip_Date"], sort=False):
        anchor = _anchor_legs(g, anchor_services, same_route_collapse_max_minutes, on_demand_service)
        if len(anchor) < 2:
            continue
        if not _valid_chain(anchor, transfer_prefix, max_transfer_gap_minutes, max_transfer_jump_miles):
            continue

        chain_routes = _route_chain(anchor["Route_No"].tolist(), on_demand_service)
        transfer_pattern = " to ".join(chain_routes)
        chain_gap = _total_chain_gap(anchor)
        chain_dist = float(np.nansum([_leg_travel_dist_mi(row) for _, row in anchor.iterrows()]))
        chain_time = float(np.nansum([_leg_travel_time_min(row) for _, row in anchor.iterrows()]))
        chain_id = f"{user_trip_id}|{trip_date}"
        pattern_transfer_type = _classify_type(anchor.iloc[0]["Service_Name"], anchor.iloc[-1]["Service_Name"], fixed_route_service, on_demand_service)

        for i in range(len(anchor) - 1):
            a = anchor.iloc[i]
            b = anchor.iloc[i + 1]
            from_route = _normalize_route(a["Route_No"], on_demand_service)
            to_route = _normalize_route(b["Route_No"], on_demand_service)
            gap_min = (b["start_time_utc"] - a["end_time_utc"]).total_seconds() / 60.0

            events.append(
                {
                    "Trip_Date": trip_date,
                    "Chain_ID": chain_id,
                    "Transfer_Stop": _transfer_stop(a, on_demand_service),
                    "From_Route": from_route,
                    "To_Route": to_route,
                    "Transfer_Seq": i + 1,
                    "Leg_Transfer_Pattern": f"{from_route} to {to_route}",
                    "Transfer_Pattern": transfer_pattern,
                    "Pattern_From_Route": chain_routes[0],
                    "Pattern_To_Route": chain_routes[-1],
                    "Transfer_Type": _classify_type(a["Service_Name"], b["Service_Name"], fixed_route_service, on_demand_service),
                    "Pattern_Transfer_Type": pattern_transfer_type,
                    "Leg_Chains": max(len(chain_routes) - 1, 0),
                    "Route_Count": len(chain_routes),
                    "Gap_Min": float(gap_min),
                    "Chain_Gap_Min": float(chain_gap),
                    "Leg_Dist": _leg_travel_dist_mi(a),
                    "Leg_Time": _leg_travel_time_min(a),
                    "Chain_Dist": chain_dist,
                    "Chain_Time": chain_time,
                    "Longitude": float(a["end_longitude"]),
                    "Latitude": float(a["end_latitude"]),
                }
            )

    if not events:
        return pd.DataFrame()

    ev = pd.DataFrame(events)
    pattern_counts = (
        ev.groupby(["Trip_Date", "Transfer_Pattern"], dropna=False)["Chain_ID"]
        .nunique()
        .reset_index()
        .rename(columns={"Chain_ID": "Pattern_Count"})
    )
    gcols = [
        "Trip_Date", "Transfer_Stop", "From_Route", "To_Route", "Transfer_Seq",
        "Leg_Transfer_Pattern", "Transfer_Pattern", "Pattern_From_Route", "Pattern_To_Route",
        "Transfer_Type", "Pattern_Transfer_Type", "Leg_Chains", "Route_Count",
    ]
    agg = (
        ev.groupby(gcols, dropna=False)
        .agg(
            Hotspot_Count=("Trip_Date", "size"),
            Avg_Gap_Min=("Gap_Min", "mean"),
            Avg_Chain_Gap_Min=("Chain_Gap_Min", "mean"),
            Avg_Chain_Dist=("Chain_Dist", "mean"),
            Avg_Chain_Time=("Chain_Time", "mean"),
            leg_avg_dist=("Leg_Dist", "mean"),
            leg_avg_time=("Leg_Time", "mean"),
            Longitude=("Longitude", "mean"),
            Latitude=("Latitude", "mean"),
        )
        .reset_index()
    )
    return agg.merge(pattern_counts, on=["Trip_Date", "Transfer_Pattern"], how="left")


def write_transfer_chain_table(df: pd.DataFrame, out_table: str) -> str:
    field_defs = [
        ("Trip_Date", "DATE", None), ("Route_No", "TEXT", 50), ("From_Route", "TEXT", 50),
        ("To_Route", "TEXT", 50), ("Transfer_Pattern", "TEXT", 200), ("Transfer_Type", "TEXT", 40),
        ("Transfer_Stop", "TEXT", 200), ("Leg_Chains", "LONG", None), ("Route_Count", "LONG", None),
        ("Transfer_Count", "LONG", None), ("Avg_Transfer_Time", "DOUBLE", None),
        ("Min_Transfer_Time", "DOUBLE", None), ("Max_Transfer_Time", "DOUBLE", None),
        ("Median_Transfer_Time", "DOUBLE", None), ("Avg_Distance", "DOUBLE", None),
        ("Min_Distance", "DOUBLE", None), ("Max_Distance", "DOUBLE", None), ("Median_Distance", "DOUBLE", None),
        ("Longitude", "DOUBLE", None), ("Latitude", "DOUBLE", None),
    ]
    return _write_table(df, out_table, field_defs, date_fields={"Trip_Date"})


def write_transfer_unified_table(df: pd.DataFrame, out_table: str) -> str:
    field_defs = [
        ("Trip_Date", "DATE", None), ("Transfer_Stop", "TEXT", 200), ("From_Route", "TEXT", 50),
        ("To_Route", "TEXT", 50), ("Transfer_Seq", "LONG", None), ("Leg_Transfer_Pattern", "TEXT", 120),
        ("Transfer_Pattern", "TEXT", 200), ("Pattern_From_Route", "TEXT", 50), ("Pattern_To_Route", "TEXT", 50),
        ("Transfer_Type", "TEXT", 40), ("Pattern_Transfer_Type", "TEXT", 40), ("Leg_Chains", "LONG", None),
        ("Route_Count", "LONG", None), ("Hotspot_Count", "LONG", None), ("Pattern_Count", "LONG", None),
        ("Avg_Gap_Min", "DOUBLE", None), ("Avg_Chain_Gap_Min", "DOUBLE", None),
        ("Avg_Chain_Dist", "DOUBLE", None), ("Avg_Chain_Time", "DOUBLE", None),
        ("leg_avg_dist", "DOUBLE", None), ("leg_avg_time", "DOUBLE", None),
        ("Longitude", "DOUBLE", None), ("Latitude", "DOUBLE", None),
    ]
    return _write_table(df, out_table, field_defs, date_fields={"Trip_Date"})


def build_points_fc(in_table: str, out_fc: str) -> str:
    _require_arcpy()
    if arcpy.Exists(out_fc):
        arcpy.management.Delete(out_fc)
    sr_wgs84 = arcpy.SpatialReference(4326)
    arcpy.management.XYTableToPoint(in_table, out_fc, "Longitude", "Latitude", coordinate_system=sr_wgs84)
    arcpy.management.DefineProjection(out_fc, sr_wgs84)
    return out_fc


def _prepare_transfer_dataframe(df: pd.DataFrame, on_demand_service: str) -> pd.DataFrame:
    out = df.copy()
    out["start_time_utc"] = pd.to_datetime(out["start_time_utc"], errors="coerce", utc=True)
    out["end_time_utc"] = pd.to_datetime(out["end_time_utc"], errors="coerce", utc=True)
    if "Trip_Date" not in out.columns:
        out["Trip_Date"] = out["start_time_utc"].dt.date
    out["Service_Name"] = out["service_name"].astype("string").fillna("(UNKNOWN)")
    out["Route_No"] = out["route_short_name"].astype("string")
    out.loc[out["Service_Name"] == on_demand_service, "Route_No"] = on_demand_service
    out["Route_No"] = out["Route_No"].fillna("(NA)")
    for c in ["start_longitude", "start_latitude", "end_longitude", "end_latitude", "manhattan_distance_mi"]:
        out[c] = pd.to_numeric(out[c], errors="coerce")
    return out


def _anchor_legs(g: pd.DataFrame, anchor_services: set[str], same_route_collapse_max_minutes: int, on_demand_service: str) -> pd.DataFrame:
    g = g.sort_values("start_time_utc").reset_index(drop=True)
    anchor_full = g[g["Service_Name"].isin(anchor_services)].reset_index(drop=True)
    if len(anchor_full) < 2:
        return pd.DataFrame()

    keep_rows = [anchor_full.iloc[0]]
    for i in range(1, len(anchor_full)):
        prev = keep_rows[-1]
        curr = anchor_full.iloc[i]
        prev_route = _normalize_route(prev["Route_No"], on_demand_service)
        curr_route = _normalize_route(curr["Route_No"], on_demand_service)
        gap_min = (curr["start_time_utc"] - prev["end_time_utc"]).total_seconds() / 60.0
        if prev_route == curr_route and 0 <= gap_min <= same_route_collapse_max_minutes:
            continue
        keep_rows.append(curr)
    return pd.DataFrame(keep_rows).reset_index(drop=True)


def _valid_chain(anchor: pd.DataFrame, transfer_prefix: str | None, max_gap: int, max_jump_miles: float) -> bool:
    for i in range(len(anchor) - 1):
        a = anchor.iloc[i]
        b = anchor.iloc[i + 1]
        gap_min = (b["start_time_utc"] - a["end_time_utc"]).total_seconds() / 60.0
        if gap_min < 0 or gap_min > max_gap:
            return False
        jump_mi = haversine_miles(a["end_longitude"], a["end_latitude"], b["start_longitude"], b["start_latitude"])
        if np.isfinite(jump_mi) and jump_mi > max_jump_miles:
            return False
        if transfer_prefix:
            if not str(a.get("Dest_BG", "")).startswith(str(transfer_prefix)):
                return False
            if not str(b.get("Origin_BG", "")).startswith(str(transfer_prefix)):
                return False
    return True


def _total_chain_gap(anchor: pd.DataFrame) -> float:
    total = 0.0
    for i in range(len(anchor) - 1):
        total += (anchor.iloc[i + 1]["start_time_utc"] - anchor.iloc[i]["end_time_utc"]).total_seconds() / 60.0
    return float(total)


def _leg_travel_dist_mi(row: pd.Series) -> float:
    md = safe_float(row.get("manhattan_distance_mi"))
    if np.isfinite(md) and md > 0:
        return float(md)
    return haversine_miles(row.get("start_longitude"), row.get("start_latitude"), row.get("end_longitude"), row.get("end_latitude"))


def _leg_travel_time_min(row: pd.Series) -> float:
    try:
        return float((row["end_time_utc"] - row["start_time_utc"]).total_seconds() / 60.0)
    except Exception:
        return np.nan


def _route_chain(values: list, on_demand_service: str) -> list[str]:
    return [_normalize_route(v, on_demand_service) for v in values]


def _normalize_route(value, on_demand_service: str) -> str:
    text = str(value).strip()
    if text.lower() in {on_demand_service.lower(), "on demand", "microtransit"}:
        return on_demand_service
    return text or "(NA)"


def _classify_type(from_service: str, to_service: str, fixed_route_service: str, on_demand_service: str) -> str:
    fs = str(from_service).strip()
    ts = str(to_service).strip()
    if fs == fixed_route_service and ts == fixed_route_service:
        return "Fixed to Fixed"
    if fs == on_demand_service and ts == on_demand_service:
        return "On Demand to On Demand"
    if fs == on_demand_service and ts == fixed_route_service:
        return "On Demand to Fixed"
    if fs == fixed_route_service and ts == on_demand_service:
        return "Fixed to On Demand"
    return "Other"


def _transfer_stop(row: pd.Series, on_demand_service: str) -> str:
    if str(row.get("Service_Name")) == on_demand_service:
        return on_demand_service
    val = row.get("end_stop_name")
    if val is None or pd.isna(val):
        return "(UNKNOWN_STOP)"
    return str(val)


def _require_arcpy() -> None:
    if arcpy is None:
        raise ImportError("arcpy is required for this operation. Run from the ArcGIS Pro Python environment.")


def _write_table(df: pd.DataFrame, out_table: str, field_defs: list[tuple[str, str, int | None]], *, date_fields: set[str]) -> str:
    _require_arcpy()
    if arcpy.Exists(out_table):
        arcpy.management.Delete(out_table)
    out_gdb, out_name = os.path.dirname(out_table), os.path.basename(out_table)
    arcpy.management.CreateTable(out_gdb, out_name)
    for name, ftype, length in field_defs:
        if ftype == "TEXT":
            arcpy.management.AddField(out_table, name, ftype, field_length=length)
        else:
            arcpy.management.AddField(out_table, name, ftype)

    fields = [x[0] for x in field_defs]
    with arcpy.da.InsertCursor(out_table, fields) as cur:
        for _, row in df.iterrows():
            cur.insertRow(tuple(_clean(row.get(f), is_date=f in date_fields) for f in fields))
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
