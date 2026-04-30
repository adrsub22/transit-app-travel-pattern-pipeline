"""Pipeline entry point.

Run from the ArcGIS Pro Python environment after copying
``config/config.example.yml`` to ``config/config.yml`` and updating placeholders.
"""
from __future__ import annotations

import argparse
from pathlib import Path

try:
    import arcpy
except ImportError:
    arcpy = None

from arcgis_publish import ensure_fgdb, safe_overwrite_feature_layer, safe_overwrite_table
from config_loader import gdb_path, load_config, local_dataset_path
from data_access import load_reference_monthly_ridership, load_trip_legs
from od_processing import (
    build_destination_polygon_fc,
    build_lines_fc,
    build_od_daily_summary,
    build_od_destination_summary,
    write_destination_summary_table,
    write_od_table,
)
from route_share_processing import build_monthly_route_share, write_monthly_route_share_table
from transfer_processing import (
    build_points_fc,
    build_transfer_chain_summary,
    build_transfer_unified_summary,
    write_transfer_chain_table,
    write_transfer_unified_table,
)
from utils import apply_time_safeguards, compute_window


def run_pipeline(config_path: str | Path) -> None:
    cfg = load_config(config_path)
    if arcpy is not None:
        arcpy.env.overwriteOutput = True

    ensure_fgdb(gdb_path(cfg))

    services = cfg["arcgis"]["hosted_services"]
    labels = cfg["service_labels"]
    geo = cfg["geography"]
    safeguards = cfg["safeguards"]

    # OD outputs
    od_start, od_end_incl, od_end_excl = compute_window(
        cfg["analysis_windows"]["od_lag_days"],
        cfg["analysis_windows"]["od_analysis_days"],
    )
    print(f"OD window: {od_start} to {od_end_incl} inclusive")
    od_legs = load_trip_legs(cfg, od_start, od_end_excl)
    od_legs = apply_time_safeguards(
        od_legs,
        od_start,
        od_end_incl,
        future_cushion_hours=safeguards["future_cushion_hours"],
        max_leg_minutes=safeguards["max_leg_minutes"],
    )
    od_summary = build_od_daily_summary(
        od_legs,
        origin_destination_prefix=geo.get("origin_destination_prefix"),
        fixed_route_service=labels["fixed_route"],
        on_demand_service=labels["on_demand"],
    )
    od_table = write_od_table(od_summary, local_dataset_path(cfg, "od_table"))
    od_lines = build_lines_fc(od_table, local_dataset_path(cfg, "od_lines_fc"))
    safe_overwrite_feature_layer(cfg, od_lines, services["od_lines"])

    od_dest = build_od_destination_summary(od_summary)
    od_poly_table = write_destination_summary_table(od_dest, local_dataset_path(cfg, "od_poly_table"))
    od_poly_fc = build_destination_polygon_fc(
        geo["block_group_feature_class"],
        geo["block_group_id_field"],
        od_poly_table,
        local_dataset_path(cfg, "od_poly_fc"),
    )
    safe_overwrite_feature_layer(cfg, od_poly_fc, services["od_destination_polygons"])

    # Transfer outputs
    xfer_start, xfer_end_incl, xfer_end_excl = compute_window(
        cfg["analysis_windows"]["transfer_lag_days"],
        cfg["analysis_windows"]["transfer_analysis_days"],
    )
    print(f"Transfer window: {xfer_start} to {xfer_end_incl} inclusive")
    xfer_legs = load_trip_legs(cfg, xfer_start, xfer_end_excl)
    xfer_legs = apply_time_safeguards(
        xfer_legs,
        xfer_start,
        xfer_end_incl,
        future_cushion_hours=safeguards["future_cushion_hours"],
        max_leg_minutes=safeguards["max_leg_minutes"],
    )

    transfer_chain = build_transfer_chain_summary(
        xfer_legs,
        transfer_prefix=geo.get("transfer_prefix"),
        fixed_route_service=labels["fixed_route"],
        on_demand_service=labels["on_demand"],
        max_transfer_gap_minutes=safeguards["max_transfer_gap_minutes"],
        max_transfer_jump_miles=safeguards["max_transfer_jump_miles"],
        same_route_collapse_max_minutes=safeguards["same_route_collapse_max_minutes"],
    )
    transfer_table = write_transfer_chain_table(transfer_chain, local_dataset_path(cfg, "transfer_table"))
    transfer_points = build_points_fc(transfer_table, local_dataset_path(cfg, "transfer_points_fc"))
    safe_overwrite_feature_layer(cfg, transfer_points, services["transfer_chain"])

    transfer_unified = build_transfer_unified_summary(
        xfer_legs,
        transfer_prefix=geo.get("transfer_prefix"),
        fixed_route_service=labels["fixed_route"],
        on_demand_service=labels["on_demand"],
        max_transfer_gap_minutes=safeguards["max_transfer_gap_minutes"],
        max_transfer_jump_miles=safeguards["max_transfer_jump_miles"],
        same_route_collapse_max_minutes=safeguards["same_route_collapse_max_minutes"],
    )
    unified_table = write_transfer_unified_table(transfer_unified, local_dataset_path(cfg, "transfer_unified_table"))
    unified_points = build_points_fc(unified_table, local_dataset_path(cfg, "transfer_unified_points_fc"))
    safe_overwrite_feature_layer(cfg, unified_points, services["transfer_unified"])

    # Optional monthly route-share table
    if cfg.get("ridership_reference", {}).get("enabled", False):
        reference = load_reference_monthly_ridership(cfg)
        monthly_share = build_monthly_route_share(
            xfer_legs,
            reference,
            min_month=cfg["ridership_reference"].get("min_month"),
        )
        monthly_table = write_monthly_route_share_table(monthly_share, local_dataset_path(cfg, "monthly_share_table"))
        safe_overwrite_table(cfg, monthly_table, services["monthly_route_share"])

    print("Pipeline complete.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run mobility spatial decision-support publishing pipeline.")
    parser.add_argument("--config", default="config/config.yml", help="Path to local YAML config file.")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_pipeline(args.config)
