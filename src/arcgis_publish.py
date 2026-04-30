"""ArcGIS Pro / ArcGIS Online publishing helpers."""
from __future__ import annotations

import os
import time
from pathlib import Path

try:
    import arcpy
except ImportError:
    arcpy = None


def ensure_fgdb(gdb_path: str) -> str:
    """Create the configured file geodatabase if it does not already exist."""
    _require_arcpy()
    if arcpy.Exists(gdb_path):
        return gdb_path
    folder = os.path.dirname(gdb_path)
    name = os.path.basename(gdb_path)
    os.makedirs(folder, exist_ok=True)
    arcpy.management.CreateFileGDB(folder, name)
    if not arcpy.Exists(gdb_path):
        raise RuntimeError(f"Failed to create file geodatabase: {gdb_path}")
    return gdb_path


def feature_count(dataset_path: str) -> int:
    _require_arcpy()
    return int(arcpy.management.GetCount(dataset_path)[0])


def safe_overwrite_feature_layer(cfg: dict, local_fc: str, service_name: str) -> None:
    """Refuse to overwrite a hosted layer when the local feature class is empty."""
    count = feature_count(local_fc)
    if count == 0:
        raise RuntimeError(f"ABORT: {local_fc} has 0 features — refusing to overwrite '{service_name}'.")
    overwrite_hosted_item(cfg, local_fc, service_name, item_type="feature")


def safe_overwrite_table(cfg: dict, local_table: str, service_name: str) -> None:
    """Refuse to overwrite a hosted table when the local table is empty."""
    count = feature_count(local_table)
    if count == 0:
        raise RuntimeError(f"ABORT: {local_table} has 0 rows — refusing to overwrite '{service_name}'.")
    overwrite_hosted_item(cfg, local_table, service_name, item_type="table")


def overwrite_hosted_item(cfg: dict, local_dataset: str, service_name: str, *, item_type: str) -> None:
    """Overwrite an ArcGIS Online hosted feature layer or table from an APRX map."""
    _require_arcpy()
    arcgis_cfg = cfg["arcgis"]
    aprx_path = arcgis_cfg["aprx_path"]
    map_name = arcgis_cfg["map_name"]
    scratch_dir = Path(arcgis_cfg["scratch_dir"])
    portal_server_connection = arcgis_cfg.get("portal_server_connection", "My Hosted Services")

    if not os.path.exists(aprx_path):
        raise FileNotFoundError(f"APRX not found: {aprx_path}")
    if not arcpy.Exists(local_dataset):
        raise FileNotFoundError(f"Local dataset not found: {local_dataset}")

    scratch_dir.mkdir(parents=True, exist_ok=True)
    aprx = arcpy.mp.ArcGISProject(aprx_path)
    maps = aprx.listMaps(map_name)
    if not maps:
        raise ValueError(f"Map '{map_name}' not found in APRX.")
    mp = maps[0]

    _clear_map(mp)
    mp.addDataFromPath(local_dataset)
    aprx.save()

    sddraft_path = scratch_dir / f"{service_name}.sddraft"
    sd_path = scratch_dir / f"{service_name}.sd"
    for path in [sddraft_path, sd_path]:
        if path.exists():
            path.unlink()

    sharing_draft = mp.getWebLayerSharingDraft(
        server_type="HOSTING_SERVER",
        service_type="FEATURE",
        service_name=service_name,
    )
    sharing_draft.overwriteExistingService = True
    sharing_draft.summary = "Automated mobility spatial decision-support output"
    sharing_draft.tags = "mobility, GIS, spatial analysis, planning, dashboard"
    sharing_draft.exportToSDDraft(str(sddraft_path))

    arcpy.StageService_server(str(sddraft_path), str(sd_path))
    upload_service_definition_with_retries(str(sd_path), portal_server_connection)
    print(f"Overwrote hosted {item_type}: {service_name}")


def upload_service_definition_with_retries(sd_path: str, server_connection: str, *, tries: int = 4, base_sleep_s: float = 8.0) -> None:
    """Retry service-definition uploads when Portal/AGOL timeouts occur."""
    _require_arcpy()
    last_err: Exception | None = None
    for attempt in range(1, tries + 1):
        try:
            arcpy.UploadServiceDefinition_server(sd_path, server_connection)
            return
        except Exception as exc:  # ArcPy raises generic exceptions for many portal failures.
            last_err = exc
            msg = str(exc).lower()
            transient = any(term in msg for term in ["status code 28", "took too long", "timed out", "timeout"])
            if attempt < tries and transient:
                sleep_s = base_sleep_s * (2 ** (attempt - 1))
                print(f"Upload timeout/transient error. Retry {attempt}/{tries} in {sleep_s:.0f}s...")
                time.sleep(sleep_s)
                continue
            raise
    if last_err:
        raise last_err


def _clear_map(mp) -> None:
    for lyr in mp.listLayers():
        try:
            mp.removeLayer(lyr)
        except Exception:
            pass
    for table in mp.listTables():
        try:
            mp.removeTable(table)
        except Exception:
            pass


def _require_arcpy() -> None:
    if arcpy is None:
        raise ImportError("arcpy is required for publishing. Run from the ArcGIS Pro Python environment.")
