"""Configuration helpers"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml


def load_config(config_path: str | Path) -> dict[str, Any]:
    """Load a YAML config file and return a dictionary.

    The public repository includes only ``config/config.example.yml``. Copy that file
    to ``config/config.yml`` locally and replace placeholders with your environment.
    """
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")

    with path.open("r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}

    _validate_required_sections(cfg, ["database", "arcgis", "analysis_windows", "geography", "safeguards"])
    return cfg


def _validate_required_sections(cfg: dict[str, Any], sections: list[str]) -> None:
    missing = [s for s in sections if s not in cfg]
    if missing:
        raise ValueError(f"Missing required config section(s): {', '.join(missing)}")


def gdb_path(cfg: dict[str, Any]) -> str:
    """Return the configured local file geodatabase path."""
    arcgis = cfg["arcgis"]
    return str(Path(arcgis["gdb_folder"]) / arcgis["gdb_name"])


def local_dataset_path(cfg: dict[str, Any], key: str) -> str:
    """Return a configured table/feature-class path inside the local FGDB."""
    return str(Path(gdb_path(cfg)) / cfg["local_names"][key])
