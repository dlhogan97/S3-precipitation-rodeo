from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Dict, Any, List
import yaml

from .paths import CONFIG_DIR, RAW_DIR

@dataclass
class DatasetEntry:
    key: str
    type: str  # point | grid | model
    raw_path: Optional[str] = None
    raw_glob: Optional[str] = None
    meta_path: Optional[str] = None
    timezone: str = "UTC"
    crs: str = "EPSG:4326"
    loader: str = "xarray_nc"
    variables: Optional[Dict[str, Any]] = None
    time_name: Optional[str] = None
    lat_name: Optional[str] = None
    lon_name: Optional[str] = None

    def expand_paths(self) -> "DatasetEntry":
        if self.raw_path:
            self.raw_path = str((Path(self.raw_path) if self.raw_path.startswith("/") else (RAW_DIR.parent / self.raw_path)).resolve())
        if self.raw_glob:
            self.raw_glob = str((Path(self.raw_glob) if self.raw_glob.startswith("/") else (RAW_DIR.parent / self.raw_glob)).resolve())
        if self.meta_path:
            self.meta_path = str((Path(self.meta_path) if self.meta_path.startswith("/") else (RAW_DIR.parent / self.meta_path)).resolve())
        return self

class DatasetRegistry:
    def __init__(self, cfg_path: Optional[Path] = None):
        self.cfg_path = cfg_path or (CONFIG_DIR / "datasets.yml")
        with open(self.cfg_path, "r") as f:
            raw = yaml.safe_load(f) or {}
        self._entries: Dict[str, DatasetEntry] = {}
        for key, val in raw.items():
            self._entries[key] = DatasetEntry(key=key, **val).expand_paths()

    def get(self, key: str) -> DatasetEntry:
        if key not in self._entries:
            raise KeyError(f"Dataset '{key}' not found in registry {self.cfg_path}")
        return self._entries[key]

    def list(self) -> List[str]:
        return list(self._entries.keys())

__all__ = ["DatasetRegistry", "DatasetEntry"]
