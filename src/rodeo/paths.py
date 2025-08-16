from __future__ import annotations
import os
from pathlib import Path

# Project root is two levels up from this file
ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
EXTERNAL_DIR = DATA_DIR / "external"
CONFIG_DIR = ROOT / "config"

for p in (DATA_DIR, RAW_DIR, PROCESSED_DIR, EXTERNAL_DIR, CONFIG_DIR):
    p.mkdir(parents=True, exist_ok=True)

__all__ = [
    "ROOT",
    "DATA_DIR",
    "RAW_DIR",
    "PROCESSED_DIR",
    "EXTERNAL_DIR",
    "CONFIG_DIR",
]
