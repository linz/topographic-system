"""Helpers for deriving feature identities.

The reproducible UUIDv7 generation mirrors kart-import's ``uuid7`` module.
"""

import hashlib
import uuid
from datetime import date

import geopandas as gpd
import pandas as pd


def reproducible_uuid7(timestamp_ms: int, text: str) -> uuid.UUID:
    """Generate a reproducible UUIDv7 from a millisecond timestamp and text.

    The timestamp fills the leading 48 bits; the remaining bits come from the
    SHA-256 hash of ``text`` so the same inputs always produce the same id.
    """
    uuid_bytes = bytearray(16)
    uuid_bytes[0:6] = timestamp_ms.to_bytes(6, byteorder="big")
    uuid_bytes[6:16] = hashlib.sha256(text.encode("utf-8")).digest()[0:10]
    uuid_bytes[6] = (uuid_bytes[6] & 0x0F) | 0x70  # version 7
    uuid_bytes[8] = (uuid_bytes[8] & 0x3F) | 0x80  # variant 10
    return uuid.UUID(bytes=bytes(uuid_bytes))


def earliest_created_at(gdf: gpd.GeoDataFrame) -> date:
    """Return the earliest ``created_at`` date in the source."""
    if "created_at" not in gdf.columns:
        raise ValueError("Source has no created_at column; cannot derive a stable created_at.")
    created_at = pd.to_datetime(gdf["created_at"], errors="coerce").dropna()
    if created_at.empty:
        raise ValueError("Source has no valid created_at values; cannot derive a stable created_at.")
    return created_at.min().date()
