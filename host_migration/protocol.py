"""Canonical hashing for verifiable state transfer."""

from __future__ import annotations

import hashlib
import json


def canonical(obj) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"))


def state_hash(snapshot: dict) -> str:
    """Hash of a world snapshot plus its input boundary."""
    return hashlib.sha256(canonical(snapshot).encode()).hexdigest()


def short(h: str) -> str:
    return h[:12] if h else "-"
