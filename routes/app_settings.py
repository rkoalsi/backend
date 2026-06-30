"""Lightweight admin-configurable app settings (singleton doc in `settings`)."""
from ..config.root import get_database

SETTINGS_KEY = "app_config"

# Defaults applied when a value is missing from the stored doc.
DEFAULTS = {
    "min_order_value_self_registered": 5000,
}


def get_settings() -> dict:
    db = get_database()
    doc = db.settings.find_one({"key": SETTINGS_KEY}) or {}
    merged = dict(DEFAULTS)
    for k in DEFAULTS:
        if doc.get(k) is not None:
            merged[k] = doc[k]
    return merged


def update_settings(updates: dict) -> dict:
    db = get_database()
    clean = {k: v for k, v in updates.items() if k in DEFAULTS and v is not None}
    if clean:
        db.settings.update_one({"key": SETTINGS_KEY}, {"$set": clean}, upsert=True)
    return get_settings()


def get_min_order_value_self_registered() -> float:
    try:
        return float(get_settings().get("min_order_value_self_registered") or 0)
    except (TypeError, ValueError):
        return 0.0
