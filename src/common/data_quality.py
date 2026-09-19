"""Shared data-quality vocabulary used by Silver jobs and tests."""

from __future__ import annotations

import re


NULL_LIKE_VALUES = ("null", "n/a", "na", "none", "-", "")

# Deliberately conservative. Names alone are not classified as PII because a
# reliable name detector requires context; generated accidental PII always uses
# the explicit ``contact:`` marker so it remains detectable and redactable.
_PII_EMAIL_BODY = r"[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}"
_PII_PHONE_BODY = r"(?:\+?\d[\d\s().-]{6,}\d)"
_PII_CONTACT_MARKER_BODY = r"(?:^|[|;])\s*contact\s*:"
PII_EMAIL_PATTERN = rf"(?i){_PII_EMAIL_BODY}"
PII_PHONE_PATTERN = rf"(?i){_PII_PHONE_BODY}"
PII_CONTACT_MARKER_PATTERN = rf"(?i){_PII_CONTACT_MARKER_BODY}"
PII_ANY_PATTERN = rf"(?i)(?:{_PII_EMAIL_BODY})|(?:{_PII_PHONE_BODY})|(?:{_PII_CONTACT_MARKER_BODY})"
PII_CONTACT_SUFFIX_PATTERN = r"(?i)\s*(?:[|;])\s*contact\s*:.*$"

VEHICLE_TYPE_ALIASES = {
    "sedan": "sedan",
    "saloon": "sedan",
    "hatchback": "hatchback",
    "hatch back": "hatchback",
    "motorbike": "motorbike",
    "motorcycle": "motorbike",
    "moto": "motorbike",
    "bike": "motorbike",
}

MAX_ACCEPTANCE_DELAY_MINUTES = 30
MAX_TRIP_DURATION_MINUTES = 180


def contains_potential_pii(value: str | None) -> bool:
    """Return whether a free-text value matches the conservative PII rules."""
    return bool(value and re.search(PII_ANY_PATTERN, value))


def canonical_vehicle_type(value: str | None) -> str | None:
    """Normalize known vehicle-type spelling variants without guessing unknowns."""
    if value is None:
        return None
    normalized = " ".join(value.strip().lower().split())
    return VEHICLE_TYPE_ALIASES.get(normalized, normalized or None)
