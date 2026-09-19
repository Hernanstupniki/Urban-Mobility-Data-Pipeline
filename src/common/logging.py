"""Small structured logging helper with stable fields for every job."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any


def log_event(job: str, stage: str, status: str, **fields: Any) -> None:
    event = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "job": job,
        "stage": stage,
        "status": status,
        **{key: value for key, value in fields.items() if value is not None},
    }
    print(json.dumps(event, default=str, sort_keys=True), flush=True)
