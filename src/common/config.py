"""Validated runtime configuration for local and scheduled pipeline jobs."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path, PurePosixPath


_SAFE_NAME = re.compile(r"^[A-Za-z0-9_-]+$")
_SAFE_TABLE = re.compile(r"^[a-z][a-z0-9_]*$")


def env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    value = raw.strip().lower()
    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{name} must be a boolean, got {raw!r}")


def safe_table_list(raw: str) -> list[str]:
    tables = [item.strip() for item in raw.split(",") if item.strip()]
    invalid = [item for item in tables if not _SAFE_TABLE.fullmatch(item)]
    if invalid:
        raise ValueError(f"Unsafe table name(s): {', '.join(invalid)}")
    return tables


def safe_child(base: str | Path, *parts: str) -> str:
    """Return a normalized child path and reject traversal/absolute fragments."""
    clean: list[str] = []
    for part in parts:
        candidate = PurePosixPath(part)
        if candidate.is_absolute() or ".." in candidate.parts:
            raise ValueError(f"Unsafe path fragment: {part!r}")
        clean.extend(piece for piece in candidate.parts if piece not in {"", "."})
    return str(Path(base).joinpath(*clean))


@dataclass(frozen=True)
class Settings:
    env: str
    data_root: str
    db_host: str
    db_port: int
    db_name: str
    db_user: str
    db_password: str | None

    @classmethod
    def from_env(cls, *, require_database: bool = False) -> "Settings":
        env = os.getenv("ENV", "dev").strip()
        if not _SAFE_NAME.fullmatch(env):
            raise ValueError("ENV may only contain letters, numbers, '-' and '_'")
        data_root = os.getenv("DATA_ROOT", "data").strip()
        if not data_root:
            raise ValueError("DATA_ROOT cannot be empty")
        try:
            db_port = int(os.getenv("OLTP_DB_PORT", os.getenv("DB_PORT", "5432")))
        except ValueError as exc:
            raise ValueError("DB_PORT must be an integer") from exc
        if not 1 <= db_port <= 65535:
            raise ValueError("DB_PORT must be between 1 and 65535")
        password = os.getenv("OLTP_DB_PASSWORD", os.getenv("DB_PASSWORD"))
        if require_database and not password:
            raise ValueError("DB_PASSWORD is required for JDBC ingestion")
        return cls(
            env=env,
            data_root=data_root,
            db_host=os.getenv("OLTP_DB_HOST", os.getenv("DB_HOST", "localhost")).strip(),
            db_port=db_port,
            db_name=os.getenv("OLTP_DB_NAME", os.getenv("DB_NAME", "mobility_oltp")).strip(),
            db_user=os.getenv("OLTP_DB_USER", os.getenv("DB_USER", "postgres")).strip(),
            db_password=password,
        )

    @property
    def env_root(self) -> str:
        return safe_child(self.data_root, self.env)

    @property
    def jdbc_url(self) -> str:
        return f"jdbc:postgresql://{self.db_host}:{self.db_port}/{self.db_name}"

    def path(self, *parts: str) -> str:
        return safe_child(self.env_root, *parts)
