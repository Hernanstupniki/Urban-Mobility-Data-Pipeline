"""Policy tests: exactly one module may touch SparkSession internals."""

from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]

SCANNED_DIRS = ["src", "gdpr", "retention", "migrations"]

FORBIDDEN = (
    "SparkSession.builder",
    ".getOrCreate()",
    "configure_spark_with_delta_pip",
    "spark.conf.set",
    "spark-submit",
)

ALLOWED_FILE = REPO_ROOT / "src" / "common" / "spark.py"


def test_only_the_spark_factory_touches_sessions():
    offenders = []
    for rel_dir in SCANNED_DIRS:
        for py in (REPO_ROOT / rel_dir).rglob("*.py"):
            if py.resolve() == ALLOWED_FILE.resolve():
                continue
            text = py.read_text(encoding="utf-8")
            for token in FORBIDDEN:
                if token in text:
                    offenders.append(f"{py.relative_to(REPO_ROOT)}: {token}")
    assert not offenders, "Use src.common.spark.build_spark(): " + "; ".join(offenders)


def test_job_entry_points_stop_their_session():
    missing = []
    for rel_dir in SCANNED_DIRS:
        for py in sorted((REPO_ROOT / rel_dir).rglob("*.py")):
            text = py.read_text(encoding="utf-8")
            if "build_spark(" in text and "__main__" in text and "spark.stop()" not in text:
                missing.append(str(py.relative_to(REPO_ROOT)))
    assert not missing, "Entry points must stop their session in finally: " + ", ".join(missing)
