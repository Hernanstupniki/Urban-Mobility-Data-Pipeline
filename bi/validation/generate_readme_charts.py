"""Render README evidence charts from the published development dataset."""

from __future__ import annotations

from datetime import date, datetime, timezone
from html import escape
from pathlib import Path
import os

import psycopg2


ROOT = Path(__file__).resolve().parents[2]
OUTPUT = ROOT / "docs" / "assets"
TEAL = "#087E78"
BLUE = "#355C7D"
ORANGE = "#D58446"
INK = "#17324A"
MUTED = "#5F7182"
GRID = "#DFE7EC"


def _settings() -> dict[str, str]:
    values = {}
    local_env = ROOT / "infra" / "airflow" / ".env"
    if local_env.exists():
        for line in local_env.read_text().splitlines():
            if "=" in line and not line.lstrip().startswith("#"):
                key, value = line.split("=", 1)
                values[key] = value.strip().strip('"').strip("'")
    return values


def _query(connection, sql: str):
    with connection.cursor() as cursor:
        cursor.execute(sql)
        return cursor.fetchall()


def _text(x, y, value, *, size=13, color=INK, weight="normal", anchor="start"):
    return (f'<text x="{x}" y="{y}" text-anchor="{anchor}" '
            f'fill="{color}" font-family="Arial, sans-serif" '
            f'font-size="{size}" font-weight="{weight}">{escape(str(value))}</text>')


def _document(title: str, subtitle: str, body: list[str], height: int = 310):
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d UTC")
    lines = [
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 960 {height}" '
        f'role="img" aria-label="{escape(title)}">',
        f'<rect width="960" height="{height}" fill="#FFFFFF"/>',
        _text(44, 38, title, size=21, weight="bold"),
        _text(44, 59, subtitle, size=12, color=MUTED),
        *body,
        _text(44, height - 12, f"Synthetic dev data · mobility_dw.reporting · {today}",
              size=10, color=MUTED),
        "</svg>",
    ]
    return "\n".join(lines) + "\n"


def _line_chart(title, subtitle, rows, series, filename, *, percent=False):
    if not rows:
        raise RuntimeError(f"No data for {filename}")
    x0, x1, y0, y1 = 67, 924, 88, 251
    values = [float(row[index]) for row in rows for index, _, _ in series]
    ceiling = max(values) * 1.13
    if ceiling <= 0:
        raise RuntimeError(f"No positive values for {filename}")
    chart = []
    for tick in range(5):
        value = ceiling * tick / 4
        y = y1 - (y1 - y0) * tick / 4
        chart.append(f'<line x1="{x0}" y1="{y:.1f}" x2="{x1}" y2="{y:.1f}" stroke="{GRID}"/>')
        label = f"{value * 100:.0f}%" if percent else f"{value:,.0f}"
        chart.append(_text(x0 - 9, y + 4, label, size=11, color=MUTED, anchor="end"))
    count = len(rows)
    for index in sorted({0, count // 4, count // 2, 3 * count // 4, count - 1}):
        x = x0 + (x1 - x0) * index / max(1, count - 1)
        label = rows[index][0]
        if isinstance(label, (date, datetime)):
            label = label.strftime("%b %d")
        chart.append(_text(f"{x:.1f}", 271, label, size=11, color=MUTED, anchor="middle"))
    for index, label, color in series:
        points = []
        for n, row in enumerate(rows):
            x = x0 + (x1 - x0) * n / max(1, count - 1)
            y = y1 - float(row[index]) / ceiling * (y1 - y0)
            points.append(f"{x:.1f},{y:.1f}")
        chart.append(f'<polyline points="{" ".join(points)}" fill="none" '
                     f'stroke="{color}" stroke-width="2.5" stroke-linejoin="round"/>')
    if len(series) > 1:
        for n, (_, label, color) in enumerate(series):
            x = 682 + n * 120
            chart.append(f'<line x1="{x}" y1="53" x2="{x+22}" y2="53" stroke="{color}" stroke-width="3"/>')
            chart.append(_text(x + 27, 57, label, size=11, color=MUTED))
    (OUTPUT / filename).write_text(_document(title, subtitle, chart))


def _bar_chart(title, subtitle, rows, filename, *, percent=False, floor=0.0, decimals=1):
    if not rows:
        raise RuntimeError(f"No data for {filename}")
    labels = [str(row[0]) for row in rows]
    values = [float(row[1]) for row in rows]
    ceiling = max(values) * 1.12
    if ceiling <= floor:
        raise RuntimeError(f"Invalid scale for {filename}")
    x0, x1, y0 = 205, 865, 95
    row_height = 48
    chart = []
    for n, (label, value) in enumerate(zip(labels, values)):
        y = y0 + n * row_height
        chart.append(_text(x0 - 16, y + 18, label.title(), size=13, anchor="end"))
        chart.append(f'<rect x="{x0}" y="{y}" width="{x1-x0}" height="23" rx="4" fill="#EEF3F5"/>')
        width = max(1, (value - floor) / (ceiling - floor) * (x1 - x0))
        chart.append(f'<rect x="{x0}" y="{y}" width="{width:.1f}" height="23" rx="4" fill="{TEAL}"/>')
        rendered = f"{value * 100:.{decimals}f}%" if percent else f"{value:.{decimals}f}"
        chart.append(_text(x1 + 12, y + 18, rendered, size=12, weight="bold"))
    height = max(282, y0 + len(rows) * row_height + 25)
    (OUTPUT / filename).write_text(_document(title, subtitle, chart, height=height))


def main():
    settings = _settings()
    password = os.getenv("ANALYTICS_DB_PASSWORD") or settings.get("ANALYTICS_DB_PASSWORD")
    if not password:
        raise RuntimeError("ANALYTICS_DB_PASSWORD is required")
    OUTPUT.mkdir(exist_ok=True)
    with psycopg2.connect(
        host=os.getenv("ANALYTICS_DB_HOST", "127.0.0.1"),
        port=int(os.getenv("ANALYTICS_DB_PORT", "5433")),
        dbname=os.getenv("ANALYTICS_DB_NAME", "mobility_dw"),
        user=os.getenv("ANALYTICS_DB_USER", "analytics"),
        password=password,
    ) as connection:
        daily = _query(connection, """
            SELECT d.date::date, count(*)
            FROM reporting.fact_trips t
            JOIN reporting.dim_date d ON d.date_key = t.request_date_key
            GROUP BY 1 ORDER BY 1
        """)
        collection = _query(connection, """
            SELECT m.payment_method_name,
                   sum(p.amount) FILTER (WHERE p.status = 'paid') / nullif(sum(p.amount), 0)
            FROM reporting.fact_payments p
            JOIN reporting.dim_payment_method m USING (payment_method_key)
            GROUP BY 1 HAVING sum(p.amount) > 0 ORDER BY 2 DESC
        """)
        ratings = _query(connection, """
            SELECT v.vehicle_type, avg(r.score) FILTER (WHERE NOT r.score_invalid)
            FROM reporting.fact_ratings r
            JOIN reporting.fact_trips t ON t.trip_id = r.trip_key
            JOIN reporting.dim_vehicle v ON v.vehicle_id = t.vehicle_key
            WHERE v.vehicle_type <> 'UNKNOWN'
            GROUP BY 1
            HAVING count(*) FILTER (WHERE NOT r.score_invalid) > 30
            ORDER BY 2 DESC
        """)
        health = _query(connection, """
            SELECT d.date::date,
                   count(*) FILTER (WHERE t.coordinates_missing
                       OR t.completed_but_ended_at_null OR t.ended_before_started
                       OR t.driver_vehicle_mismatch OR t.is_distance_outlier
                       OR t.is_acceptance_delay_outlier OR t.is_trip_duration_outlier
                       OR t.requested_at_source_invalid)::numeric / count(*),
                   count(*) FILTER (WHERE t.requested_at_source_invalid)::numeric / count(*)
            FROM reporting.fact_trips t
            JOIN reporting.dim_date d ON d.date_key = t.request_date_key
            GROUP BY 1 ORDER BY 1
        """)
    _line_chart("Daily trip demand", "Trip count by request date (UTC calendar)", daily,
                [(1, "Trips", TEAL)], "daily-demand.svg")
    _bar_chart("Collection rate by payment method", "Paid amount / billed amount", collection,
               "collection-by-method.svg", percent=True)
    _bar_chart("Average valid rating by vehicle type", "Valid ratings only; 1–5 scale", ratings,
               "rating-by-vehicle.svg", floor=1.0, decimals=2)
    _line_chart("Data-quality issue rate", "Share of trips by request date (UTC calendar)", health,
                [(1, "Any issue", TEAL), (2, "Invalid date", ORANGE)],
                "data-health-trend.svg", percent=True)
    print(f"Updated four README charts in {OUTPUT}")


if __name__ == "__main__":
    main()
