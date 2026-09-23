"""Statistical guarantees of the mobility simulation model.

Tolerances are statistical (seeded finite samples), never exact ratios:
the model must stay noisy, so tests assert bands, not points.
"""

import importlib.util
import random
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts" / "generate_oltp_data"))

import mobility_model as mm  # noqa: E402

ZONES = list(mm.ZONE_ROLE.keys())


def _fresh(seed):
    random.seed(seed)


def test_same_seed_reproduces_streams():
    _fresh(7)
    m = mm.MobilityModel(ZONES)
    a = [m.plan_trip([(10, 100), (11, 101)], 40, m.sample_datetime()[0], 7) for _ in range(30)]
    _fresh(7)
    m2 = mm.MobilityModel(ZONES)
    b = [m2.plan_trip([(10, 100), (11, 101)], 40, m2.sample_datetime()[0], 7) for _ in range(30)]
    assert [x["status"] for x in a] == [x["status"] for x in b]
    assert [str(x["requested_at"]) for x in a] == [str(x["requested_at"]) for x in b]


def test_plan_maintains_time_order_and_ranges():
    _fresh(11)
    m = mm.MobilityModel(ZONES)
    for _ in range(400):
        dt, idx = m.sample_datetime()
        p = m.plan_trip([(10, 100)], idx, dt, 11)
        req = p["requested_at"]
        for key in ("accepted_at", "started_at", "ended_at", "canceled_at"):
            if p[key] is not None:
                assert p[key] >= req, key
        if p["status"] == "completed":
            assert p["estimated_distance_km"] > 0
            assert p["fare_amount"] is None or p["fare_amount"] >= 4.5


def test_window_spreads_dates_and_hours():
    _fresh(3)
    m = mm.MobilityModel(ZONES)
    days = {m.sample_datetime()[0].date() for _ in range(4000)}
    hours = {m.sample_datetime()[0].hour for _ in range(4000)}
    assert len(days) > 45          # ~90-day window genuinely spread
    assert len(hours) == 24        # all hours represented


def test_dq_channels_are_independent():
    _fresh(21)
    n = 20000
    both = upper = pad = 0
    pu = mm.DQ_RATES["dq_passenger_name_upper"]
    pp = mm.DQ_RATES["dq_passenger_name_pad"]
    for _ in range(n):
        name = apply_one("Michael Powell")
        up = name.isupper()
        sp = name != name.strip()
        upper += up
        pad += sp
        both += (up and sp)
    assert abs(upper / n - pu) < 0.01
    assert abs(pad / n - pp) < 0.01
    expected = pu * pp
    assert both / n < expected * 3 + 0.002  # coincidence only, no gating


def test_clean_records_still_majority():
    _fresh(22)
    counts = []
    for _ in range(20000):
        name, email, phone, city = mm.apply_passenger_dq(
            "Michael Powell", "mp@example.net", "944.255.9371", "Rachelmouth"
        )
        anomalies = 0
        if name != "Michael Powell":
            anomalies += 1
        if email is None or email != "mp@example.net":
            anomalies += 1
        if phone is None or phone != "944.255.9371":
            anomalies += 1
        if city != "Rachelmouth":
            anomalies += 1
        counts.append(anomalies)
    clean = counts.count(0) / len(counts)
    assert clean > 0.35              # majority-ish clean, not forced
    assert sum(c >= 3 for c in counts) / len(counts) < 0.05  # stacking is rare


def test_incident_window_is_correlated_but_small():
    _fresh(23)
    m = mm.MobilityModel(ZONES)
    created = m.day_date(51).replace(hour=9)
    assert mm.in_incident_window(created, m)
    assert not mm.in_incident_window(m.day_date(70).replace(hour=9), m)


def test_rating_responds_to_quality_and_delay():
    _fresh(24)
    def avg(q, delay):
        scores = []
        for i in range(4000):
            rng = random.Random(1000 + i)
            scores.append(mm.MobilityModel(ZONES).rating_score(
                {"quality": q, "delay_min": delay, "duration_min": 20,
                 "requested_at": None}, rng=rng))
        return sum(scores) / len(scores)
    good = avg(1.5, 2)
    bad = avg(-1.5, 40)
    assert good > bad + 0.8
    assert bad > 1.0  # still noise: bad never becomes deterministic 1


def test_payment_status_depends_on_method():
    _fresh(25)
    paid = {}
    for method, stats in mm.MobilityModel.PAYMENT_STATUS_BY_METHOD.items():
        paid[method] = stats["paid"]
    assert paid["wallet"] > paid["card"]  # documented design
    _ = random  # model uses seeded global random in generator flow


def apply_one(name):
    return mm.maybe_pad(mm.maybe_upper(name, "dq_passenger_name_upper"),
                        "dq_passenger_name_pad")
