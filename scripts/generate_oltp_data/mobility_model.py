"""Business + data-quality simulation model for the Urban Mobility OLTP.

Three explicit layers, never mixed:

1. BUSINESS MODEL — plausible correlations with noise: ~90-day window,
   weekday/weekend curves, rush hours, zone roles (downtown/business/
   residential/nightlife/leisure/airport) with weighted demand, OD-driven
   distances, congestion-driven durations, demand-driven acceptance delay,
   delay-driven cancellation and rating, distance/time/surge-driven fares,
   method-dependent payment success. Latent per-driver quality. A few
   dated synthetic events modulate demand/delay/surge locally.

2. DATA QUALITY — every anomaly type is an INDEPENDENT Bernoulli trial
   applied to an already-clean value. No row is "selected dirty" and no
   anomaly enables another. Coincidences come only from the product of
   independent probabilities (verified by the 0/1/2/3+ anomaly-count
   distribution and the co-occurrence matrix in docs/bi_data_validation.md).

3. INCIDENTS — documented source-system incident windows where a small
   set of corruption keys rise together. Baseline DQ trials stay independent
   outside those windows.

Determinism: consumes only the module-level ``random`` stream and the
``Faker`` instance passed in (both seeded once by the generator via
RANDOM_SEED), plus a hash-derived per-driver RNG (stable across runs and
independent of iteration order). No other randomness.
"""

from __future__ import annotations

import os
import random
from datetime import datetime, timedelta

# Config.

WINDOW_DAYS = int(os.getenv("GEN_WINDOW_DAYS", "90"))

# Zone roles for the 21 seeded zones (zone_id: role). Tuned to the real
# catalogue in db/mobility_oltp.sql.
ZONE_ROLE = {
    1: "downtown", 2: "residential", 3: "airport", 4: "residential",
    5: "residential", 6: "downtown", 7: "nightlife", 8: "leisure",
    9: "leisure", 10: "downtown", 11: "residential", 12: "nightlife",
    13: "downtown", 14: "nightlife", 15: "business", 16: "downtown",
    17: "nightlife", 18: "nightlife", 19: "downtown", 20: "business",
    21: "nightlife",
}

# Relative popularity of each role as trip origin.
ROLE_DEMAND = {
    "downtown": 1.15, "business": 0.95, "nightlife": 0.80,
    "residential": 0.55, "leisure": 0.50, "airport": 0.70,
}

# Origin-role -> destination-role transition weights.
ROLE_OD = {
    "downtown":    {"downtown": 0.32, "business": 0.22, "residential": 0.26, "nightlife": 0.10, "leisure": 0.05, "airport": 0.05},
    "business":    {"downtown": 0.30, "business": 0.24, "residential": 0.26, "airport": 0.12, "nightlife": 0.06, "leisure": 0.02},
    "residential": {"downtown": 0.22, "business": 0.14, "residential": 0.22, "nightlife": 0.16, "leisure": 0.10, "airport": 0.16},
    "nightlife":   {"residential": 0.48, "nightlife": 0.22, "downtown": 0.20, "business": 0.05, "leisure": 0.03, "airport": 0.02},
    "leisure":     {"leisure": 0.18, "residential": 0.36, "downtown": 0.22, "nightlife": 0.14, "business": 0.05, "airport": 0.05},
    "airport":     {"downtown": 0.42, "business": 0.22, "residential": 0.20, "nightlife": 0.06, "leisure": 0.06, "airport": 0.04},
}

# Trip length (km) by origin/destination role pair: (mean, sd).
ROLE_DISTANCE = {
    frozenset({"airport", "downtown"}): (24.0, 7.0),
    frozenset({"airport", "business"}): (23.0, 6.5),
    frozenset({"airport", "residential"}): (27.0, 8.0),
    frozenset({"airport", "nightlife"}): (25.0, 8.0),
    frozenset({"airport", "leisure"}): (26.0, 8.0),
    frozenset({"airport"}): (26.0, 8.0),
    frozenset({"downtown", "business"}): (6.5, 2.5),
    frozenset({"downtown", "residential"}): (11.0, 4.0),
    frozenset({"downtown", "nightlife"}): (8.0, 3.0),
    frozenset({"downtown", "leisure"}): (12.0, 4.5),
    frozenset({"business", "residential"}): (11.5, 4.0),
    frozenset({"business", "nightlife"}): (8.5, 3.2),
    frozenset({"business", "leisure"}): (12.5, 4.5),
    frozenset({"residential", "nightlife"}): (7.5, 2.8),
    frozenset({"residential", "leisure"}): (13.0, 4.5),
    frozenset({"nightlife", "leisure"}): (9.0, 3.5),
    frozenset({"residential"}): (8.0, 3.0),
    frozenset({"downtown"}): (4.8, 1.8),
    frozenset({"business"}): (5.5, 2.0),
    frozenset({"nightlife"}): (7.0, 2.6),
    frozenset({"leisure"}): (11.5, 4.0),
}

# Hour-of-day intensity curves (index 0..23), weekday and weekend.
HOUR_WEEKDAY = [0.2, 0.15, 0.1, 0.1, 0.15, 0.4, 1.0, 2.1, 2.3, 1.5, 1.0, 1.0,
                1.1, 1.0, 1.0, 1.1, 1.4, 2.0, 2.3, 1.8, 1.4, 1.1, 0.8, 0.4]
HOUR_WEEKEND = [1.1, 0.9, 0.5, 0.2, 0.1, 0.1, 0.15, 0.2, 0.35, 0.6, 0.8, 0.95,
                1.0, 1.0, 1.0, 1.05, 1.1, 1.15, 1.2, 1.3, 1.5, 1.8, 2.0, 1.6]

# Day-of-week multipliers and gentle growth over the window.
DOW_WEIGHT = {0: 0.92, 1: 0.95, 2: 1.0, 3: 1.05, 4: 1.18, 5: 1.30, 6: 1.05}

# Role extra weight by hour band: nightlife dominates late night and weekends.
ROLE_HOUR_BAND = {
    "nightlife": lambda hour, is_weekend: 2.2 if (hour >= 21 or hour <= 2) else (0.8 if not is_weekend else 1.3),
    "business":  lambda hour, is_weekend: 1.6 if (7 <= hour <= 10 or 16 <= hour <= 19) and not is_weekend else 0.6,
    "downtown":  lambda hour, is_weekend: 1.3 if 7 <= hour <= 20 else 0.8,
    "residential": lambda hour, is_weekend: 1.3 if (7 <= hour <= 10 or 17 <= hour <= 22) else 0.8,
    "leisure":   lambda hour, is_weekend: 1.4 if (10 <= hour <= 22) and is_weekend else 0.9,
    "airport":   lambda hour, is_weekend: 1.7 if (5 <= hour <= 9 or 16 <= hour <= 21) else 0.7,
}

# Synthetic events: day offsets inside the window, multipliers on
# demand/delay/cancellation/surge. Deliberately small: they shape a few
# days, not the 90-day baseline.
EVENTS = [
    {"name": "marathon_weekend", "days": [28, 29], "roles": ["downtown", "leisure"],
     "demand": 1.5, "delay": 1.4, "cancel": 0.03, "surge": 0.18},
    {"name": "rain_storm_week", "days": list(range(45, 50)), "roles": ["downtown", "business", "residential"],
     "demand": 1.25, "delay": 1.7, "cancel": 0.05, "surge": 0.22},
    {"name": "music_festival", "days": [62, 63, 64], "roles": ["nightlife", "downtown"],
     "demand": 1.7, "delay": 1.25, "cancel": 0.01, "surge": 0.30},
]

# Incident windows: THE documented exception to DQ independence. A cohort of
# passenger rows created inside the day-window gets correlated casing +
# whitespace corruption from a simulated importer bug.
INCIDENT_WINDOWS = [
    {"name": "importer_case_whitespace", "days": [50, 51, 52, 53],
     "entity": "passenger", "fields": ["full_name", "email"]},
    {"name": "importer_date_format", "days": [50, 51, 52, 53],
     "entity": "trip", "fields": ["requested_at_source"]},
]


def _clamp(v, lo, hi):
    return max(lo, min(hi, v))


# Model.

class MobilityModel:
    """Stateless helpers bound to the window; deterministic via global RNG."""

    def __init__(self, zone_ids, end=None, window_days=WINDOW_DAYS):
        self.zone_ids = list(zone_ids)
        self.window_days = window_days
        self.end = end or datetime.now().replace(microsecond=0)
        self._role_zones = {}
        for z in self.zone_ids:
            role = ZONE_ROLE.get(z, "residential")
            self._role_zones.setdefault(role, []).append(z)
        # Fallback roles for unexpected zone ids.
        for role in ROLE_DEMAND:
            self._role_zones.setdefault(role, [self.zone_ids[0]] if self.zone_ids else [])

    # Calendar helpers.
    def day_date(self, day_idx):
        return self.end - timedelta(days=self.window_days - 1 - day_idx)

    def business_day_dates(self):
        """Business calendar (dates only). updated_at in the DB stays at the
        real INSERT wall-clock on purpose: Bronze watermarks must advance
        monotonically across runs, so backfilled history carries business
        created_at/requested_at but fresh operational updated_at values."""
        return [self.day_date(i).date() for i in range(self.window_days)]

    def _day_weights(self):
        weights = []
        for i in range(self.window_days):
            d = self.day_date(i)
            w = DOW_WEIGHT[d.weekday()] * (1.0 + 0.22 * i / self.window_days)
            for ev in EVENTS:
                if i in ev["days"]:
                    w *= ev["demand"]
            weights.append(w)
        return weights

    def sample_datetime(self, rng=random):
        day_idx = rng.choices(range(self.window_days), weights=self._day_weights())[0]
        date = self.day_date(day_idx)
        is_weekend = date.weekday() >= 5
        curve = HOUR_WEEKEND if is_weekend else HOUR_WEEKDAY
        hour = rng.choices(range(24), weights=curve)[0]
        minute = rng.randrange(60)
        second = rng.randrange(60)
        candidate = date.replace(hour=hour, minute=minute, second=second)
        if candidate > self.end:
            # last day of the window: clamp to "now" so business time never
            # runs into the future
            candidate = self.end - timedelta(
                minutes=rng.randint(5, 180), seconds=rng.randrange(60)
            )
        return candidate, day_idx

    # Congestion / demand.
    def rush_factor(self, dt):
        curve = HOUR_WEEKEND if dt.weekday() >= 5 else HOUR_WEEKDAY
        return curve[dt.hour] / 2.3  # ~0..1

    def event_for(self, dt):
        day_idx = self.window_days - 1 - (self.end.date() - dt.date()).days
        return [ev for ev in EVENTS if 0 <= day_idx < self.window_days and day_idx in ev["days"]]

    def role_of(self, zone_id):
        return ZONE_ROLE.get(zone_id, "residential")

    def sample_zone_pair(self, dt, rng=random):
        is_weekend = dt.weekday() >= 5
        hour = dt.hour
        origins = []
        for z in self.zone_ids:
            role = self.role_of(z)
            w = ROLE_DEMAND[role] * ROLE_HOUR_BAND[role](hour, is_weekend)
            for ev in self.event_for(dt):
                if role in ev["roles"]:
                    w *= ev["demand"]
            origins.append(w)
        pickup = rng.choices(self.zone_ids, weights=origins)[0]
        role_o = self.role_of(pickup)
        dests = ROLE_OD[role_o]
        role_d = rng.choices(list(dests), weights=list(dests.values()))[0]
        dropoff = rng.choice(self._role_zones[role_d])
        return pickup, dropoff, role_o, role_d

    def sample_distance_km(self, role_o, role_d, rng=random):
        mean, sd = ROLE_DISTANCE.get(frozenset({role_o, role_d})) if role_o != role_d \
            else ROLE_DISTANCE.get(frozenset({role_o}), (9.0, 3.0))
        if mean is None:
            mean, sd = 9.0, 3.0
        return _clamp(rng.gauss(mean, sd), 1.0, 48.0)

    # Driver quality (stable hash-derived rng).
    @staticmethod
    def driver_quality(driver_id, seed):
        return _clamp(random.Random(f"{seed}|driver|{driver_id}").gauss(0.0, 0.8), -2.2, 2.2)

    # Trip lifecycle.
    def plan_trip(self, driver_vehicle_pairs, day_idx, dt, driver_seed, rng=random):
        """Returns a dict (BUSINESS-clean; no DQ anomalies yet)."""
        pickup, dropoff, role_o, role_d = self.sample_zone_pair(dt, rng)
        est_km = self.sample_distance_km(role_o, role_d, rng)

        events = self.event_for(dt)
        delay_mult = 1.0 + sum((ev["delay"] - 1.0) for ev in events if role_o in ev["roles"])
        cancel_add = sum(ev["cancel"] for ev in events if role_o in ev["roles"])
        surge_add = sum(ev["surge"] for ev in events if role_o in ev["roles"])

        congestion = self.rush_factor(dt) * _clamp(delay_mult, 0.5, 3.0) \
            * {"downtown": 1.25, "business": 1.2, "nightlife": 1.05, "airport": 1.1,
               "residential": 0.95, "leisure": 0.9}.get(role_o, 1.0)

        driver_id, vehicle_id = rng.choice(driver_vehicle_pairs) if driver_vehicle_pairs else (None, None)
        quality = self.driver_quality(driver_id, driver_seed) if driver_id else 0.0
        delay_scale = max(0.8, (1.4 + 8.5 * congestion) * _clamp(1.0 - 0.16 * quality, 0.65, 1.35))
        delay_min = _clamp(rng.expovariate(1.0 / delay_scale) + rng.gauss(0, 0.7), 0.3, 65.0)

        speed = _clamp(33.0 - 15.0 * congestion + rng.gauss(0, 4.5), 9.0, 62.0)
        duration_min = _clamp(est_km / speed * 60.0 + rng.gauss(0, 3.5), 4.0, 170.0)

        p_cancel = _clamp(0.045 + 0.115 * max(0.0, delay_min - 8.0) / 40.0
                          + (0.05 if quality < -1.0 else 0.0) + cancel_add
                          + (0.03 if (dt.hour >= 22 or dt.hour <= 2) and role_o in ("nightlife", "downtown") else 0.0),
                          0.02, 0.60)

        recent = day_idx >= self.window_days - 2
        p_stuck = 0.30 if recent else 0.045

        base = self.day_date(day_idx)
        requested = base.replace(hour=dt.hour, minute=dt.minute, second=dt.second)
        accepted_at = started_at = ended_at = canceled_at = None
        if rng.random() < p_stuck:
            if not recent:
                # abandoned long ago = lost cancellation, never an open request
                status = "canceled"
            else:
                status = rng.choices(["requested", "accepted", "started"], weights=[0.45, 0.30, 0.25])[0]
        elif rng.random() < p_cancel:
            status = "canceled"
        else:
            status = "completed"

        fare_km_part = 1.30 * est_km
        fare_min_part = 0.22 * duration_min
        surge_pct = 0.12 * self.rush_factor(dt) + surge_add
        subtotal = 2.60 + fare_km_part + fare_min_part
        fare = _clamp(subtotal * (1.0 + surge_pct) + rng.gauss(0, 0.05 * subtotal), 4.5, 240.0)

        if status in ("accepted", "started", "completed"):
            accepted_at = requested + timedelta(minutes=delay_min)
        if status in ("started", "completed"):
            started_at = accepted_at + timedelta(minutes=rng.uniform(0.5, 6.0))
        if status == "completed":
            ended_at = started_at + timedelta(minutes=duration_min)
        if status == "canceled":
            pre_accept = delay_min > 10.0 and rng.random() < 0.55
            canceled_at = requested + timedelta(minutes=delay_min * rng.uniform(0.9, 1.15) if pre_accept
                                                 else delay_min + rng.uniform(0, 10))
            if not pre_accept:
                accepted_at = requested + timedelta(minutes=delay_min)

        cancel_by = cancel_reason = None
        if status == "canceled":
            if accepted_at is None or (canceled_at and canceled_at <= accepted_at):
                cancel_by = cancel_reason = "passenger" if rng.random() < 0.7 else "system"
            else:
                cancel_by = rng.choices(["driver", "system", "passenger"], weights=[0.45, 0.35, 0.20])[0]
                cancel_reason = rng.choices(["driver", "system", "passenger"], weights=[0.40, 0.40, 0.20])[0]

        return {
            "status": status,
            "requested_at": requested, "accepted_at": accepted_at, "started_at": started_at,
            "ended_at": ended_at, "canceled_at": canceled_at,
            "pickup_zone_id": pickup, "dropoff_zone_id": dropoff,
            "driver_id": driver_id, "vehicle_id": vehicle_id,
            "estimated_distance_km": round(est_km, 2),
            "fare_amount": round(fare, 2),
            "delay_min": delay_min, "duration_min": duration_min,
            "cancel_by": cancel_by, "cancel_reason": cancel_reason,
            "quality": quality, "congestion": congestion,
        }

    # Ratings (business signal; independent of dq).
    def rating_score(self, plan, rng=random):
        mu = 4.42 + 0.55 * plan["quality"]
        vehicle_type = str(plan.get("vehicle_type") or "").strip().lower()
        if vehicle_type in {"motorbike", "motorcycle", "moto", "bike"}:
            mu -= 0.32
        elif vehicle_type in {"sedan", "saloon"}:
            mu += 0.12
        mu -= min(1.9, 0.10 * max(0.0, plan["delay_min"] - 6.0))
        if plan["duration_min"] > 120:
            mu -= 0.35
        req = plan.get("requested_at")
        if req is not None and any(ev["name"] == "rain_storm_week" for ev in self.event_for(req)):
            mu -= 0.12
        mu += rng.gauss(0, 0.45)
        return int(_clamp(round(mu), 1, 5))

    # Payments (business signal).
    PAYMENT_METHOD_WEIGHT = {"cash": 0.22, "card": 0.48, "wallet": 0.30}
    PAYMENT_STATUS_BY_METHOD = {
        "cash":   {"paid": 0.92, "failed": 0.03, "pending": 0.05},
        "card":   {"paid": 0.72, "failed": 0.17, "pending": 0.11},
        "wallet": {"paid": 0.84, "failed": 0.05, "pending": 0.11},
    }
    PAYMENT_PRESENCE = {"completed": 0.97, "canceled": 0.30, "requested": 0.05,
                        "accepted": 0.06, "started": 0.07}

    def payment_plan(self, plan, rng=random):
        if rng.random() >= self.PAYMENT_PRESENCE.get(plan["status"], 0.1):
            return None
        method = rng.choices(list(self.PAYMENT_METHOD_WEIGHT), weights=list(self.PAYMENT_METHOD_WEIGHT.values()))[0]
        smap = self.PAYMENT_STATUS_BY_METHOD[method]
        status = rng.choices(list(smap), weights=list(smap.values()))[0]
        if plan["fare_amount"] is not None:
            amount = round(plan["fare_amount"] * rng.uniform(0.985, 1.015), 2)
        else:
            amount = round(_clamp(rng.gauss(35, 15), 5, 130), 2)
        paid_at = None
        if status == "paid":
            anchor = plan["ended_at"] or plan["requested_at"]
            paid_at = anchor + timedelta(minutes=rng.uniform(0.5, 25))
        return {"method": method, "status": status, "amount": amount,
                "currency": "USD", "paid_at": paid_at}


# Dq.
# Every injector below is a SEPARATE Bernoulli trial over the clean value.
# Legacy aggregate keys (TEXT_FORMAT_NOISE_RATE etc.) are superseded by the
# granular dq_* keys but kept exported for the config contract tests.

DQ_RATES = {
    # passenger names/emails/phones/cities (independent per anomaly type)
    "dq_passenger_name_upper": 0.035,
    "dq_passenger_name_lower": 0.025,
    "dq_passenger_name_pad": 0.05,
    "dq_passenger_email_upper": 0.03,
    "dq_passenger_email_pad": 0.04,
    "dq_passenger_email_invalid": 0.04,
    "dq_passenger_email_null": 0.15,
    "dq_passenger_phone_invalid": 0.04,
    "dq_passenger_phone_pad": 0.05,
    "dq_passenger_phone_null": 0.14,
    "dq_passenger_city_upper": 0.03,
    "dq_passenger_city_lower": 0.04,
    "dq_passenger_city_pad": 0.03,
    # drivers / vehicles
    "dq_driver_name_upper": 0.03,
    "dq_driver_name_lower": 0.025,
    "dq_driver_name_pad": 0.04,
    "dq_license_lower": 0.05,
    "dq_license_pad": 0.03,
    "dq_plate_lower": 0.06,
    "dq_plate_pad": 0.04,
    "dq_vehicle_year_invalid": 0.02,
    "dq_vehicle_missing_driver": 0.01,
    # trips / notes
    "dq_note_pad": 0.05,
    "dq_note_multiline": 0.02,
    "dq_note_emoji": 0.02,
    "dq_note_nulllike": 0.03,
    "dq_note_empty": 0.015,
    # trips numeric/NULL channels (reused by generator legacy gates)
    "dq_trip_fare_null": 0.18,
    "dq_trip_requested_at_alt_format": 0.08,
    "dq_trip_requested_at_invalid": 0.015,
}

_NULLISH = ("NULL", "null", "N/A", "-", "None", "  NULL  ")


def maybe_upper(value, key, rng=random):
    if isinstance(value, str) and rng.random() < DQ_RATES[key]:
        return value.upper()
    return value


def maybe_lower(value, key, rng=random):
    if isinstance(value, str) and rng.random() < DQ_RATES[key]:
        return value.lower()
    return value


def maybe_pad(value, key, rng=random):
    if isinstance(value, str) and rng.random() < DQ_RATES[key]:
        return " " * rng.randint(1, 3) + value + " " * rng.randint(1, 3)
    return value


def maybe_null(value, key, rng=random):
    return None if rng.random() < DQ_RATES[key] else value


def maybe_invalid(value, key, kind, rng=random):
    if value is None or rng.random() >= DQ_RATES[key]:
        return value
    if kind == "email":
        return rng.choice([value.replace("@", " at "), f"invalid-{rng.randrange(10**10):010x}",
                           value.split("@")[0] + "@"])
    if kind == "phone":
        return rng.choice(["N/A", "000", "sin telefono", f"ext {rng.randint(1, 99)}"])
    return value


def requested_at_source_text(value, incident=False, rng=random):
    """Keep a raw timestamp representation for format-quality exercises."""
    invalid_rate = 0.25 if incident else DQ_RATES["dq_trip_requested_at_invalid"]
    if rng.random() < invalid_rate:
        return rng.choice(("not-a-date", "31/02/2026 09:00:00", "2026-13-01T09:00:00"))
    if rng.random() < DQ_RATES["dq_trip_requested_at_alt_format"]:
        pattern = rng.choice(("%Y/%m/%d %H:%M:%S", "%d/%m/%Y %H:%M:%S"))
        return value.strftime(pattern)
    return value.strftime("%Y-%m-%dT%H:%M:%S")


def apply_passenger_dq(name, email, phone, city, incident=False, rng=random):
    """Clean values in, independently-corrupted values out.

    ``incident=True`` activates the documented correlated importer window
    (case + whitespace fire TOGETHER, once), which is deliberately the only
    non-independent corruption path in the model.
    """
    if incident:
        if rng.random() < 0.5:
            name = name.upper()
            email = email.upper() if email else email
            name = "  " + name + " "
            email = (" " + email + " ") if email else email
        return name, email, phone, city
    name = maybe_upper(name, "dq_passenger_name_upper", rng)
    name = maybe_lower(name, "dq_passenger_name_lower", rng)
    name = maybe_pad(name, "dq_passenger_name_pad", rng)
    email = maybe_upper(email, "dq_passenger_email_upper", rng)
    email = maybe_invalid(email, "dq_passenger_email_invalid", "email", rng)
    email = maybe_pad(email, "dq_passenger_email_pad", rng)
    email = maybe_null(email, "dq_passenger_email_null", rng)
    phone = maybe_invalid(phone, "dq_passenger_phone_invalid", "phone", rng)
    phone = maybe_pad(phone, "dq_passenger_phone_pad", rng)
    phone = maybe_null(phone, "dq_passenger_phone_null", rng)
    city = maybe_upper(city, "dq_passenger_city_upper", rng)
    city = maybe_lower(city, "dq_passenger_city_lower", rng)
    city = maybe_pad(city, "dq_passenger_city_pad", rng)
    return name, email, phone, city


def apply_driver_dq(name, license_number, rng=random):
    name = maybe_upper(name, "dq_driver_name_upper", rng)
    name = maybe_lower(name, "dq_driver_name_lower", rng)
    name = maybe_pad(name, "dq_driver_name_pad", rng)
    license_number = maybe_lower(license_number, "dq_license_lower", rng)
    license_number = maybe_pad(license_number, "dq_license_pad", rng)
    return name, license_number


def apply_vehicle_dq(plate, rng=random):
    plate = maybe_lower(plate, "dq_plate_lower", rng)
    plate = maybe_pad(plate, "dq_plate_pad", rng)
    return plate


def apply_note_dq(base_note, rng=random):
    """cancel_note channels: presence gate first (legacy CANCEL_NOTE_*),
    then each formatting anomaly is independent over the resulting value."""
    note = base_note
    if note is None:
        return None
    if rng.random() < DQ_RATES["dq_note_nulllike"]:
        return rng.choice(_NULLISH)
    if rng.random() < DQ_RATES["dq_note_empty"]:
        return ""
    note = maybe_pad(note, "dq_note_pad", rng)
    if rng.random() < DQ_RATES["dq_note_multiline"]:
        note = note + "\n" + "gracias"
    if rng.random() < DQ_RATES["dq_note_emoji"]:
        note = note + " " + rng.choice(["😅", "", "❌", "🕒"])
    return note


def in_incident_window(created_dt, model, entity="passenger"):
    day_idx = model.window_days - 1 - (model.end.date() - created_dt.date()).days
    return any(inc["entity"] == entity and day_idx in inc["days"] for inc in INCIDENT_WINDOWS)
