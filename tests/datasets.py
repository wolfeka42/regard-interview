"""Chained dummy datasets (D9): bronze rows -> expected clean_* -> expected gold.

Every value is invented. Expected hashes are recomputed with the stdlib `hmac`
(`tests.assertions.expected_hash`), never with pipeline code.
"""

from __future__ import annotations

import csv
from datetime import UTC, date, datetime
from pathlib import Path

from tests.assertions import clean_text, expected_hash, normalize, parse_ts

SALT = "synthetic-test-salt-not-a-real-secret"
RUN_TS = datetime(2024, 3, 1, 12, 0, 0, tzinfo=UTC)
NBSP = " "

# Quote, comma and newline: exercises escape='"' and multiLine on the CSV round trip.
BEN_NAME = 'Dr. "Q", Example\nJr.'

B2_ENC = "6F1C2D3E-0000-4000-8000-00000000B002"
B2_PATIENT = "9B2A7C4D-0000-4000-8000-0000000000A1"
B2_PROVIDER = "C0FFEE00-0000-4000-8000-0000000000D1"


def t(day: int, hour: int, month: int = 1) -> datetime:
    return datetime(2024, month, day, hour, 0, 0, tzinfo=UTC)


def h(customer: str, patient: str | None) -> str | None:
    return expected_hash(SALT, customer, patient)


# ---- Bronze (all strings) ---------------------------------------------------------
# customer, encounter_id, patient_id, provider_id, start_time, end_time, updated_at, status
BRONZE_ENCOUNTER_ROWS = [
    (
        "alpha",
        "1001",
        "1",
        "1",
        "2024-01-05T08:00:00Z",
        "2024-01-05T14:00:00Z",
        "2024-01-05T15:00:00Z",
        "completed",
    ),
    (
        " ALPHA",
        "1002",
        " 2 ",
        "1",
        "2024-01-05T09:00:00Z",
        "2024-01-05T21:00:00Z",
        "2024-01-05T22:00:00Z",
        "Completed",
    ),
    (
        " ALPHA",
        "1002",
        " 2 ",
        "1",
        "2024-01-05T09:00:00Z",
        "2024-01-05T21:00:00Z",
        "2024-01-05T22:00:00Z",
        "Completed",
    ),
    (
        "alpha",
        "1003",
        "3",
        "1",
        "2024-01-06T10:00:00Z",
        "2024-01-07T10:00:00Z",
        "2024-01-07T11:00:00Z",
        "completed",
    ),
    (
        "alpha",
        "1003",
        "3",
        "2",
        "2024-01-06T10:00:00Z",
        "2024-01-07T10:00:00Z",
        "2024-01-07T11:00:00Z",
        "completed",
    ),
    # Newer version listed first; later updated_at must win, compared as a timestamp.
    (
        "alpha",
        "1004",
        "4",
        "2",
        "2024-01-07T08:00:00Z",
        "2024-01-07T20:00:00Z",
        "2024-01-07T21:00:00Z",
        "completed",
    ),
    ("alpha", "1004", "4", "2", "2024-01-07T08:00:00Z", "", "2024-01-07T07:00:00Z", "scheduled"),
    (
        "alpha",
        "1005",
        "5",
        "1",
        "2024-01-05T11:00:00Z",
        "2024-01-05T12:00:00Z",
        "2024-01-05T12:00:00Z",
        " Cancelled",
    ),
    (
        "alpha",
        "1006",
        "6",
        "3",
        "2024-01-08T11:00:00Z",
        "2024-01-08T12:00:00Z",
        "2024-01-08T12:00:00Z",
        "canceled",
    ),
    ("alpha", "1007", "7", "2", "2024-01-08T08:00:00Z", "", "2024-01-08T09:00:00Z", "completed"),
    (
        "alpha",
        "1008",
        "",
        "2",
        "2024-01-08T10:00:00Z",
        "2024-01-08T09:00:00Z",
        "2024-01-08T11:00:00Z",
        "",
    ),
    (
        "alpha",
        "1009",
        "1",
        "99",
        "2024-01-05T13:00:00Z",
        "2024-01-05T19:00:00Z",
        "2024-01-05T20:00:00Z",
        "completed",
    ),
    ("alpha", "1010", "1", "1", "", "2024-01-05T19:00:00Z", "2024-01-05T20:00:00Z", "completed"),
    (
        "alpha",
        "1011",
        "1",
        "1",
        "not-a-time",
        "2024-01-05T19:00:00Z",
        "2024-01-05T20:00:00Z",
        "completed",
    ),
    ("alpha", "1012", "1", "1", "2024-01-05T08:00:00+05:30", "", "", "completed"),
    ("alpha", "1013", "1", " ", "2024-01-05T08:00:00Z", "", "", "completed"),
    (
        "beta",
        "B-0001",
        "1",
        "1",
        "2024-01-05T08:00:00Z",
        "2024-01-05T14:00:00Z",
        "2024-01-05T15:00:00Z",
        "completed",
    ),
    (
        " Beta",
        B2_ENC,
        f"{NBSP}{B2_PATIENT}",
        B2_PROVIDER,
        "2024-01-06T09:00:00Z",
        "2024-01-06T21:00:00Z",
        "2024-01-06T22:00:00Z",
        "COMPLETED",
    ),
    # Story 1 part 2: sentinels. Optional end_time and status sentinels give nulls; kept.
    ("alpha", "1014", "8", "3", "2024-01-09T08:00:00Z", "unknown", "2024-01-09T09:00:00Z", " n/a "),
    # Sentinel patient: kept with null patient_id and hash; "?" updated_at, "Null" status.
    (
        "alpha",
        "1015",
        "unknown",
        "3",
        "2024-01-09T10:00:00Z",
        "2024-01-09T16:00:00Z",
        "?",
        "Null",
    ),
    # Sentinels in required fields: dropped.
    ("alpha", "1016", "1", "UNKNOWN", "2024-01-09T08:00:00Z", "", "", "completed"),
    ("alpha", "1017", "1", "1", "NULL", "", "", "completed"),
    (f"{NBSP}-{NBSP}", "1018", "1", "1", "2024-01-09T08:00:00Z", "", "", "completed"),
    ("alpha", "none", "1", "1", "2024-01-09T08:00:00Z", "", "", "completed"),
]

# customer, lab_id, encounter_id, test_name, result_value, result_unit, observed_time, status
BRONZE_LAB_ROWS = [
    ("alpha", "5001", "1001", "glucose", "95", "mg/dL", "2024-01-05T09:00:00Z", "final"),
    ("alpha", "5001", "1001", "glucose", "95", "mg/dL", "2024-01-05T09:00:00Z", "final"),
    ("alpha", "5002", "1001", "sodium", "140", "mmol/L", "2024-01-05T09:05:00Z", ""),
    ("alpha", "5003", "1002", "potassium", "4.1", "mmol/L", "2024-01-05T10:00:00Z", "Final"),
    ("alpha", "5004", "1002", "glucose", "300", "mg/dL", "2024-01-05T10:00:00Z", "retracted"),
    ("alpha", "5005", "1003", "hemoglobin", "13.6", "g/dL", "2024-01-07T08:00:00Z", "final"),
    ("alpha", "5005", "1003", "hemoglobin", "13.5", "g/dL", "2024-01-07T08:00:00Z", "final"),
    ("alpha", "5006", "1005", "glucose", "88", "mg/dL", "2024-01-05T11:30:00Z", "final"),
    ("alpha", "5007", "", "glucose", "88", "mg/dL", "2024-01-05T11:30:00Z", "final"),
    ("alpha", "5008", "1004", "glucose", "abc", "mg/dL", "2024-01-07T09:00:00Z", "final"),
    ("alpha", "5009", "1009", "sodium", "138", "mmol/L", "2024-01-05T14:00:00Z", "final"),
    ("beta", "L-0001", "B-0001", "glucose", "101", "mg/dL", "2024-01-05T09:00:00Z", "final"),
    # Story 1 part 2: test_name formatting, sentinels in optional fields, result_value.
    (
        "alpha",
        "5010",
        "1014",
        "  Hemoglobin   A1C ",
        " 4.1 ",
        "-",
        "2024-01-09T09:00:00Z",
        "NULL",
    ),
    ("alpha", "5011", "1015", "GLUCOSE", "N/A", "mg/dL", "2024-01-09T11:00:00Z", "final"),
    (
        "alpha",
        "5012",
        "1015",
        f"Hemoglobin{NBSP}\tA1C",
        "6.0",
        "%",
        "2024-01-09T11:00:00Z",
        "final",
    ),
    # "Na" (sodium) is not a sentinel: kept as text.
    ("alpha", "5013", "1015", "Na", "139", "mmol/L", "2024-01-09T11:00:00Z", "final"),
    # Sentinels in required fields: dropped.
    ("alpha", "5014", "N/A", "glucose", "90", "mg/dL", "2024-01-09T11:00:00Z", "final"),
    ("?", "5015", "1001", "glucose", "90", "mg/dL", "2024-01-05T09:00:00Z", "final"),
    ("alpha", "unknown", "1001", "glucose", "90", "mg/dL", "2024-01-05T09:00:00Z", "final"),
]

# customer, provider_id, provider_name, specialty, active_flag
BRONZE_PROVIDER_ROWS = [
    ("alpha", "1", "Dr. Ada Synthetic", "Cardiology", "true"),
    ("alpha", "1", "Dr. Ada Synthetic", "Cardiology", "true"),
    ("alpha", "2", BEN_NAME, "", "Y"),
    ("alpha", "3", "Dr. Cy Sample", "Pediatrics", "no"),
    ("alpha", " 3", "Dr. Cy Sample", "Neurology", "0"),
    ("alpha", "", "Dr. Nobody", "Surgery", "yes"),
    ("beta", "1", "Dr. Dee Placeholder", "Oncology", "F"),
    ("beta", B2_PROVIDER, "Dr. Eve Fictional", "Radiology", "maybe"),
    # Story 1 part 2: optional sentinels, whitespace-only specialty, active_flag forms.
    ("alpha", "4", "Dr. Flo Invented", "N/A", "N/A"),
    ("alpha", "5", "Dr. Gus Madeup", f"  {NBSP} ", " TRUE "),
    ("alpha", "6", "Dr. Hal Imaginary", "Nephrology", "N"),
    ("alpha", "7", "Dr. Ivy Pretend", "Dermatology", "Yes"),
    # Sentinels in required fields: dropped.
    ("alpha", "unknown", "Dr. Nil Nobody", "Surgery", "yes"),
    ("-", "8", "Dr. Dash Nobody", "Surgery", "yes"),
]

# ---- Expected silver --------------------------------------------------------------
P1, P2 = "c0ffee00-0000-4000-8000-0000000000d1", B2_ENC.lower()
EXPECTED_CLEAN_ENCOUNTERS = [
    ("alpha", "1001", None, "1", t(5, 8), t(5, 14), "completed", 0.25, h("alpha", "1"), RUN_TS),
    ("alpha", "1002", None, "1", t(5, 9), t(5, 21), "completed", 0.5, h("alpha", "2"), RUN_TS),
    ("alpha", "1003", None, "1", t(6, 10), t(7, 10), "completed", 1.0, h("alpha", "3"), RUN_TS),
    ("alpha", "1003", None, "2", t(6, 10), t(7, 10), "completed", 1.0, h("alpha", "3"), RUN_TS),
    ("alpha", "1004", None, "2", t(7, 8), t(7, 20), "completed", 0.5, h("alpha", "4"), RUN_TS),
    (
        "alpha",
        "1005",
        None,
        "1",
        t(5, 11),
        t(5, 12),
        "cancelled",
        3600 / 86400,
        h("alpha", "5"),
        RUN_TS,
    ),
    (
        "alpha",
        "1006",
        None,
        "3",
        t(8, 11),
        t(8, 12),
        "canceled",
        3600 / 86400,
        h("alpha", "6"),
        RUN_TS,
    ),
    ("alpha", "1007", None, "2", t(8, 8), None, "completed", None, h("alpha", "7"), RUN_TS),
    ("alpha", "1008", None, "2", t(8, 10), t(8, 9), None, None, None, RUN_TS),
    ("alpha", "1009", None, "99", t(5, 13), t(5, 19), "completed", 0.25, h("alpha", "1"), RUN_TS),
    ("beta", "b-0001", None, "1", t(5, 8), t(5, 14), "completed", 0.25, h("beta", "1"), RUN_TS),
    ("beta", P2, None, P1, t(6, 9), t(6, 21), "completed", 0.5, h("beta", B2_PATIENT), RUN_TS),
    ("alpha", "1014", None, "3", t(9, 8), None, None, None, h("alpha", "8"), RUN_TS),
    ("alpha", "1015", None, "3", t(9, 10), t(9, 16), None, 0.25, None, RUN_TS),
]

EXPECTED_CLEAN_LABS = [
    (
        "alpha",
        "5001",
        "1001",
        "glucose",
        95.0,
        "mg/dL",
        datetime(2024, 1, 5, 9, 0, tzinfo=UTC),
        "final",
        RUN_TS,
    ),
    (
        "alpha",
        "5002",
        "1001",
        "sodium",
        140.0,
        "mmol/L",
        datetime(2024, 1, 5, 9, 5, tzinfo=UTC),
        None,
        RUN_TS,
    ),
    ("alpha", "5003", "1002", "potassium", 4.1, "mmol/L", t(5, 10), "final", RUN_TS),
    ("alpha", "5004", "1002", "glucose", 300.0, "mg/dL", t(5, 10), "retracted", RUN_TS),
    ("alpha", "5005", "1003", "hemoglobin", 13.5, "g/dL", t(7, 8), "final", RUN_TS),
    (
        "alpha",
        "5006",
        "1005",
        "glucose",
        88.0,
        "mg/dL",
        datetime(2024, 1, 5, 11, 30, tzinfo=UTC),
        "final",
        RUN_TS,
    ),
    ("alpha", "5008", "1004", "glucose", None, "mg/dL", t(7, 9), "final", RUN_TS),
    ("alpha", "5009", "1009", "sodium", 138.0, "mmol/L", t(5, 14), "final", RUN_TS),
    ("beta", "l-0001", "b-0001", "glucose", 101.0, "mg/dL", t(5, 9), "final", RUN_TS),
    ("alpha", "5010", "1014", "hemoglobin a1c", 4.1, None, t(9, 9), None, RUN_TS),
    ("alpha", "5011", "1015", "glucose", None, "mg/dL", t(9, 11), "final", RUN_TS),
    ("alpha", "5012", "1015", "hemoglobin a1c", 6.0, "%", t(9, 11), "final", RUN_TS),
    ("alpha", "5013", "1015", "na", 139.0, "mmol/L", t(9, 11), "final", RUN_TS),
]

EXPECTED_CLEAN_PROVIDERS = [
    ("alpha", "1", "Dr. Ada Synthetic", "Cardiology", True, RUN_TS),
    ("alpha", "2", BEN_NAME, None, True, RUN_TS),
    ("alpha", "3", "Dr. Cy Sample", "Neurology", False, RUN_TS),
    ("beta", "1", "Dr. Dee Placeholder", "Oncology", False, RUN_TS),
    ("beta", P1, "Dr. Eve Fictional", "Radiology", None, RUN_TS),
    ("alpha", "4", "Dr. Flo Invented", None, None, RUN_TS),
    ("alpha", "5", "Dr. Gus Madeup", None, True, RUN_TS),
    ("alpha", "6", "Dr. Hal Imaginary", "Nephrology", False, RUN_TS),
    ("alpha", "7", "Dr. Ivy Pretend", "Dermatology", True, RUN_TS),
]

# ---- Expected gold (from the expected silver above) -------------------------------
ADA = ("Dr. Ada Synthetic", "Cardiology", True)
BEN = (BEN_NAME, None, True)
EXPECTED_GOLD = [
    ("alpha", "1", date(2024, 1, 5), *ADA, 2, 2, 0.75, 3),
    ("alpha", "1", date(2024, 1, 6), *ADA, 1, 1, 1.0, 1),
    ("alpha", "2", date(2024, 1, 6), *BEN, 1, 1, 1.0, 1),
    ("alpha", "2", date(2024, 1, 7), *BEN, 1, 1, 0.5, 1),
    ("alpha", "2", date(2024, 1, 8), *BEN, 2, 1, 0.0, 0),
    # 1014 (null end, null status) and 1015 (sentinel patient); labs 5010-5013.
    ("alpha", "3", date(2024, 1, 9), "Dr. Cy Sample", "Neurology", False, 2, 1, 0.25, 4),
    ("alpha", "99", date(2024, 1, 5), None, None, None, 1, 1, 0.25, 1),
    ("beta", "1", date(2024, 1, 5), "Dr. Dee Placeholder", "Oncology", False, 1, 1, 0.25, 1),
    ("beta", P1, date(2024, 1, 6), "Dr. Eve Fictional", "Radiology", None, 1, 1, 0.5, 0),
]


def write_bronze_csvs(data_root: Path, headers: dict[str, list[str]]) -> None:
    """Write the dummy bronze rows as `{data_root}/bronze/{source}.csv`."""
    bronze = data_root / "bronze"
    bronze.mkdir(parents=True, exist_ok=True)
    for source, rows in (
        ("encounters", BRONZE_ENCOUNTER_ROWS),
        ("labs", BRONZE_LAB_ROWS),
        ("providers", BRONZE_PROVIDER_ROWS),
    ):
        with (bronze / f"{source}.csv").open("w", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh)
            writer.writerow(headers[source])
            writer.writerows(rows)


# ---- Independent Python oracle for gold from any synthetic bronze -----------------
_TRUE = {"true", "t", "y", "yes", "1"}
_FALSE = {"false", "f", "n", "no", "0"}


def _text(v):
    """Trim; a sentinel (tests.assertions.SENTINELS) becomes null."""
    return clean_text(v)


def _test_name(v):
    s = _text(v)
    return " ".join(s.split()).lower() if s is not None else None


def _float(v):
    try:
        return float(_text(v))
    except (TypeError, ValueError):
        return None


def _order(values):
    """Ascending, nulls last, per column."""
    return tuple((v is None, v if v is not None else 0) for v in values)


def _pick(rows, key_len, required, first=None):
    rows = list(dict.fromkeys(rows))  # exact duplicates
    rows = [r for r in rows if all(r[i] is not None for i in required)]
    best = {}
    for r in rows:
        k = r[:key_len]
        rank = (first(r) if first else ()) + _order(r[key_len:])
        if k not in best or rank < best[k][0]:
            best[k] = (rank, r)
    return [r for _, r in best.values()]


def _latest_first(r):
    """Encounters: latest updated_at (index 5) first, nulls last."""
    return (r[5] is None, -r[5].timestamp() if r[5] else 0)


def oracle_gold(bronze: dict[str, list[dict[str, str]]]) -> list[tuple]:
    """Compute provider_daily_activity from bronze dict rows, following data-model.md."""
    # encounters keyed (customer, encounter_id, provider_id, ...)
    enc = []
    for r in bronze["encounters"]:
        start, end, upd = (
            parse_ts(r["start_time"]),
            parse_ts(r["end_time"]),
            parse_ts(r["updated_at"]),
        )
        status = _text(r["status"])
        enc.append(
            (
                normalize(r["customer"]),
                normalize(r["encounter_id"]),
                normalize(r["provider_id"]),
                start,
                end,
                upd,
                status.lower() if status else None,
                normalize(r["patient_id"]),
            )
        )
    enc = _pick(enc, 3, (0, 1, 2, 3), _latest_first)

    labs = []
    for r in bronze["labs"]:
        status = _text(r["status"])
        labs.append(
            (
                normalize(r["customer"]),
                normalize(r["lab_id"]),
                normalize(r["encounter_id"]),
                _test_name(r["test_name"]),
                _float(r["result_value"]),
                _text(r["result_unit"]),
                parse_ts(r["observed_time"]),
                status.lower() if status else None,
            )
        )
    labs = _pick(labs, 2, (0, 1, 2))

    provs = []
    for r in bronze["providers"]:
        flag = (_text(r["active_flag"]) or "").lower()
        active = True if flag in _TRUE else False if flag in _FALSE else None
        provs.append(
            (
                normalize(r["customer"]),
                normalize(r["provider_id"]),
                _text(r["provider_name"]),
                _text(r["specialty"]),
                active,
            )
        )
    providers = {p[:2]: p[2:] for p in _pick(provs, 2, (0, 1))}

    days: dict[tuple, dict] = {}
    by_encounter: dict[tuple, list[tuple]] = {}
    for c, e, p, start, end, _upd, status, patient in enc:
        if status in ("cancelled", "canceled"):
            continue
        day = (c, p, start.astimezone(UTC).date())
        agg = days.setdefault(day, {"enc": set(), "pat": set(), "dur": 0.0, "labs": set()})
        agg["enc"].add(e)
        hashed = expected_hash(SALT, c, patient)
        if hashed:
            agg["pat"].add(hashed)
        if end is not None and end >= start:
            agg["dur"] += (end - start).total_seconds() / 86400
        by_encounter.setdefault((c, e), []).append(day)
    for c, lab_id, e, *_rest, status in labs:
        if status == "retracted":
            continue
        for day in by_encounter.get((c, e), []):
            days[day]["labs"].add(lab_id)

    out = []
    for (c, p, d), agg in days.items():
        name, spec, active = providers.get((c, p), (None, None, None))
        out.append(
            (
                c,
                p,
                d,
                name,
                spec,
                active,
                len(agg["enc"]),
                len(agg["pat"]),
                agg["dur"],
                len(agg["labs"]),
            )
        )
    return out
