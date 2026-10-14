"""Write synthetic `{DATA_ROOT}/bronze/{encounters,labs,providers}.csv` with a fixed seed.

Every value is invented. Two customers: `alpha` uses integer IDs, `beta` UUIDs (plus a
deliberate integer collision with alpha). Hand-written rows cover each story 1 case:
duplicates, versioned encounters, multi-provider encounters, cancelled visits, lab
statuses, next-day labs, unknown providers, bad durations and missing required fields;
and each silver quality rule: sentinels (trimmed, any case, NBSP-padded) in optional and
required fields, a sentinel patient, `test_name` formatting, "Na" kept as text, blank
`specialty`, `active_flag` forms and non-numeric `result_value`. A seeded bulk adds
ordinary activity on top.
"""

from __future__ import annotations

import csv
import random
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

from pipeline.config import DATA_ROOT, require_env
from pipeline.schemas import BRONZE_ENCOUNTERS, BRONZE_LABS, BRONZE_PROVIDERS

SEED = 42
NBSP = " "
BULK_DAYS = 5
BULK_START = datetime(2024, 2, 1, tzinfo=UTC)


def ts(value: datetime) -> str:
    return value.strftime("%Y-%m-%dT%H:%M:%SZ")


def _uuid(rng: random.Random) -> str:
    return str(uuid.UUID(int=rng.getrandbits(128), version=4))


# ---- Hand-written case rows -------------------------------------------------------
# encounters: customer, encounter_id, patient_id, provider_id, start, end, updated_at, status
CASE_ENCOUNTERS = [
    # Happy path: provider 1 at alpha, two encounters on 2024-01-05, two patients.
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
        "ALPHA ",
        "1002",
        "2",
        "1",
        "2024-01-05T09:00:00Z",
        "2024-01-05T21:00:00Z",
        "2024-01-05T22:00:00Z",
        "Completed",
    ),
    # Exact duplicate of the row above.
    (
        "ALPHA ",
        "1002",
        "2",
        "1",
        "2024-01-05T09:00:00Z",
        "2024-01-05T21:00:00Z",
        "2024-01-05T22:00:00Z",
        "Completed",
    ),
    # Multi-provider encounter: one row per provider.
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
    # Two versions; the later updated_at wins.
    ("alpha", "1004", "4", "2", "2024-01-07T08:00:00Z", "", "2024-01-07T07:00:00Z", "scheduled"),
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
    # Cancelled variants: kept in silver, excluded from gold (with their labs).
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
    # Duration: end missing; end before start (patient unknown -> null hash).
    ("alpha", "1007", "7", "2", "2024-01-08T08:00:00Z", "", "2024-01-08T09:00:00Z", "completed"),
    (
        "alpha",
        "1008",
        "",
        "2",
        "2024-01-08T10:00:00Z",
        "2024-01-08T09:00:00Z",
        "2024-01-08T11:00:00Z",
        "completed",
    ),
    # Unknown provider 99.
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
    # Missing or unparseable start_time: dropped.
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
    # Cross-customer collision: patient 1 and provider 1 also exist at beta.
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
        "6F1C2D3E-0000-4000-8000-00000000B002",
        f"{NBSP}9B2A7C4D-0000-4000-8000-0000000000A1",
        "C0FFEE00-0000-4000-8000-0000000000D1",
        "2024-01-06T09:00:00Z",
        "2024-01-06T21:00:00Z",
        "2024-01-06T22:00:00Z",
        "COMPLETED",
    ),
    # Sentinels in optional fields (end_time, status, updated_at): null, row kept.
    ("alpha", "1014", "8", "3", "2024-01-09T08:00:00Z", "unknown", "2024-01-09T09:00:00Z", " n/a "),
    # Sentinel patient: kept with null patient_id and null hash.
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

# labs: customer, lab_id, encounter_id, test_name, result_value, result_unit, observed_time, status
CASE_LABS = [
    ("alpha", "5001", "1001", "glucose", "95", "mg/dL", "2024-01-05T09:00:00Z", "final"),
    ("alpha", "5001", "1001", "glucose", "95", "mg/dL", "2024-01-05T09:00:00Z", "final"),  # dup
    ("alpha", "5002", "1001", "sodium", "140", "mmol/L", "2024-01-05T09:05:00Z", ""),  # null status
    ("alpha", "5003", "1002", "potassium", "4.1", "mmol/L", "2024-01-05T10:00:00Z", "Final"),
    ("alpha", "5004", "1002", "glucose", "300", "mg/dL", "2024-01-05T10:00:00Z", "retracted"),
    # Repeated lab_id with different values; observed the day after its encounter.
    ("alpha", "5005", "1003", "hemoglobin", "13.5", "g/dL", "2024-01-07T08:00:00Z", "final"),
    ("alpha", "5005", "1003", "hemoglobin", "13.6", "g/dL", "2024-01-07T08:00:00Z", "final"),
    (
        "alpha",
        "5006",
        "1005",
        "glucose",
        "88",
        "mg/dL",
        "2024-01-05T11:30:00Z",
        "final",
    ),  # cancelled enc
    (
        "alpha",
        "5007",
        "",
        "glucose",
        "88",
        "mg/dL",
        "2024-01-05T11:30:00Z",
        "final",
    ),  # no encounter
    ("alpha", "5008", "1004", "glucose", "abc", "mg/dL", "2024-01-07T09:00:00Z", "final"),
    ("alpha", "5009", "1009", "sodium", "138", "mmol/L", "2024-01-05T14:00:00Z", "final"),
    ("beta", "L-0001", "B-0001", "glucose", "101", "mg/dL", "2024-01-05T09:00:00Z", "final"),
    # test_name formatting; sentinel unit and status; padded and sentinel result_value.
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
    ("alpha", "5013", "1015", "Na", "139", "mmol/L", "2024-01-09T11:00:00Z", "final"),  # sodium
    # Sentinels in required fields: dropped.
    ("alpha", "5014", "N/A", "glucose", "90", "mg/dL", "2024-01-09T11:00:00Z", "final"),
    ("?", "5015", "1001", "glucose", "90", "mg/dL", "2024-01-05T09:00:00Z", "final"),
    ("alpha", "unknown", "1001", "glucose", "90", "mg/dL", "2024-01-05T09:00:00Z", "final"),
]

# providers: customer, provider_id, provider_name, specialty, active_flag
CASE_PROVIDERS = [
    ("alpha", "1", "Dr. Ada Synthetic", "Cardiology", "true"),
    ("alpha", "1", "Dr. Ada Synthetic", "Cardiology", "true"),  # dup
    ("alpha", "2", "Dr. Ben Example", "", "Y"),  # blank specialty -> null
    ("alpha", "3", "Dr. Cy Sample", "Pediatrics", "no"),
    ("alpha", " 3", "Dr. Cy Sample", "Neurology", "0"),  # repeated key
    ("alpha", "", "Dr. Nobody", "Surgery", "yes"),  # missing provider_id
    ("beta", "1", "Dr. Dee Placeholder", "Oncology", "F"),
    ("beta", "c0ffee00-0000-4000-8000-0000000000d1", "Dr. Eve Fictional", "Radiology", "maybe"),
    ("alpha", "4", "Dr. Flo Invented", "N/A", "N/A"),  # sentinel specialty and flag -> null
    ("alpha", "5", "Dr. Gus Madeup", f"  {NBSP} ", " TRUE "),  # whitespace specialty -> null
    ("alpha", "6", "Dr. Hal Imaginary", "Nephrology", "N"),
    ("alpha", "7", "Dr. Ivy Pretend", "Dermatology", "Yes"),
    ("alpha", "unknown", "Dr. Nil Nobody", "Surgery", "yes"),  # sentinel provider_id
    ("-", "8", "Dr. Dash Nobody", "Surgery", "yes"),  # sentinel customer
]

STATUSES = ("completed", "Completed", " completed ", "in-progress", "cancelled")
LAB_STATUSES = ("final", "final", "preliminary", "", "retracted")
TESTS = (("glucose", "mg/dL"), ("sodium", "mmol/L"), ("potassium", "mmol/L"))
SPECIALTIES = ("Family Medicine", "Internal Medicine", "Emergency", "")


def _bulk(rng: random.Random):
    """Ordinary seeded activity for 2024-02-01..05; IDs never overlap the case rows."""
    encounters, labs, providers = [], [], []
    for customer, uuids in (("alpha", False), ("beta", True)):
        provider_ids = [_uuid(rng) if uuids else str(200 + i) for i in range(4)]
        patient_ids = [_uuid(rng) if uuids else f"{3000 + i}" for i in range(12)]
        for n, pid in enumerate(provider_ids):
            providers.append(
                (customer, pid, f"Dr. Bulk {customer.title()} {n}", rng.choice(SPECIALTIES), "1")
            )
        for n in range(20):
            enc_id = _uuid(rng).upper() if uuids else str(7000 + n)
            start = BULK_START + timedelta(days=rng.randrange(BULK_DAYS), hours=rng.randint(7, 12))
            # Durations are exact binary fractions of a day, so sums compare exactly.
            end = start + timedelta(hours=rng.choice((3, 6, 12, 24)))
            patient = rng.choice(patient_ids)
            status = rng.choice(STATUSES)
            for pid in rng.sample(provider_ids, rng.choice((1, 1, 2))):
                encounters.append(
                    (customer, enc_id, patient, pid, ts(start), ts(end), ts(end), status)
                )
            for k in range(rng.randint(0, 3)):
                test, unit = rng.choice(TESTS)
                lab_id = _uuid(rng) if uuids else str(90000 + n * 10 + k)
                labs.append(
                    (
                        customer,
                        lab_id,
                        enc_id,
                        test,
                        str(rng.randint(1, 300)),
                        unit,
                        ts(start + timedelta(hours=1)),
                        rng.choice(LAB_STATUSES),
                    )
                )
    return encounters, labs, providers


def generate(seed: int = SEED) -> dict[str, list[tuple[str, ...]]]:
    rng = random.Random(seed)
    encounters, labs, providers = _bulk(rng)
    out = {
        "encounters": CASE_ENCOUNTERS + encounters,
        "labs": CASE_LABS + labs,
        "providers": CASE_PROVIDERS + providers,
    }
    for rows in out.values():
        rng.shuffle(rows)
    return out


HEADERS = {
    "encounters": BRONZE_ENCOUNTERS.fieldNames(),
    "labs": BRONZE_LABS.fieldNames(),
    "providers": BRONZE_PROVIDERS.fieldNames(),
}


def write_bronze(data_root: str | Path, seed: int = SEED) -> dict[str, Path]:
    out_dir = Path(data_root) / "bronze"
    out_dir.mkdir(parents=True, exist_ok=True)
    paths = {}
    for source, rows in generate(seed).items():
        path = out_dir / f"{source}.csv"
        with path.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh)
            writer.writerow(HEADERS[source])
            writer.writerows(rows)
        paths[source] = path
    return paths


def main() -> None:
    paths = write_bronze(require_env(DATA_ROOT).rstrip("/"))
    for source, path in paths.items():
        print(f"wrote synthetic {source} to {path}")


if __name__ == "__main__":
    main()
