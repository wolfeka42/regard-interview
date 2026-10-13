"""Write synthetic `{RAW_ROOT}/encounters.csv` with a fixed seed.

Every value is invented. The data deliberately includes messy identities
(padding, inner spaces, NBSP, lowercase facility), invalid identities
(blank, whitespace-only and all-zero MRNs, blank facility) and bad dates.
"""

from __future__ import annotations

import csv
import random
from datetime import date, timedelta
from pathlib import Path

from pipeline.config import RAW_ROOT, require_env
from pipeline.schemas import RAW_ENCOUNTERS

SEED = 42
FACILITIES = ("FAC1", "FAC2")
N_PATIENTS = 40
N_ENCOUNTERS = 200
ENCOUNTER_TYPES = ("OUTPATIENT", "INPATIENT", "EMERGENCY", "TELEHEALTH")
NBSP = "\u00a0"


def _messy_facility(rng: random.Random, facility: str) -> str:
    return rng.choice([facility, facility.lower(), f" {facility} ", f"{NBSP}{facility}"])


def _messy_mrn(rng: random.Random, mrn: str) -> str:
    variants = [mrn, f"00{mrn}", f" {mrn} ", f"{mrn}{NBSP}"]
    if len(mrn) > 1:
        variants.append(f"{mrn[0]} {mrn[1:]}")
    return rng.choice(variants)


def generate_encounters(seed: int = SEED) -> list[dict[str, str]]:
    rng = random.Random(seed)
    # Synthetic patients; some MRNs repeat across facilities on purpose.
    patients = []
    for i in range(N_PATIENTS):
        facility = FACILITIES[i % 2]
        mrn = str(100 + (i // 2) if i < 10 else rng.randint(1000, 99999))
        patients.append((facility, mrn))

    rows = []
    start = date(2020, 1, 1)
    for n in range(N_ENCOUNTERS):
        facility, mrn = rng.choice(patients)
        enc_date = start + timedelta(days=rng.randint(0, 1800))
        enc_type = rng.choice(ENCOUNTER_TYPES)
        discharge = ""
        if enc_type == "INPATIENT":
            discharge = (enc_date + timedelta(days=rng.randint(1, 10))).isoformat()
        rows.append(
            {
                "facility": _messy_facility(rng, facility),
                "mrn": _messy_mrn(rng, mrn),
                "encounter_id": f"E{n:05d}",
                "encounter_date": enc_date.isoformat(),
                "encounter_type": enc_type,
                "discharge_date": discharge,
            }
        )

    # Invalid rows that silver must drop and count.
    bad = [
        {"mrn": ""},
        {"mrn": "   "},
        {"mrn": NBSP},
        {"mrn": "0000"},
        {"facility": ""},
        {"facility": "  "},
        {"encounter_date": ""},
        {"encounter_date": "not-a-date"},
        {"encounter_date": "2023-13-45"},
        {"discharge_date": "2023-02-30"},
        {"discharge_date": "soon"},
    ]
    for j, override in enumerate(bad):
        row = dict(rows[j])
        row["encounter_id"] = f"X{j:05d}"
        row.update(override)
        rows.append(row)

    rng.shuffle(rows)
    return rows


def write_encounters(raw_root: str | Path, seed: int = SEED) -> Path:
    out_dir = Path(raw_root)
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "encounters.csv"
    rows = generate_encounters(seed)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=RAW_ENCOUNTERS.fieldNames())
        writer.writeheader()
        writer.writerows(rows)
    return path


def main() -> None:
    path = write_encounters(require_env(RAW_ROOT))
    print(f"wrote synthetic encounters to {path}")


if __name__ == "__main__":
    main()
