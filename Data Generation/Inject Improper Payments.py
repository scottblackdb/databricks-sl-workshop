#!/usr/bin/env python3
"""Seed improper-payment / fraud patterns into the workshop TMSIS claims.

The base ``tmsis_claims.json`` is fully random, so anomaly and program-integrity
labs have nothing to detect.  This script reads the committed claims file,
*appends* a small population (~2%) of records that carry recognizable
improper-payment signatures, and writes the file back in the same single-line
JSON-array format Spark expects.

It is deterministic (fixed seed) and idempotent: it strips any previously
injected records (ICN_NUM starting with the sentinel prefix) before re-seeding,
so it can be re-run safely.

Each seeded pattern is detectable with the SQL rules in
``Lab 1 - Data Ingestion/1.8 Gold - Improper Payment Flags.ipynb``.  No column
marks a claim as fraudulent — participants find them by writing the edit checks.

Patterns seeded
---------------
A. Duplicate paid claims      -> same BENE_ID + PRVDR_ID + service date + procedure
B. Paid before service        -> MDCD_PD_DT < SRVC_BGNNG_DT
C. Discharge before admission -> DSCHRG_DT < ADMISSION_DT
D. Service before birth       -> SRVC_BGNNG_DT < BIRTH_DT
E. Provider outlier + upcoding-> a few NPIs billing high-dollar outpatient visits
F. Same bene, two states, 1 day
G. Excessive daily visit volume for a single beneficiary

Usage:
    python "Data Generation/Inject Improper Payments.py" \
        [--input workshop_setup/tmsis_claims.json] [--output <same>]
"""

from __future__ import annotations

import argparse
import copy
import json
import random
from pathlib import Path

DAY_MS = 86_400_000

# Injected records get this ICN prefix so the script can find and replace them
# on re-run, and so instructors can audit exactly what was seeded.
SEEDED_ICN_PREFIX = "9900"

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CLAIMS_FILE = REPO_ROOT / "workshop_setup" / "tmsis_claims.json"

# Outpatient office-visit procedure/bill codes used for the overpayment pattern.
OFFICE_VISIT_CODES = ["99213", "99214", "99215"]
OUTPATIENT_BILL_TYPE = "131"

# Bad-actor providers: high-volume NPIs that submit inflated outpatient claims.
BAD_PROVIDER_NPIS = [f"NPI99900{i:04d}" for i in range(6)]


def _icn(seq: int) -> str:
    """Distinct, numeric ICN for a seeded record."""
    return f"{SEEDED_ICN_PREFIX}{seq:09d}"


def load_claims(path: Path) -> list[dict]:
    with path.open() as fh:
        return json.load(fh)


def strip_previous_seed(claims: list[dict]) -> list[dict]:
    kept = [c for c in claims if not str(c.get("ICN_NUM", "")).startswith(SEEDED_ICN_PREFIX)]
    removed = len(claims) - len(kept)
    if removed:
        print(f"  Removed {removed:,} previously seeded records")
    return kept


def inject(claims: list[dict], rng: random.Random) -> list[dict]:
    seeded: list[dict] = []
    seq = 0

    def clone(src: dict) -> dict:
        nonlocal seq
        rec = copy.deepcopy(src)
        rec["ICN_NUM"] = _icn(seq)
        seq += 1
        return rec

    # Candidate pools from the legitimate data.
    outpatient = [c for c in claims if c.get("BILL_TYPE_CD") == OUTPATIENT_BILL_TYPE]
    inpatient = [c for c in claims if c.get("ADMISSION_DT") and c.get("DSCHRG_DT")]
    # Beneficiaries born recently enough that (birth - years) stays a positive epoch.
    recent_birth = [c for c in claims if c.get("BIRTH_DT", 0) > 20 * 365 * DAY_MS]

    # A. Duplicate paid claims (~500): exact re-bill with a new ICN.
    for src in rng.sample(outpatient, 500):
        seeded.append(clone(src))

    # B. Paid before service (~300): payment dated before service began.
    for src in rng.sample(outpatient, 300):
        rec = clone(src)
        rec["MDCD_PD_DT"] = rec["SRVC_BGNNG_DT"] - rng.randint(5, 30) * DAY_MS
        seeded.append(rec)

    # C. Discharge before admission (~200).
    for src in rng.sample(inpatient, 200):
        rec = clone(src)
        rec["DSCHRG_DT"] = rec["ADMISSION_DT"] - rng.randint(1, 5) * DAY_MS
        seeded.append(rec)

    # D. Service before birth (~150).
    for src in rng.sample(recent_birth, 150):
        rec = clone(src)
        rec["SRVC_BGNNG_DT"] = rec["BIRTH_DT"] - rng.randint(30, 3650) * DAY_MS
        seeded.append(rec)

    # E. Provider outlier + upcoding (~1200): 6 NPIs billing inflated office visits.
    for _ in range(1200):
        src = rng.choice(outpatient)
        rec = clone(src)
        rec["PRVDR_ID"] = rng.choice(BAD_PROVIDER_NPIS)
        rec["PRCDR_CD_1"] = "99215"
        rec["BILL_TYPE_CD"] = OUTPATIENT_BILL_TYPE
        rec["TOT_MDCD_PD_AMT"] = round(rng.uniform(1500, 3500), 2)
        seeded.append(rec)

    # F. Same beneficiary billed in two states on the same day (~150 pairs).
    other_states = ["NY", "CA", "TX", "FL", "IL"]
    for src in rng.sample(outpatient, 150):
        rec = clone(src)
        current = rec.get("SUBMTG_STATE_CD")
        rec["SUBMTG_STATE_CD"] = rng.choice([s for s in other_states if s != current])
        seeded.append(rec)  # same BENE_ID + SRVC_BGNNG_DT as the original claim

    # G. Excessive daily visit volume: 5 beneficiaries with ~30 visits in one day.
    for _ in range(5):
        base = rng.choice(outpatient)
        bene = base["BENE_ID"]
        service_dt = base["SRVC_BGNNG_DT"]
        for _ in range(rng.randint(25, 35)):
            rec = clone(base)
            rec["BENE_ID"] = bene
            rec["SRVC_BGNNG_DT"] = service_dt
            rec["SRVC_ENDG_DT"] = service_dt
            rec["PRCDR_CD_1"] = rng.choice(OFFICE_VISIT_CODES)
            rec["PRVDR_ID"] = rng.choice(BAD_PROVIDER_NPIS)
            seeded.append(rec)

    print(f"  Seeded {len(seeded):,} improper-payment records")
    return claims + seeded


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=DEFAULT_CLAIMS_FILE)
    parser.add_argument("--output", type=Path, default=None)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    output = args.output or args.input

    print(f"Reading {args.input}")
    claims = load_claims(args.input)
    print(f"  {len(claims):,} records loaded")

    claims = strip_previous_seed(claims)
    claims = inject(claims, random.Random(args.seed))

    print(f"Writing {len(claims):,} records to {output}")
    with output.open("w") as fh:
        json.dump(claims, fh, separators=(",", ":"))
    print("Done.")


if __name__ == "__main__":
    main()
