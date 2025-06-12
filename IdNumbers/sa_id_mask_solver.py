#!/usr/bin/env python3
"""
sa_id_mask_solver.py  –  generate every South-African ID that matches a 13-char mask.
  * Use ‘*’ for unknown digits in MASK.
  * Set GENDER = 'M', 'F', or 'U' (unknown).

Example:
    MASK   = "970420****08*"
    GENDER = "F"
"""
from itertools import product
from datetime import datetime

# ────>  EDIT THESE TWO LINES  <────
MASK   = "970420****08*"
GENDER = "M"              # 'M' / 'F' / 'U'
# ─────────────────────────────────

assert len(MASK) == 13 and set(MASK) <= set("0123456789*"), "Mask must be 13 chars of 0-9 or *"

def luhn_sa(stem12: str) -> str:
    d = [int(x) for x in stem12]
    odd  = sum(d[::2])
    even = sum(int(y) for y in str(int(''.join(map(str, d[1::2])))*2))
    return str((10 - (odd + even) % 10) % 10)

def valid_date(yy, mm, dd) -> bool:
    try:
        year = int(yy) + (1900 if int(yy) > int(datetime.now().strftime("%y")) else 2000)
        datetime(year, int(mm), int(dd))
        return True
    except ValueError:
        return False

def gender_ok(id13: str) -> bool:
    block = int(id13[6:10])            # digits 7-10 as a 4-digit number
    if GENDER.upper() == "M":
        return block >= 5000
    if GENDER.upper() == "F":
        return block < 5000
    return True                        # 'U' → accept either

def generate_ids(mask: str):
    opts = []
    for i, ch in enumerate(mask):
        if ch != "*":
            opts.append(ch)            # fixed digit
        else:
            opts.append("0123456789")  # wildcard
    for combo in product(*opts):
        cand = "".join(combo)
        if not (valid_date(cand[:2], cand[2:4], cand[4:6]) and gender_ok(cand)):
            continue
        if luhn_sa(cand[:-1]) == cand[-1]:
            yield cand

if __name__ == "__main__":
    total = 0
    for sa_id in generate_ids(MASK):
        print(sa_id)
        total += 1
    print(f"\nGenerated {total} IDs that satisfy the mask.")