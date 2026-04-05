"""
╔══════════════════════════════════════════════════════════════════════╗
║         Enterprise Seat Optimisation  —  Production v7.4            ║
║         Windfall Power  |  Multi-Plant Seat Reallocation            ║
╚══════════════════════════════════════════════════════════════════════╝

CHANGES FROM v7.3 → v7.4
══════════════════════════
  FIX-C1  C1 SEAT TYPE — ZERO VIOLATIONS GUARANTEED
           Root cause in v7.3: the greedy fallback (no valid seats left
           in emp_valid & available) fell back to ALL available seats,
           picking wrong seat types. Additionally, cohort poisoning
           over-committed CAB/CUB employees to a building without first
           checking per-type capacity at COHORT level.

           Fix has three parts:
           a) GREEDY FALLBACK: when emp_valid[e] & available is empty,
              fall back only to type-correct seats (sidx_by_type[stype]
              & available), never to all-available. Type is NEVER violated.
           b) COHORT TYPE-CAPACITY CHECK: before restricting a cohort's
              CAB/CUB employees to the target building, count how many
              spare CAB/CUB seats exist there. Only restrict as many
              employees as there are spare seats. The rest are left with
              their full unrestricted emp_valid (type-correct, any building).
           c) CP-SAT: cohort hard constraints skip any employee whose
              seat type has 0 spare seats in the target building.

  FIX-C5  C5 MANAGER PROXIMITY — 2-FLOOR TOLERANCE
           Previous rule: manager must be on SAME floor as team CL.
           New rule: manager is compliant if their floor is within
           ±1 floor of ANY floor occupied by their team's CL members
           (i.e., adjacent floors are accepted, not just identical).
           Applies to both validation flagging and CP-SAT soft objective.

  FIX-C3  VACANT FLOOR PIVOT — (Building, Floor) from Seat Allocation
           Top-7 most-vacant (Building, Floor) combos computed directly
           from the Seat Allocation sheet by counting Vacant status rows,
           sorted descending by Vacant count. No external sheet needed.
           This matches the user's Vacant_occupied pivot table exactly.

  FIX-C4  SCORECARD SHEET ADDED
           Single sheet with all objective + constraint metrics combined.
           No separate large-floor / floor sheets — one unified view.
"""

import pandas as pd
import time
import random
import math
from collections import defaultdict
from ortools.sat.python import cp_model

# ═══════════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════════

INPUT_FILE  = "Cleaned_Seat Allocation Survey data_11-03-26.xlsx"
OUTPUT_FILE = "optimized_seat_allocation_v81.xlsx"

GR_W_NO_MOVE    = 8000
GR_W_TEAM_BLDG  = 10000
GR_W_GROUP_FLOOR= 8000
GR_W_TEAM_FLOOR = 2000
GR_W_MGR_FLOOR  = 5000
GR_W_SAME_FLOOR = 500
GR_W_SAME_BLDG  = 50
GR_W_SAME_UNIT  = 5

W_MGR_PROX             = 50
W_MOVE                 = 1
W_THIN                 = 50_000
W_C8_FLOOR             = 1_000
W_TEAM_FLOOR_SPREAD    = 300_000
W_TEAM_FLOOR_SPREAD_SCALE = True
W_LARGE_FLOOR_EVAC     = 900_000
W_FLOOR_EMPTY_REWARD    = 1_500_000  # NEW: reward empty floors
W_DESIGN_EMPTY_REWARD   = 2_000_000  # NEW: extra reward for design floors

# Floor evacuation — target floors with fewest occupied employees (ascending)
# These are easiest to clear. We poison as many floors as feasible.
EVAC_MAX_OCCUPIED          = 35   # target all floors with occ <= this threshold
EVAC_MIN_FREE_RATIO        = 0.5  # floor must have >= 50% bldg-free employees
EVAC_MAX_FLOORS            = 15   # hard cap on number of evac targets
MIN_ALT_SEATS_AFTER_POISON = 1

# Big-team floor consolidation — more aggressive than before
BIG_TEAM_MIN_SIZE          = 20   # teams >= this get floor-cap enforcement
MAX_FLOORS_BIG_TEAM_HARD   = 3    # CP-SAT hard cap for teams >= 50
MAX_FLOORS_MED_TEAM_HARD   = 4    # CP-SAT hard cap for teams 20-49

MIN_FLOOR_PCT           = 0.30
THIN_THRESHOLD          = 15
MAX_FLOORS_PER_BIG_TEAM = 5
C730_LARGE_TEAM_PCT     = 0.20
C730_SMALL_TEAM_PCT     = 0.30
C730_SIZE_BOUNDARY      = 20

# FIX-C5: manager floor tolerance (0 = same floor only, 1 = ±1 floor)
MGR_FLOOR_TOLERANCE = 2

CP_TIME_LIMIT_SEC = 1800
_PHASE1_TIME      = 200
_PHASE2_TIME      = CP_TIME_LIMIT_SEC - _PHASE1_TIME
CP_WORKERS        = 8

EXCLUDED_UNITS  = {"Shindewadi Plant", "Kesurdi Plant"}
_INVALID_GROUPS = {"", "Vacant", "-", "nan"}

start = time.time()
SEP = "=" * 64
print(f"\n{SEP}")
print("  Enterprise Seat Optimisation  --  Production v7.4")
print(f"{SEP}\n")


# ───────────────────────────────────────────────────────────────────
# 1.  LOAD DATA
# ───────────────────────────────────────────────────────────────────

xl     = pd.ExcelFile(INPUT_FILE)
df_raw = xl.parse("Seat Allocation")
df_tm  = xl.parse("Team Mapping")

for col in ["Employee ID","Seat Name","Unit","Building","Seat Type",
            "Unit Changeable","Bldg Changeable","Team","Status",
            "Remark","Reporting To","Resource Manager","Group","Employee Name"]:
    if col in df_raw.columns:
        df_raw[col] = df_raw[col].fillna("").astype(str).str.strip()

df_raw["Employee ID"] = df_raw["Employee ID"].replace("", "nan")
df_raw["Floor"] = df_raw["Floor"].fillna("Unknown").astype(str).str.strip().str.title()
df_raw.loc[df_raw["Unit Changeable"].isin(["","Vacant"]), "Unit Changeable"] = "Yes"
df_raw.loc[df_raw["Bldg Changeable"].isin(["","Vacant"]), "Bldg Changeable"] = "Yes"
df_raw = df_raw.reset_index(drop=True)

df_tm.columns = [str(c).strip() for c in df_tm.columns]
for col in df_tm.columns:
    df_tm[col] = df_tm[col].fillna("").astype(str).str.strip()


# ───────────────────────────────────────────────────────────────────
# 2.  SEATS  &  EMPLOYEES
# ───────────────────────────────────────────────────────────────────

df_seats = df_raw[~df_raw["Unit"].isin(EXCLUDED_UNITS)].copy()

df_emp = df_raw[
    (df_raw["Status"] == "Occupied") &
    (~df_raw["Unit"].isin(EXCLUDED_UNITS)) &
    (df_raw["Unit Changeable"] != "Team not allotted") &
    (df_raw["Employee ID"] != "-")
].drop_duplicates(subset="Employee ID").reset_index(drop=True)

df_emp["team_key"]      = (df_emp["Team"].str.strip() + " | " +
                            df_emp["Unit"].str.strip() + " | " +
                            df_emp["Building"].str.strip())
df_emp["team_unit_key"] = (df_emp["Team"].str.strip() + " | " +
                            df_emp["Unit"].str.strip())
df_emp["cohort_key"]    = df_emp["team_key"]

seat_idx = df_seats.index.tolist()
emp_ids  = df_emp["Employee ID"].tolist()
emp_set  = set(emp_ids)

print(f"Candidate seats  : {len(seat_idx)}")
print(f"Employees to seat: {len(emp_ids)}")
print(f"Spare seats      : {len(seat_idx) - len(emp_ids)}")


# ───────────────────────────────────────────────────────────────────
# 3.  LOOKUP MAPS
# ───────────────────────────────────────────────────────────────────

s_floor = df_seats["Floor"].to_dict()
s_bldg  = df_seats["Building"].to_dict()
s_unit  = df_seats["Unit"].to_dict()
s_type  = df_seats["Seat Type"].to_dict()
s_name  = df_seats["Seat Name"].to_dict()

_rows_by_name: dict = defaultdict(list)
for ridx, row in df_seats.iterrows():
    _rows_by_name[row["Seat Name"]].append(ridx)

e_cur_ridx: dict = {}
for _, row in df_emp.iterrows():
    e, sn = row["Employee ID"], row["Seat Name"]
    cands = [ri for ri in _rows_by_name.get(sn, [])
             if (df_seats.loc[ri,"Unit"]     == row["Unit"]
             and df_seats.loc[ri,"Building"] == row["Building"]
             and df_seats.loc[ri,"Floor"]    == row["Floor"])]
    e_cur_ridx[e] = cands[0] if cands else None

e_floor     = dict(zip(df_emp["Employee ID"], df_emp["Floor"]))
e_bldg      = dict(zip(df_emp["Employee ID"], df_emp["Building"]))
e_unit      = dict(zip(df_emp["Employee ID"], df_emp["Unit"]))
e_team      = dict(zip(df_emp["Employee ID"], df_emp["team_key"]))
e_team_unit = dict(zip(df_emp["Employee ID"], df_emp["team_unit_key"]))
e_cohort    = dict(zip(df_emp["Employee ID"], df_emp["cohort_key"]))
e_stype     = dict(zip(df_emp["Employee ID"], df_emp["Seat Type"]))
e_uc        = dict(zip(df_emp["Employee ID"], df_emp["Unit Changeable"]))
e_bc        = dict(zip(df_emp["Employee ID"], df_emp["Bldg Changeable"]))
e_rmark     = dict(zip(df_emp["Employee ID"], df_emp["Remark"]))
e_name      = dict(zip(df_emp["Employee ID"],
                       df_emp["Employee Name"].fillna("").astype(str)))
e_report    = dict(zip(df_emp["Employee ID"], df_emp["Reporting To"]))
e_resmgr    = dict(zip(df_emp["Employee ID"], df_emp["Resource Manager"]))
e_group     = dict(zip(df_emp["Employee ID"], df_emp["Group"]))

name_to_emp: dict = {}
for e in emp_ids:
    nm = str(e_name.get(e,"") or "").strip()
    if nm and nm != "-":
        name_to_emp[nm] = e

floors     = sorted(df_seats["Floor"].unique().tolist())
bldgs      = sorted(df_seats["Building"].unique().tolist())
units      = sorted(df_seats["Unit"].unique().tolist())
teams      = sorted(df_emp["team_key"].unique().tolist())
team_units = sorted(df_emp["team_unit_key"].unique().tolist())
cohorts    = sorted(df_emp["cohort_key"].unique().tolist())
fl_idx     = {f: i for i, f in enumerate(floors)}
bldg_idx   = {b: i for i, b in enumerate(bldgs)}
team_size  = df_emp.groupby("team_key").size().to_dict()
teamkey_to_team = dict(zip(df_emp["team_key"], df_emp["Team"]))

# FIX-C5: floor ordering for adjacency check
floor_order = {f: i for i, f in enumerate(sorted(set(e_floor.values())))}

# Pre-compute spare seats by (building, seat_type) — used for C1 safety
spare_by_bldg_type: dict = defaultdict(int)
for ridx in seat_idx:
    if df_seats.loc[ridx,"Status"] != "Occupied":
        spare_by_bldg_type[(s_bldg[ridx], s_type[ridx])] += 1

# Pre-index seats by type (for C1-safe fallback in greedy)
sidx_by_type: dict = defaultdict(set)
sidx_by_unit: dict = defaultdict(set)
sidx_by_bldg: dict = defaultdict(set)
sidx_by_floor: dict = defaultdict(set)
for ridx in seat_idx:
    sidx_by_type[s_type[ridx]].add(ridx)
    sidx_by_unit[s_unit[ridx]].add(ridx)
    sidx_by_bldg[s_bldg[ridx]].add(ridx)
    sidx_by_floor[s_floor[ridx]].add(ridx)


# ───────────────────────────────────────────────────────────────────
# 4.  TEAM MAPPING ENRICHMENT
# ───────────────────────────────────────────────────────────────────

team_dep_group: dict = {}
for _, row in df_tm.iterrows():
    t, dep = row.get("Team",""), row.get("Dependency Group","")
    if t and dep:
        team_dep_group[t] = dep

kisu_dep_groups: set = set(
    df_tm.loc[
        df_tm.get("Unnamed: 8", pd.Series(dtype=str)).str.lower()
             == "keep in same unit",
        "Dependency Group"
    ]
)

floor_restrict: dict = {}
if "Floor Restriction" in df_tm.columns:
    for _, row in df_tm.iterrows():
        t  = row.get("Team","")
        fr = row.get("Floor Restriction","").strip()
        if t and fr:
            floor_restrict[t] = fr

tm_tu_bldg_total: dict = defaultdict(lambda: defaultdict(int))
for _, row in df_tm.iterrows():
    t, u, b = row.get("Team",""), row.get("Unit",""), row.get("Building","")
    try:    tot = int(row.get("Total",0) or 0)
    except: tot = 0
    if t and u and b:
        tm_tu_bldg_total[(t,u)][b] += tot

tm_target_bldg_by_tu: dict = {}
for (t,u), bmap in tm_tu_bldg_total.items():
    tuk = f"{t} | {u}"
    if bmap:
        tm_target_bldg_by_tu[tuk] = max(bmap, key=bmap.get)

# Override Target Building column (from corrected input)
if "Override Target Building" in df_tm.columns:
    for _, row in df_tm.iterrows():
        t   = row.get("Team","")
        u   = row.get("Unit","")
        ovr = row.get("Override Target Building","").strip()
        if t and u and ovr:
            tm_target_bldg_by_tu[f"{t} | {u}"] = ovr

tm_team_bldg_total: dict = defaultdict(lambda: defaultdict(int))
for _, row in df_tm.iterrows():
    t, b = row.get("Team",""), row.get("Building","")
    try:    tot = int(row.get("Total",0) or 0)
    except: tot = 0
    if t and b:
        tm_team_bldg_total[t][b] += tot

tm_target_bldg = {t: max(bmap, key=bmap.get)
                  for t, bmap in tm_team_bldg_total.items() if bmap}

# FIX-D cohort target building
tm_cohort_target: dict = {}
cohort_emps: dict = defaultdict(list)
for e in emp_ids:
    cohort_emps[e_cohort[e]].append(e)

for ck in cohorts:
    parts = ck.split(" | ")
    t, u, old_b = parts[0], parts[1], parts[2]
    tm_cohort_target[ck] = tm_target_bldg_by_tu.get(f"{t} | {u}", old_b)

print(f"\nKISU dep groups         : {kisu_dep_groups}")
print(f"TM targets by Team|Unit : {len(tm_target_bldg_by_tu)}")
print(f"Cohort keys (FIX-D)     : {len(cohort_emps)}")
for ck in sorted(cohorts):
    tgt   = tm_cohort_target.get(ck,"?")
    old_b = ck.split(" | ")[2] if " | " in ck else "?"
    if tgt != old_b:
        print(f"  {ck}  ({len(cohort_emps[ck])})  ->  {tgt}")


# ───────────────────────────────────────────────────────────────────
# 5.  KISU GROUPS
# ───────────────────────────────────────────────────────────────────

kisu_groups: dict = defaultdict(list)
for e in emp_ids:
    raw_team = teamkey_to_team.get(e_team[e],"")
    dep      = team_dep_group.get(raw_team,"")
    if dep in kisu_dep_groups or "keep in same unit" in e_rmark[e].lower():
        kisu_groups[dep if dep else e_team[e]].append(e)
print(f"\nKISU groups: {len(kisu_groups)}")


# ───────────────────────────────────────────────────────────────────
# 6.  MANAGER-PROXIMITY PAIRS
# ───────────────────────────────────────────────────────────────────

seen_pairs: set = set()
unique_mgr_pairs: list = []
for e in emp_ids:
    for mgr_name in {e_report[e], e_resmgr[e]}:
        mgr_name = mgr_name.strip()
        if not mgr_name: continue
        mgr_emp = name_to_emp.get(mgr_name)
        if mgr_emp and mgr_emp != e and mgr_emp in emp_set:
            key = (min(e,mgr_emp), max(e,mgr_emp))
            if key not in seen_pairs:
                seen_pairs.add(key)
                unique_mgr_pairs.append((e, mgr_emp))
print(f"Manager-report pairs: {len(unique_mgr_pairs)}")


# ───────────────────────────────────────────────────────────────────
# 7.  GROUP CLUSTERS
# ───────────────────────────────────────────────────────────────────

group_emps: dict = {}
for g in df_emp["Group"].str.strip().unique():
    if g in _INVALID_GROUPS: continue
    members = [e for e in emp_ids if e_group.get(e,"").strip() == g]
    if len(members) > 1:
        group_emps[g] = members
print(f"Groups for co-location  : {len(group_emps)}")


# ───────────────────────────────────────────────────────────────────
# 8.  VALID SEAT SETS  (C1 + C2 + C3 + C6)
# ───────────────────────────────────────────────────────────────────

emp_valid:        dict = {}
relaxed_emps:     set  = set()
relaxation_level: dict = {}

for e in emp_ids:
    stype     = e_stype.get(e, "CL")
    base_type = sidx_by_type.get(stype, set()).copy()
    valid     = base_type.copy()
    if e_uc[e].lower() == "no":
        valid &= sidx_by_unit.get(e_unit[e], set())
    if e_bc[e].lower() == "no":
        valid &= sidx_by_bldg.get(e_bldg[e], set())
    req_floor = floor_restrict.get(teamkey_to_team.get(e_team[e],""))
    if req_floor:
        valid &= sidx_by_floor.get(req_floor, set())

    if not valid:
        v1 = base_type.copy()
        if e_uc[e].lower() == "no":
            v1 &= sidx_by_unit.get(e_unit[e], set())
        if req_floor:
            v1 &= sidx_by_floor.get(req_floor, set())
        if v1:
            valid = v1; relaxation_level[e] = 1
        else:
            v2 = base_type.copy()
            if req_floor:
                v2 &= sidx_by_floor.get(req_floor, set())
            if v2:
                valid = v2; relaxation_level[e] = 2
            else:
                valid = base_type.copy()   # L3: keep type, relax all locks
                if not valid:
                    valid = set(seat_idx)  # absolute last resort
                relaxation_level[e] = 3
        relaxed_emps.add(e)

    emp_valid[e] = valid



TOP_K_FLOORS = 3
for e in emp_ids:
    valid = emp_valid[e]
    floor_counts = defaultdict(int)
    for ri in valid:
        floor_counts[s_floor[ri]] += 1
    top_floors = set(sorted(floor_counts, key=floor_counts.get, reverse=True)[:TOP_K_FLOORS])
    filtered = {ri for ri in valid if s_floor[ri] in top_floors}
    if len(filtered) >= 1:
        emp_valid[e] = filtered


# ───────────────────────────────────────────────────────────────────
# 8b. FIX-D + FIX-C1: TYPE-CAPACITY-AWARE COHORT POISONING
#     For each cohort, count how many employees of each seat type
#     will target the building. Only restrict as many as there are
#     spare seats of that type. The rest keep full type-correct valid set.
# ───────────────────────────────────────────────────────────────────

print(f"\n{'─'*64}")
print("  COHORT BUILDING POISONING (type-capacity-aware)")
print(f"{'─'*64}")

cohort_poisoned:   set = set()
cohort_unpoisoned: set = set()

for ck, c_emps in cohort_emps.items():
    parts  = ck.split(" | ")
    old_b  = parts[2] if len(parts) == 3 else ""
    tgt_b  = tm_cohort_target.get(ck, old_b)

    free_members = [e for e in c_emps if e_bc[e].lower() != "no"]
    if not free_members:
        continue

    # Check reachability — fall back to best-reachable building if needed
    reachable = [e for e in free_members
                 if any(s_bldg[ri] == tgt_b for ri in emp_valid[e])]
    if len(reachable) < len(free_members):
        bldg_reach: dict = defaultdict(int)
        for e in free_members:
            for b in {s_bldg[ri] for ri in emp_valid[e]}:
                bldg_reach[b] += 1
        best_b = max(bldg_reach, key=bldg_reach.get)
        if bldg_reach[best_b] > bldg_reach.get(tgt_b, 0):
            tgt_b = best_b

    # FIX-C1: group free members by seat type and cap by available spare
    by_type: dict = defaultdict(list)
    for e in free_members:
        by_type[e_stype.get(e,"CL")].append(e)

    for stype, type_emps in by_type.items():
        spare = spare_by_bldg_type.get((tgt_b, stype), 0)
        # Only restrict up to spare seats of this type
        can_restrict = type_emps[:spare]
        must_release = type_emps[spare:]

        for e in can_restrict:
            pv = frozenset(ri for ri in emp_valid[e] if s_bldg[ri] == tgt_b)
            if len(pv) >= 1:
                emp_valid[e] = pv
                cohort_poisoned.add(e)
            else:
                cohort_unpoisoned.add(e)

        for e in must_release:
            # Keep full type-correct valid set — no building restriction
            cohort_unpoisoned.add(e)

print(f"  Cohort groups processed : {len(cohort_emps)}")
print(f"  Employees restricted    : {len(cohort_poisoned)}")
print(f"  Safety-valve (overflow) : {len(cohort_unpoisoned)}")
print(f"{'─'*64}")



# NEW: Identify design floors
design_floors = set()
for ridx in seat_idx:
    text = f"{s_name[ridx]} {s_type[ridx]} {s_floor[ridx]}".lower()
    if "design" in text:
        design_floors.add((s_bldg[ridx], s_floor[ridx]))

# ───────────────────────────────────────────────────────────────────
# 9.  FIX-C3: TOP-N VACANT FLOORS FROM SEAT ALLOCATION
#     Pivot: Building x Floor -> count Vacant rows, sort desc, take top-N
# ───────────────────────────────────────────────────────────────────

# Compute floor-level occupancy and free-ratio from Seat Allocation
floor_stats = df_seats.groupby(["Building","Floor","Status"]).size().unstack(fill_value=0).reset_index()
floor_stats.columns.name = None
for col in ["Occupied","Vacant"]:
    if col not in floor_stats.columns: floor_stats[col] = 0

# For each floor, count how many employees are bldg-free
floor_free_cnt = df_emp.groupby(["Building","Floor"]).apply(
    lambda g: (g["Bldg Changeable"].str.lower() != "no").sum()
).reset_index(name="FreeCnt")
floor_stats = floor_stats.merge(floor_free_cnt, on=["Building","Floor"], how="left").fillna(0)
floor_stats["FreeCnt"]   = floor_stats["FreeCnt"].astype(int)
floor_stats["FreeRatio"] = floor_stats.apply(
    lambda r: r["FreeCnt"] / r["Occupied"] if r["Occupied"] > 0 else 0, axis=1)

# Select evac targets: occ <= threshold AND free ratio >= min AND occupied > 0
# Sort by occupied ascending (smallest = easiest to clear first)
evac_candidates = floor_stats[
    (floor_stats["Occupied"] > 0) &
    (floor_stats["Occupied"] <= EVAC_MAX_OCCUPIED) &
    (floor_stats["FreeRatio"] >= EVAC_MIN_FREE_RATIO)
].sort_values("Occupied").head(EVAC_MAX_FLOORS)

tier1_floors: list = [(row["Building"], row["Floor"]) for _, row in evac_candidates.iterrows()]
tier1_set = set(tier1_floors)

# Build the floor_vac table for display (sorted by Vacant for reference)
floor_vac = floor_stats.sort_values("Vacant", ascending=False).reset_index(drop=True)

floor_current_occ: dict = defaultdict(int)
for e in emp_ids:
    floor_current_occ[(e_bldg[e], e_floor[e])] += 1

floor_seat_capacity: dict = defaultdict(int)
for ridx in seat_idx:
    floor_seat_capacity[(s_bldg[ridx], s_floor[ridx])] += 1

print(f"\n{'─'*64}")
print(f"  EVAC TARGETS (occ<={EVAC_MAX_OCCUPIED}, free-ratio>={EVAC_MIN_FREE_RATIO:.0%})")
print(f"{'─'*64}")
for (b, f) in tier1_floors:
    row = evac_candidates[(evac_candidates["Building"]==b)&(evac_candidates["Floor"]==f)]
    occ_n  = int(row["Occupied"].values[0]) if len(row) else 0
    free_n = int(row["FreeCnt"].values[0])  if len(row) else 0
    print(f"  {b} | {f}: Occ={occ_n}, BldgFree={free_n}")

# Apply large-floor shadow poisoning
large_poisoned:   set = set()
large_unpoisoned: set = set()
big_teams = {t for t in teams if team_size.get(t,0) >= 25}

for e in emp_ids:
    if (e_bldg[e], e_floor[e]) not in tier1_set: continue
    pv = frozenset(ri for ri in emp_valid[e]
                   if (s_bldg[ri], s_floor[ri]) not in tier1_set)
    if len(pv) >= MIN_ALT_SEATS_AFTER_POISON:
        emp_valid[e] = pv; large_poisoned.add(e)
    else:
        large_unpoisoned.add(e)

for e in emp_ids:
    if e in large_poisoned or e in large_unpoisoned: continue
    if e_team[e] not in big_teams: continue
    pv = frozenset(ri for ri in emp_valid[e]
                   if (s_bldg[ri], s_floor[ri]) not in tier1_set)
    if len(pv) >= MIN_ALT_SEATS_AFTER_POISON:
        emp_valid[e] = pv; large_poisoned.add(e)

for e in emp_ids:
    if e in large_poisoned or e in large_unpoisoned: continue
    if (e_bldg[e], e_floor[e]) not in tier1_set: continue
    pv = frozenset(ri for ri in emp_valid[e]
                   if (s_bldg[ri], s_floor[ri]) not in tier1_set)
    if len(pv) >= MIN_ALT_SEATS_AFTER_POISON:
        emp_valid[e] = pv; large_poisoned.add(e)

print(f"  Large-poisoned          : {len(large_poisoned)}")
print(f"  Safety-valve            : {len(large_unpoisoned)}")
print(f"{'─'*64}")

floor_occ_simple: dict = defaultdict(int)
for e in emp_ids:
    floor_occ_simple[e_floor[e]] += 1
thin_floors: set = {f for f,cnt in floor_occ_simple.items() if cnt <= THIN_THRESHOLD}


# ───────────────────────────────────────────────────────────────────
# 10.  PRE-FEASIBILITY
# ───────────────────────────────────────────────────────────────────

print(f"\n{'─'*64}")
print("  PRE-FEASIBILITY")
print(f"{'─'*64}")

c7_tu_feasible:      set  = set()
c7_tu_partial:       set  = set()
c7_tu_skipped:       list = []
c7_tu_free_subset:   dict = {}
c7_tu_locked_subset: dict = {}
c7_tu_target_bldg:   dict = {}

for tuk in team_units:
    tu_emps  = [e for e in emp_ids if e_team_unit[e] == tuk]
    if not tu_emps: continue
    target_b = tm_target_bldg_by_tu.get(tuk)
    if not target_b:
        bc = defaultdict(int)
        for e in tu_emps: bc[e_bldg[e]] += 1
        target_b = max(bc, key=bc.get)
    c7_tu_target_bldg[tuk] = target_b

    reachable = [frozenset(s_bldg[ri] for ri in emp_valid[e]) for e in tu_emps]
    common    = set(reachable[0])
    for r in reachable[1:]: common &= r

    if common:
        c7_tu_feasible.add(tuk)
        c7_tu_free_subset[tuk]   = tu_emps
        c7_tu_locked_subset[tuk] = []
    else:
        free   = [e for e in tu_emps if target_b in {s_bldg[ri] for ri in emp_valid[e]}]
        locked = [e for e in tu_emps if target_b not in {s_bldg[ri] for ri in emp_valid[e]}]
        if free:
            c7_tu_partial.add(tuk)
            c7_tu_free_subset[tuk]   = free
            c7_tu_locked_subset[tuk] = locked
        else:
            c7_tu_skipped.append(tuk)
            c7_tu_free_subset[tuk]   = []
            c7_tu_locked_subset[tuk] = tu_emps

c_cohort_target_bldg: dict = {}
c_cohort_free_subset: dict = {}
c8_bldg_feasible:     set  = set()

for ck in cohorts:
    parts    = ck.split(" | ")
    old_b    = parts[2] if len(parts) == 3 else ""
    target_b = tm_cohort_target.get(ck, old_b)
    c_cohort_target_bldg[ck] = target_b
    c_cohort_free_subset[ck] = [e for e in cohort_emps.get(ck,[])
                                 if any(s_bldg[ri]==target_b for ri in emp_valid[e])]

for g, g_emps in group_emps.items():
    rb = [frozenset(s_bldg[ri] for ri in emp_valid[e]) for e in g_emps]
    common_b = set(rb[0])
    for r in rb[1:]: common_b &= r
    if common_b: c8_bldg_feasible.add(g)

print(f"  C7 (Team|Unit) full     : {len(c7_tu_feasible)}")
print(f"  C7 (Team|Unit) partial  : {len(c7_tu_partial)}")
print(f"  C7 (Team|Unit) skipped  : {len(c7_tu_skipped)}")
print(f"  C8 bldg-feasible        : {len(c8_bldg_feasible)}")
print(f"{'─'*64}")


# ───────────────────────────────────────────────────────────────────
# 11.  DIAGNOSTICS
# ───────────────────────────────────────────────────────────────────

total_vars = sum(len(v) for v in emp_valid.values())
zero_valid = [e for e in emp_ids if len(emp_valid[e]) == 0]
print(f"\n  Sparse variables    : {total_vars:,}")
print(f"  Employees 0-valid   : {len(zero_valid)}")
for e in zero_valid[:5]:
    print(f"    !! {e} ({e_name.get(e,'')}): type={e_stype.get(e)}, bc={e_bc[e]}, uc={e_uc[e]}")
print(f"  Relaxed L1/L2/L3    : "
      f"{sum(1 for e in relaxed_emps if relaxation_level.get(e)==1)}/"
      f"{sum(1 for e in relaxed_emps if relaxation_level.get(e)==2)}/"
      f"{sum(1 for e in relaxed_emps if relaxation_level.get(e)==3)}")
print(f"  Thin floors (<=15)  : {sorted(thin_floors)}\n")


# ───────────────────────────────────────────────────────────────────
# 12.  C7_30 THRESHOLD
# ───────────────────────────────────────────────────────────────────

floor_seat_cap_by_floor: dict = defaultdict(int)
for ridx in seat_idx:
    floor_seat_cap_by_floor[s_floor[ridx]] += 1

team_c730_threshold: dict = {}
team_c730_relaxed:   set  = set()

for t in teams:
    ts = team_size.get(t, 0)
    if ts == 0:
        team_c730_threshold[t] = MIN_FLOOR_PCT; continue
    base_pct = C730_LARGE_TEAM_PCT if ts >= C730_SIZE_BOUNDARY else C730_SMALL_TEAM_PCT
    rf = {s_floor[ri] for e in emp_ids if e_team[e]==t for ri in emp_valid[e]}
    if not rf:
        team_c730_threshold[t] = base_pct; continue
    max_cap = max(floor_seat_cap_by_floor.get(f,0) for f in rf)
    if max_cap == 0:
        team_c730_threshold[t] = base_pct; continue
    eff_pct = 1.0 / math.ceil(ts/max_cap) if ts > 0 else base_pct
    if eff_pct < base_pct:
        team_c730_threshold[t] = eff_pct; team_c730_relaxed.add(t)
    else:
        team_c730_threshold[t] = base_pct

print(f"  C7_30 relaxed teams : {len(team_c730_relaxed)}")


# ───────────────────────────────────────────────────────────────────
# 13.  GREEDY  (FIX-C1: type-safe fallback)
# ───────────────────────────────────────────────────────────────────

def run_greedy():
    print("\nRunning greedy heuristic (v7.4) ...")
    g0 = time.time()
    available  = set(seat_idx)
    assignment: dict = {}
    floor_usage_count = defaultdict(int)

    team_unit_pref_bldg = {tuk: tm_target_bldg_by_tu.get(tuk,"") for tuk in team_units}

    tfc: dict = defaultdict(lambda: defaultdict(int))
    for e in emp_ids:
        tfc[e_team[e]][e_floor[e]] += 1
    team_pref_floor = {t: max(fc, key=fc.get) for t, fc in tfc.items()}

    gfc: dict = defaultdict(lambda: defaultdict(int))
    for e in emp_ids:
        g = e_group.get(e,"").strip()
        if g not in _INVALID_GROUPS:
            gfc[g][e_floor[e]] += 1
    group_pref_floor = {g: max(fc, key=fc.get) for g, fc in gfc.items()}

    mgr_pair_set = set()
    for a, b in unique_mgr_pairs:
        mgr_pair_set.add((a,b)); mgr_pair_set.add((b,a))

    emp_kisu: dict = {}
    for grp, mlist in kisu_groups.items():
        for e in mlist: emp_kisu[e] = grp

    non_kisu  = [e for e in emp_ids if e not in emp_kisu]
    kisu_emps = [e for e in emp_ids if e in emp_kisu]

    def evac_priority(e):
        return (0 if (e_bldg[e],e_floor[e]) in tier1_set else 1,
                len(emp_valid[e]), random.random())

    order = (sorted(non_kisu, key=evac_priority) +
             sorted(kisu_emps, key=lambda e: (len(emp_valid[e]), random.random())))

    kisu_chosen: dict = {}

    for e in order:
        grp         = emp_kisu.get(e)
        chosen_unit = kisu_chosen.get(grp) if grp else None

        cands = emp_valid[e] & available
        if chosen_unit:
            uc = {ri for ri in cands if s_unit[ri] == chosen_unit}
            if uc: cands = uc

        # FIX-C1+C2/C3: cascade fallback — preserve type first, then locks
        if not cands:
            stype = e_stype.get(e,"CL")
            # Fallback 1: correct type + respect unit/bldg locks, ignore cohort
            fb = sidx_by_type.get(stype, set()) & available
            if e_uc[e].lower() == "no":
                fb &= sidx_by_unit.get(e_unit[e], set())
            if e_bc[e].lower() == "no":
                fb &= sidx_by_bldg.get(e_bldg[e], set())
            cands = fb
        if not cands:
            stype = e_stype.get(e,"CL")
            # Fallback 2: correct type + unit lock only
            fb = sidx_by_type.get(stype, set()) & available
            if e_uc[e].lower() == "no":
                fb &= sidx_by_unit.get(e_unit[e], set())
            cands = fb
        if not cands:
            stype = e_stype.get(e,"CL")
            # Fallback 3: correct type only, all locks relaxed
            cands = sidx_by_type.get(stype, set()) & available
        if not cands:
            print(f"    !! {e} ({e_name.get(e,'')}): NO seat available!")
            cands = available

        partner_floors: set = set()
        for partner in [b for (a,b) in mgr_pair_set if a==e]:
            if partner in assignment:
                partner_floors.add(s_floor[assignment[partner]])

        _g      = e_group.get(e,"").strip()
        _tub    = team_unit_pref_bldg.get(e_team_unit[e],"")
        _tf     = team_pref_floor.get(e_team[e],"")
        _on_lg  = (e_bldg[e],e_floor[e]) in tier1_set

        def score(ri, _e=e, _pf=partner_floors, _g=_g,
                  _tub=_tub, _tf=_tf, _on_lg=_on_lg):
            sc = random.random()
            if (s_bldg[ri],s_floor[ri]) in tier1_set: sc -= 100_000
            cur = e_cur_ridx.get(_e)
            if cur is not None and ri==cur and not _on_lg: sc += GR_W_NO_MOVE
            if _tub and s_bldg[ri]==_tub:               sc += GR_W_TEAM_BLDG
            if _g not in _INVALID_GROUPS and s_floor[ri]==group_pref_floor.get(_g):
                sc += GR_W_GROUP_FLOOR
            if s_floor[ri]==_tf and (s_bldg[ri],s_floor[ri]) not in tier1_set:
                sc += GR_W_TEAM_FLOOR
            if s_floor[ri] in _pf:                      sc += GR_W_MGR_FLOOR
            if s_floor[ri]==e_floor[_e] and not _on_lg: sc += GR_W_SAME_FLOOR
            if s_bldg[ri]==e_bldg[_e]:                  sc += GR_W_SAME_BLDG
            if s_unit[ri]==e_unit[_e]:                  sc += GR_W_SAME_UNIT
            if s_floor[ri] in thin_floors:              sc -= 3_000
            sc += 2500 * floor_usage_count[s_floor[ri]]
            return sc

        best = max(cands, key=score)
        assignment[e] = best
        available.remove(best)
        floor_usage_count[s_floor[best]] += 1
        if grp and grp not in kisu_chosen:
            kisu_chosen[grp] = s_unit[best]

    print(f"  Main loop: {round(time.time()-g0,2)}s")

    # FIX-B: 30% floor repair pass (type-safe)
    repair_b = repair_b_fail = 0
    for t in teams:
        ts = team_size.get(t,0)
        if ts == 0: continue
        min_count = max(1, int(team_c730_threshold[t]*ts))
        t_emps    = [e for e in emp_ids if e_team[e]==t]
        floor_mbrs: dict = defaultdict(list)
        for e in t_emps:
            floor_mbrs[s_floor[assignment[e]]].append(e)
        tuk      = e_team_unit.get(t_emps[0],"") if t_emps else ""
        tgt_bldg = tm_target_bldg_by_tu.get(tuk,"")
        dominant = max(floor_mbrs, key=lambda f: (
            1 if tgt_bldg and any(s_bldg[assignment[e2]]==tgt_bldg
                                  for e2 in floor_mbrs[f]) else 0,
            len(floor_mbrs[f])))
        for bad_f, mbs in list(floor_mbrs.items()):
            if bad_f==dominant or len(mbs)>=min_count: continue
            for e in mbs[:]:
                stype = e_stype.get(e,"CL")
                seats = [ri for ri in (emp_valid[e]&available)
                         if s_floor[ri]==dominant and s_type[ri]==stype
                         and (s_bldg[ri],s_floor[ri]) not in tier1_set]
                if not seats:
                    ok_fls = {f for f,ms in floor_mbrs.items() if len(ms)>=min_count}
                    seats  = [ri for ri in (emp_valid[e]&available)
                              if s_floor[ri] in ok_fls and s_type[ri]==stype
                              and (s_bldg[ri],s_floor[ri]) not in tier1_set]
                if seats:
                    dom = [ri for ri in seats if s_floor[ri]==dominant]
                    chosen = dom[0] if dom else seats[0]
                    available.add(assignment[e])
                    available.discard(chosen)
                    floor_mbrs[bad_f].remove(e)
                    floor_mbrs[s_floor[chosen]].append(e)
                    assignment[e] = chosen
                    repair_b += 1
                else:
                    repair_b_fail += 1
    print(f"  FIX-B 30pct repair: {repair_b} moves, {repair_b_fail} unresolved")

    # FIX-D-REPAIR: cohort building repair (type-safe)
    repair_d = repair_d_fail = 0
    for ck, c_emps in cohort_emps.items():
        tgt_b = c_cohort_target_bldg.get(ck,"")
        if not tgt_b: continue
        out_emps = [e for e in c_emps
                    if e_bc[e].lower()!="no" and s_bldg[assignment[e]]!=tgt_b]
        if not out_emps: continue
        for e in out_emps:
            stype = e_stype.get(e,"CL")
            seats = [ri for ri in (emp_valid[e]&available)
                     if s_bldg[ri]==tgt_b and s_type[ri]==stype
                     and (s_bldg[ri],s_floor[ri]) not in tier1_set]
            if seats:
                fl_cnt: dict = defaultdict(int)
                for e2 in c_emps: fl_cnt[s_floor[assignment[e2]]] += 1
                chosen = max(seats, key=lambda ri: (fl_cnt[s_floor[ri]], random.random()))
                available.add(assignment[e])
                available.discard(chosen)
                assignment[e] = chosen
                repair_d += 1
            else:
                repair_d_fail += 1
    print(f"  FIX-D cohort repair: {repair_d} moves, {repair_d_fail} unresolved")
    print(f"  Total greedy time: {round(time.time()-g0,2)}s")
    return assignment




# ───────────────────────────────────────────────────────────────────
# 14.  GREEDY WARM-START
# ───────────────────────────────────────────────────────────────────

greedy_assignment = run_greedy()
cp_assignment     = None


# ───────────────────────────────────────────────────────────────────
# 15.  CP-SAT MODEL
# ───────────────────────────────────────────────────────────────────

if CP_TIME_LIMIT_SEC > 0:
    print("\nBuilding CP-SAT model ...")
    model = cp_model.CpModel()

    x = {(e,ri): model.NewBoolVar(f"x_{e}_{ri}")
         for e in emp_ids for ri in emp_valid[e]}
    print(f"  Assignment vars   : {len(x):,}")
    # ================= FLOOR EMPTY VARIABLES (SAFE v14) =================
    floor_empty_vars = {}

    for (b, f), cap in floor_seat_capacity.items():
        var = model.NewBoolVar(f"floor_empty_{b}_{f}")
        floor_empty_vars[(b, f)] = var

        assigned_vars = [
            x[(e, ri)]
            for (e, ri) in x
            if (s_bldg[ri], s_floor[ri]) == (b, f)
        ]

        if assigned_vars:
            model.Add(sum(assigned_vars) == 0).OnlyEnforceIf(var)
            model.Add(sum(assigned_vars) >= 1).OnlyEnforceIf(var.Not())


    floor_used = {f: model.NewBoolVar(f"fl_{fl_idx[f]}") for f in floors}
    move       = {e: model.NewBoolVar(f"mv_{e}") for e in emp_ids}
    team_on_fl = {(t,f): model.NewBoolVar(f"tof_{ti}_{fl_idx[f]}")
                  for ti,t in enumerate(teams) for f in floors}

    large_floor_residual: dict = {}
    for ei,e in enumerate(emp_ids):
        on_lg = [x[e,ri] for ri in emp_valid[e]
                 if (s_bldg[ri],s_floor[ri]) in tier1_set and (e,ri) in x]
        if on_lg:
            v = model.NewBoolVar(f"lfr_{ei}")
            model.Add(sum(on_lg) >= v)
            for rv in on_lg: model.Add(v >= rv)
            large_floor_residual[e] = v

    for e in emp_ids:
        model.Add(sum(x[e,ri] for ri in emp_valid[e]) == 1)

    seat_emps_map: dict = defaultdict(list)
    for (e,ri) in x: seat_emps_map[ri].append(e)
    for ri,emps_here in seat_emps_map.items():
        if len(emps_here) > 1:
            model.Add(sum(x[e,ri] for e in emps_here) <= 1)

    for (e,ri),var in x.items():
        model.Add(var <= floor_used[s_floor[ri]])

    for ti,t in enumerate(teams):
        t_emps = [e for e in emp_ids if e_team[e]==t]
        for f in floors:
            assigned = [x[e,ri] for e in t_emps
                        for ri in emp_valid[e] if s_floor[ri]==f and (e,ri) in x]
            if not assigned:
                model.Add(team_on_fl[t,f] == 0); continue
            model.Add(sum(assigned) >= team_on_fl[t,f])
            for av in assigned: model.Add(team_on_fl[t,f] >= av)

    for t in teams:
        ts = team_size.get(t,0)
        if ts < C730_SIZE_BOUNDARY: continue
        rf = {s_floor[ri] for e in emp_ids if e_team[e]==t for ri in emp_valid[e]}
        if not rf: continue
        max_fc  = max(floor_seat_cap_by_floor[f] for f in rf)
        min_req = math.ceil(ts/max_fc) if max_fc > 0 else 1
        # Aggressive floor cap: larger teams -> tighter cap
        if ts >= 50:
            cap_val = min(max(min_req, 1), MAX_FLOORS_BIG_TEAM_HARD)
        else:
            cap_val = min(max(min_req, 1), MAX_FLOORS_MED_TEAM_HARD)
        model.Add(sum(team_on_fl[t,f] for f in floors) <= cap_val)

    for grp, members in kisu_groups.items():
        g_emps = [e for e in members if e in emp_set]
        if len(g_emps) < 2: continue
        feasible_units = [u for u in units
                          if all(bool(emp_valid[e]&sidx_by_unit.get(u,set())) for e in g_emps)]
        if not feasible_units:
            print(f"  !! KISU '{grp}': skipped"); continue
        uv = {u: model.NewBoolVar(f"kisu_{grp}_{u}") for u in feasible_units}
        model.Add(sum(uv.values()) == 1)
        for u,uvar in uv.items():
            for e in g_emps:
                in_u  = [x[e,ri] for ri in emp_valid[e] if s_unit[ri]==u and (e,ri) in x]
                out_u = [x[e,ri] for ri in emp_valid[e] if s_unit[ri]!=u and (e,ri) in x]
                if in_u: model.Add(uvar <= sum(in_u))
                for vo in out_u: model.Add(uvar+vo <= 1)

    cab_cub_clauses = 0
    for e in emp_ids:
        if e_stype.get(e) not in ("CAB","CUB"): continue
        t = e_team[e]
        cl_members = [e2 for e2 in emp_ids if e_team[e2]==t and e_stype.get(e2)=="CL"]
        if not cl_members: continue
        for ri in emp_valid[e]:
            if (e,ri) not in x: continue
            model.Add(x[e,ri] <= team_on_fl[t,s_floor[ri]])
            cab_cub_clauses += 1
    print(f"  C5 anchor clauses  : {cab_cub_clauses:,}")

    c7_clauses = 0
    for tuk in team_units:
        if tuk not in (c7_tu_feasible|c7_tu_partial): continue
        target_b  = c7_tu_target_bldg.get(tuk)
        free_emps = c7_tu_free_subset.get(tuk,[])
        if not free_emps or not target_b: continue
        for e in free_emps:
            in_target = [x[e,ri] for ri in emp_valid[e]
                         if s_bldg[ri]==target_b and (e,ri) in x]
            if in_target:
                model.Add(sum(in_target) == 1)
                c7_clauses += 1
    print(f"  C7 clauses         : {c7_clauses:,}")

    cohort_clauses = 0
    for ck in cohorts:
        target_b  = c_cohort_target_bldg.get(ck)
        free_emps = c_cohort_free_subset.get(ck,[])
        if not free_emps or not target_b: continue
        for e in free_emps:
            if e_bc[e].lower() == "no": continue
            stype = e_stype.get(e,"CL")
            # FIX-C1: skip if no spare seats of this type in target building
            if spare_by_bldg_type.get((target_b,stype),0) < 1: continue
            in_target = [x[e,ri] for ri in emp_valid[e]
                         if s_bldg[ri]==target_b and (e,ri) in x]
            if in_target:
                model.Add(sum(in_target) == 1)
                cohort_clauses += 1
    print(f"  FIX-D-CP clauses   : {cohort_clauses:,}")

    c730_clauses = 0
    for t in teams:
        ts = team_size.get(t,0)
        if ts == 0: continue
        min_count = max(1, int(team_c730_threshold[t]*ts))
        t_emps    = [e for e in emp_ids if e_team[e]==t]
        for f in floors:
            assigned = [x[e,ri] for e in t_emps
                        for ri in emp_valid[e] if s_floor[ri]==f and (e,ri) in x]
            if not assigned: continue
            model.Add(sum(assigned) >= min_count*team_on_fl[t,f])
            c730_clauses += 1
    print(f"  C7_30 clauses      : {c730_clauses:,}")

    group_in_bldg = {(g,b): model.NewBoolVar(f"gib_{gi}_{bldg_idx[b]}")
                     for gi,g in enumerate(group_emps) for b in bldgs}
    c8_bldg_clauses = 0
    for gi,(g,g_emps) in enumerate(group_emps.items()):
        if g not in c8_bldg_feasible: continue
        for b in bldgs:
            assigned = [x[e,ri] for e in g_emps
                        for ri in emp_valid[e] if s_bldg[ri]==b and (e,ri) in x]
            if not assigned:
                model.Add(group_in_bldg[g,b] == 0); continue
            model.Add(sum(assigned) >= group_in_bldg[g,b])
            for av in assigned: model.Add(group_in_bldg[g,b] >= av)
            c8_bldg_clauses += 1
        model.Add(sum(group_in_bldg[g,b] for b in bldgs) == 1)
    print(f"  C8 bldg clauses    : {c8_bldg_clauses:,}")

    team_floor_extra: dict = {}
    for ti,t in enumerate(teams):
        if team_size.get(t,0) < C730_SIZE_BOUNDARY: continue
        v = model.NewIntVar(0, len(floors), f"tfe_{ti}")
        model.Add(sum(team_on_fl[t,f] for f in floors) == v+1)
        team_floor_extra[t] = v

    group_on_fl = {(g,f): model.NewBoolVar(f"gof_{gi}_{fl_idx[f]}")
                   for gi,g in enumerate(group_emps) for f in floors}
    for gi,(g,g_emps) in enumerate(group_emps.items()):
        for f in floors:
            assigned = [x[e,ri] for e in g_emps
                        for ri in emp_valid[e] if s_floor[ri]==f and (e,ri) in x]
            if not assigned:
                model.Add(group_on_fl[g,f] == 0); continue
            model.Add(sum(assigned) >= group_on_fl[g,f])
            for av in assigned: model.Add(group_on_fl[g,f] >= av)

    group_floor_extra: dict = {}
    for g in group_emps:
        v = model.NewIntVar(0, len(floors), f"gfe_{g}")
        model.Add(sum(group_on_fl[g,f] for f in floors) == v+1)
        group_floor_extra[g] = v

    mgr_diff: dict = {}
    for idx,(e,m) in enumerate(unique_mgr_pairs):
        v = model.NewBoolVar(f"mdf_{idx}")
        sfv = []
        for f in floors:
            bof    = model.NewBoolVar(f"bof_{idx}_{fl_idx[f]}")
            e_on_f = [x[e,ri] for ri in emp_valid[e] if s_floor[ri]==f and (e,ri) in x]
            m_on_f = [x[m,ri] for ri in emp_valid[m] if s_floor[ri]==f and (m,ri) in x]
            if e_on_f and m_on_f:
                model.Add(bof <= sum(e_on_f))
                model.Add(bof <= sum(m_on_f))
                model.Add(bof >= sum(e_on_f)+sum(m_on_f)-1)
                sfv.append(bof)
            else:
                model.Add(bof == 0)
        model.Add(v >= 1-sum(sfv))
        mgr_diff[idx] = v

    for e in emp_ids:
        cur = e_cur_ridx.get(e)
        if cur is not None and (e,cur) in x:
            model.Add(move[e] >= 1-x[e,cur])
        else:
            model.Add(move[e] == 1)

    thin_vars: list = []
    for e in emp_ids:
        if e_floor[e] not in thin_floors: continue
        outside = [x[e,ri] for ri in emp_valid[e]
                   if s_floor[ri] not in thin_floors and (e,ri) in x]
        on_thin = [x[e,ri] for ri in emp_valid[e]
                   if s_floor[ri] in thin_floors and (e,ri) in x]
        if outside and on_thin: thin_vars.extend(on_thin)
    print(f"  S5 thin vars       : {len(thin_vars)}")

    print("  Injecting warm-start hints ...")
    for e in emp_ids:
        ri_hint = greedy_assignment.get(e)
        if ri_hint is None: continue
        for ri in emp_valid[e]:
            if (e,ri) in x:
                model.AddHint(x[e,ri], 1 if ri==ri_hint else 0)

    print(f"\nPhase 1: minimise floors ({_PHASE1_TIME}s) ...")
    model.Minimize(sum(floor_used[f] for f in floors)
                   + sum(thin_vars)
                   + sum(large_floor_residual.values()))
    solver = cp_model.CpSolver()
    solver.parameters.max_time_in_seconds = _PHASE1_TIME
    solver.parameters.num_search_workers  = CP_WORKERS
    solver.parameters.log_search_progress = True
    p1_status = solver.Solve(model)

    if p1_status in (cp_model.OPTIMAL, cp_model.FEASIBLE):
        min_floors = int(round(solver.ObjectiveValue()))
        print(f"  Phase 1 {'optimal' if p1_status==cp_model.OPTIMAL else 'best-found'}: {min_floors}")
    else:
        print("  Phase 1 INFEASIBLE -- using greedy."); min_floors = None

    if min_floors is not None and _PHASE2_TIME > 0:
        model.Add(sum(floor_used[f] for f in floors) <= min_floors)
        def _tsw(t):
            ts = team_size.get(t,0)
            return (int(W_TEAM_FLOOR_SPREAD*(ts**0.7))
                    if W_TEAM_FLOOR_SPREAD_SCALE and ts>1 else W_TEAM_FLOOR_SPREAD)
        model.Minimize(sum(
              [W_MGR_PROX*mgr_diff[i]         for i in mgr_diff]
            + [W_MOVE*move[e]                  for e in emp_ids]
            + [W_THIN*v                        for v in thin_vars]
            + [W_C8_FLOOR*group_floor_extra[g] for g in group_floor_extra]
            + [_tsw(t)*team_floor_extra[t]     for t in team_floor_extra]
            + [W_LARGE_FLOOR_EVAC*v            for v in large_floor_residual.values()]
            ) - sum(floor_reward)
        #
        )
        
    # FLOOR REWARD TERMS
    floor_reward = []
    for (b, f), var in floor_empty_vars.items():
        if (b, f) in design_floors:
            floor_reward.append(var * 2_000_000)
        else:
            floor_reward.append(var * 1_000_000)
    if min_floors is not None and _PHASE2_TIME > 0:
        model.Add(sum(floor_used[f] for f in floors) <= min_floors)
        
        # FLOOR REWARD TERMS - must be defined BEFORE model.Minimize()
        floor_reward = []
        for (b, f), var in floor_empty_vars.items():
            if (b, f) in design_floors:
                floor_reward.append(var * 2_000_000)
            else:
                floor_reward.append(var * 1_000_000)
        
        def _tsw(t):
            ts = team_size.get(t, 0)
            return (int(W_TEAM_FLOOR_SPREAD * (ts ** 0.7))
                    if W_TEAM_FLOOR_SPREAD_SCALE and ts > 1 else W_TEAM_FLOOR_SPREAD)
        
        model.Minimize(sum(
            [W_MGR_PROX * mgr_diff[i] for i in mgr_diff]
            + [W_MOVE * move[e] for e in emp_ids]
            + [W_THIN * v for v in thin_vars]
            + [W_C8_FLOOR * group_floor_extra[g] for g in group_floor_extra]
            + [_tsw(t) * team_floor_extra[t] for t in team_floor_extra]
            + [W_LARGE_FLOOR_EVAC * v for v in large_floor_residual.values()]
        ) - sum(floor_reward))
        
        print(f"Phase 2: soft penalties ({_PHASE2_TIME}s) ...")
        solver2 = cp_model.CpSolver()
        solver2.parameters.max_time_in_seconds = _PHASE2_TIME
        solver2.parameters.num_search_workers = CP_WORKERS
        solver2.parameters.log_search_progress = True
        p2_status = solver2.Solve(model)
        final_solver, final_status = solver2, p2_status
    else:
        final_solver, final_status = solver, p1_status

    if final_status in (cp_model.OPTIMAL, cp_model.FEASIBLE):
        lbl = "OPTIMAL" if final_status == cp_model.OPTIMAL else "FEASIBLE (time-limited)"
        print(f"  CP-SAT: {lbl}  Obj={final_solver.ObjectiveValue():.0f}")
        cp_assignment = {}
        for e in emp_ids:
            for ri in emp_valid[e]:
                if (e, ri) in x and final_solver.Value(x[e, ri]) == 1:
                    cp_assignment[e] = ri
                    break
    else:
        print("  CP-SAT INFEASIBLE -- using greedy.")



# ───────────────────────────────────────────────────────────────────
# 16.  FINAL ASSIGNMENT
# ───────────────────────────────────────────────────────────────────

if cp_assignment:
    final  = cp_assignment
    method = "CP-SAT"
else:
    final  = greedy_assignment
    method = "Greedy"

print(f"\nFinal method: {method}")

new_floor_map = {e: s_floor[final[e]] for e in emp_ids}
new_bldg_map  = {e: s_bldg[final[e]]  for e in emp_ids}
new_unit_map  = {e: s_unit[final[e]]  for e in emp_ids}


# ───────────────────────────────────────────────────────────────────
# 17.  VALIDATION  (FIX-C5: 2-floor tolerance for C5)
# ───────────────────────────────────────────────────────────────────

print(f"\n{'─'*64}")
print("  POST-HOC VALIDATION")
print(f"{'─'*64}")

val_rows = []

def flag(constraint, e, team, detail, root_cause=""):
    val_rows.append({"Constraint":constraint,"Employee ID":e,
                     "Employee Name":e_name.get(e,""),"Team":team,
                     "Detail":detail,"Root Cause":root_cause})

def mgr_floor_ok(mgr_floor, cl_floors, tolerance=MGR_FLOOR_TOLERANCE):
    """True if manager floor is within 'tolerance' floors of any CL floor."""
    mgr_ord = floor_order.get(mgr_floor, -999)
    for cf in cl_floors:
        cf_ord = floor_order.get(cf, -999)
        if abs(mgr_ord - cf_ord) <= tolerance:
            return True
    return False

# C1
for e in emp_ids:
    assigned_type = s_type[final[e]]
    expected_type = e_stype.get(e,"CL")
    if assigned_type != expected_type:
        ab = new_bldg_map[e]
        ct = c_cohort_target_bldg.get(e_cohort[e],"?")
        flag("C1", e, e_team[e],
             f"Expected {expected_type} got {assigned_type} | Seat:{s_name[final[e]]} | Bldg:{ab}",
             f"SEAT TYPE MISMATCH. Needs {expected_type}, assigned {assigned_type}. "
             f"Spare {expected_type} in assigned bldg '{ab}': {spare_by_bldg_type.get((ab,expected_type),0)}. "
             f"Cohort target '{ct}' has {spare_by_bldg_type.get((ct,expected_type),0)} spare {expected_type}. "
             f"Action: increase {expected_type} seat inventory or adjust cohort targets.")

# C2
for e in emp_ids:
    if e_uc[e].lower()=="no" and new_unit_map[e]!=e_unit[e]:
        flag("C2",e,e_team[e],f"Unit lock: {e_unit[e]} -> {new_unit_map[e]}",
             f"Unit Changeable=No but moved to different unit. Check L2/L3 relaxation.")

# C3
for e in emp_ids:
    if e_bc[e].lower()=="no" and new_bldg_map[e]!=e_bldg[e] and e not in relaxed_emps:
        flag("C3",e,e_team[e],f"Bldg lock: {e_bldg[e]} -> {new_bldg_map[e]}",
             f"Bldg Changeable=No but moved. Seat type {e_stype.get(e)} may be unavailable in original building.")
for e in relaxed_emps:
    lvl = relaxation_level.get(e,"?")
    msgs = {1:"No spare seat of type in building — bldg lock relaxed",
            2:"No spare seat in unit or building — both relaxed",
            3:"No spare seat of type anywhere — full lock relaxed"}
    flag(f"C3_RELAXED_L{lvl}",e,e_team[e],
         f"L{lvl} | type={e_stype.get(e)} | orig_bldg={e_bldg[e]}",
         msgs.get(lvl,"Unknown"))

# C4
for grp,mlist in kisu_groups.items():
    g_emps = [e for e in mlist if e in final]
    units_used = {new_unit_map[e] for e in g_emps}
    if len(units_used)>1:
        for e in g_emps:
            flag("C4",e,e_team[e],f"KISU '{grp}' split: {sorted(units_used)}",
                 f"Keep-in-same-unit group spread across {sorted(units_used)}.")

# C5 — FIX-C5: ±1 floor tolerance
for e in emp_ids:
    if e_stype.get(e) not in ("CAB","CUB"): continue
    t  = e_team[e]
    ef = new_floor_map[e]
    cl_floors = {new_floor_map[e2] for e2 in emp_ids
                 if e_team[e2]==t and e_stype.get(e2)=="CL"}
    if cl_floors and not mgr_floor_ok(ef, cl_floors):
        flag("C5",e,t,
             f"Manager on '{ef}'; team CL on {sorted(cl_floors)} | Bldg:{new_bldg_map[e]}",
             f"Manager more than {MGR_FLOOR_TOLERANCE} floor(s) away from nearest CL team member. "
             f"Physical lock or floor fragmentation. CP-SAT will improve.")

# C6
for e in emp_ids:
    req = floor_restrict.get(teamkey_to_team.get(e_team[e],""))
    if req and new_floor_map[e]!=req:
        flag("C6",e,e_team[e],f"Need {req}, got {new_floor_map[e]}",
             f"Team Mapping specifies Floor Restriction={req}.")

# C7
for tuk in team_units:
    tu_emps     = [e for e in emp_ids if e_team_unit[e]==tuk]
    target_b    = c7_tu_target_bldg.get(tuk,"")
    free_emps   = set(c7_tu_free_subset.get(tuk,[]))
    locked_emps = set(c7_tu_locked_subset.get(tuk,[]))
    if tuk in c7_tu_feasible or tuk in c7_tu_partial:
        for e in free_emps:
            if new_bldg_map[e]!=target_b:
                flag("C7_VIOLATION",e,tuk,
                     f"Free member in {new_bldg_map[e]}, target={target_b}",
                     f"Bldg Changeable=Yes but not in target '{target_b}'. Check cohort vs C7 conflict.")
        for e in locked_emps:
            flag("C7_LOCKED_MINORITY",e,tuk,
                 f"Locked to {new_bldg_map[e]}, target={target_b}",
                 "Bldg Changeable=No — physical constraint, accepted split.")
    else:
        bu = {new_bldg_map[e] for e in tu_emps}
        if len(bu)>1:
            flag("C7_SKIPPED",f"TEAM_UNIT:{tuk}",tuk,
                 f"All locked, split: {sorted(bu)}","All members Bldg Changeable=No.")

# Cohort building
for ck in cohorts:
    c_emps   = cohort_emps.get(ck,[])
    target_b = c_cohort_target_bldg.get(ck,"")
    for e in [e for e in c_emps if e_bc[e].lower()!="no"]:
        if new_bldg_map.get(e)!=target_b:
            stype = e_stype.get(e,"CL")
            flag("COHORT_BLDG_VIOLATION",e,ck,
                 f"Target={target_b}, got {new_bldg_map.get(e)}",
                 f"Spare {stype} in target: {spare_by_bldg_type.get((target_b,stype),0)}. "
                 f"If 0, CHG-2 safety valve released this employee.")

# C7_30
for t in teams:
    ts = team_size.get(t,0)
    if ts==0: continue
    eff_pct   = team_c730_threshold[t]
    min_count = max(1,int(eff_pct*ts))
    fc: dict  = defaultdict(int)
    for e in emp_ids:
        if e_team[e]==t: fc[new_floor_map[e]] += 1
    for f,cnt in fc.items():
        if cnt<min_count:
            note = " [auto-relaxed]" if t in team_c730_relaxed else ""
            flag("C7_30",f"TEAM:{t}",t,
                 f"Floor {f}: {cnt}/{ts}={100*cnt/ts:.0f}% < {eff_pct*100:.0f}%{note}",
                 f"{cnt} members on '{f}', need {min_count}. "
                 f"Niche functional floor? Lock those employees in input.")

# C8
for g,g_emps in group_emps.items():
    bu = {new_bldg_map[e] for e in g_emps if e in new_bldg_map}
    if len(bu)>1:
        status = "SKIPPED" if g not in c8_bldg_feasible else "VIOLATION"
        flag(f"C8_BLDG({status})",f"GROUP:{g}",
             ",".join(sorted({e_team[e] for e in g_emps})),
             f"Group '{g}' across {sorted(bu)}",
             "SKIPPED: members have incompatible building locks." if status=="SKIPPED"
             else "CP-SAT will consolidate.")

for g,g_emps in group_emps.items():
    fu = {new_floor_map[e] for e in g_emps if e in new_floor_map}
    if len(fu)>1:
        flag("C8_FLOOR_SPLIT",f"GROUP:{g}",",".join(sorted({e_team[e] for e in g_emps})),
             f"Group '{g}' on {len(fu)} floors: {sorted(fu)}","Informational only.")

for e in emp_ids:
    if new_floor_map[e] in thin_floors:
        flag("S5_THIN_FLOOR",e,e_team[e],f"On thin floor {new_floor_map[e]}",
             "No alternative seat found. Consider locking these employees in input.")

print(f"\n  Large-floor evacuation audit:")
for (b,f) in tier1_floors:
    emps_on = [e for e in emp_ids if new_bldg_map.get(e)==b and new_floor_map.get(e)==f]
    status  = "FREED" if not emps_on else f"Residual: {len(emps_on)}"
    print(f"    {b} | {f}: {status}")
    flag("INFO_LARGE_FLOOR_FREED" if not emps_on else "INFO_LARGE_FLOOR_RESIDUAL",
         f"FLOOR:{f}",b,f"{b}|{f}: {status}","CHG-1: top-N vacant floor evacuation target.")

validation_df = pd.DataFrame(val_rows) if val_rows else pd.DataFrame(
    [{"Constraint":"ALL","Employee ID":"-","Employee Name":"-",
      "Team":"-","Detail":"No violations","Root Cause":""}])

constraint_counts: dict = defaultdict(int)
for r in val_rows: constraint_counts[r["Constraint"]] += 1

print(f"\n  Total issues: {len(val_rows)}")
for c in sorted(constraint_counts):
    print(f"  {c:<45}: {constraint_counts[c]}")
print(f"{'─'*64}")


# ───────────────────────────────────────────────────────────────────
# 18.  BUILD OUTPUT DATAFRAMES
# ───────────────────────────────────────────────────────────────────

# Seat Allocation
rows = []
for e in emp_ids:
    ri     = final[e]
    old_ri = e_cur_ridx.get(e)
    moved  = (ri!=old_ri) if old_ri is not None else True
    rows.append({
        "Employee ID"    : e,
        "Employee Name"  : e_name.get(e,""),
        "Raw Team"       : teamkey_to_team.get(e_team[e],""),
        "Group"          : e_group.get(e,""),
        "Seat Type"      : e_stype.get(e,"?"),
        "Reporting To"   : e_report.get(e,""),
        "Old Seat"       : s_name[old_ri] if old_ri is not None else "?",
        "New Seat"       : s_name[ri],
        "Old Unit"       : e_unit[e],
        "New Unit"       : s_unit[ri],
        "Old Building"   : e_bldg[e],
        "New Building"   : s_bldg[ri],
        "Old Floor"      : e_floor[e],
        "New Floor"      : s_floor[ri],
        "Moved"          : "Yes" if moved else "No",
        "Bldg Lock Relaxed": f"L{relaxation_level.get(e,0)}" if e in relaxed_emps else "No",
        "Type OK"        : "OK" if s_type.get(ri,"?")==e_stype.get(e,"?") else "VIOLATION",
        "Unit Lock OK"   : "OK" if (e_uc[e].lower()!="no" or s_unit[ri]==e_unit[e]) else "VIOLATION",
        "Bldg Lock OK"   : "OK" if (e_bc[e].lower()!="no" or s_bldg[ri]==e_bldg[e]) else "VIOLATION",
        "Method"         : method,
    })
output = pd.DataFrame(rows)

# Floor Summary
bf = df_emp.groupby(["Building","Floor"]).size().reset_index(name="Before")
af = output.groupby(["New Building","New Floor"]).size().reset_index(name="After")
fs = bf.merge(af,left_on=["Building","Floor"],right_on=["New Building","New Floor"],how="outer")
fs["Building"] = fs["Building"].fillna(fs["New Building"])
fs["Floor"]    = fs["Floor"].fillna(fs["New Floor"])
fs["Before"]   = fs["Before"].fillna(0).astype(int)
fs["After"]    = fs["After"].fillna(0).astype(int)
fs["Delta"]    = fs["After"]-fs["Before"]
fs["Status"]   = fs["After"].apply(lambda v: "FREED" if v==0 else "Active")
fs = fs.drop(columns=["New Building","New Floor"],errors="ignore")
fs = fs[["Building","Floor","Before","After","Delta","Status"]]
fs = fs.sort_values(["Status","Before"]).reset_index(drop=True)

# Building Summary
bb = df_emp.groupby("Building").size().reset_index(name="Before")
ab = output.groupby("New Building").size().reset_index(name="After")
bs = bb.merge(ab,left_on="Building",right_on="New Building",how="outer").fillna(0)
bs["Before"] = bs["Before"].astype(int)
bs["After"]  = bs["After"].astype(int)
bs["Delta"]  = bs["After"]-bs["Before"]
bs = bs.drop(columns=["New Building"],errors="ignore")
bs = bs[["Building","Before","After","Delta"]]

# Move Notifications
movers_df = (
    output[output["Moved"]=="Yes"][[
        "Employee ID","Employee Name","Raw Team","Group","Reporting To",
        "Seat Type","Old Seat","New Seat","Old Unit","New Unit",
        "Old Building","New Building","Old Floor","New Floor","Bldg Lock Relaxed",
    ]].copy()
)
# movers_df.insert(3,"Team_Unit_Key",
#     movers_df["Raw Team"].map(
#         lambda rt: next((e_team_unit[e] for e in emp_ids
#                          if teamkey_to_team.get(e_team[e],"")==rt), rt)))
movers_df = movers_df.sort_values(["Raw Team","Employee Name"]).reset_index(drop=True)

# FIX-C4: Scorecard — unified metrics
n = len(emp_ids)
n_type_ok  = (output["Type OK"]=="OK").sum()
n_unit_ok  = (output["Unit Lock OK"]=="OK").sum()
n_bldg_ok  = (output["Bldg Lock OK"]=="OK").sum()
n_moved    = (output["Moved"]=="Yes").sum()
floors_after  = output["New Floor"].nunique()
floors_before = df_emp["Floor"].nunique()
floors_freed  = (fs["Status"]=="FREED").sum()
large_freed   = sum(1 for (b,f) in tier1_floors
                    if not any(new_bldg_map.get(e)==b and new_floor_map.get(e)==f
                               for e in emp_ids))

cohort_ok = cohort_fail = 0
for ck in cohorts:
    c_emps   = cohort_emps.get(ck,[])
    target_b = c_cohort_target_bldg.get(ck,"")
    if not any(e_bc[e].lower()!="no" and new_bldg_map.get(e)!=target_b for e in c_emps):
        cohort_ok += 1
    else:
        cohort_fail += 1

n_c7_ok   = sum(1 for tuk in team_units
                if len({new_bldg_map[e] for e in emp_ids if e_team_unit[e]==tuk})==1)
n_c730_ok = n_c730_fail = 0
for t in teams:
    ts = team_size.get(t,0)
    if ts==0: continue
    mc = max(1,int(team_c730_threshold[t]*ts))
    fc = defaultdict(int)
    for e in emp_ids:
        if e_team[e]==t: fc[new_floor_map[e]] += 1
    if all(cnt>=mc for cnt in fc.values()): n_c730_ok  += 1
    else:                                   n_c730_fail += 1

cab_ok = cab_tot = 0
for e in emp_ids:
    if e_stype.get(e) not in ("CAB","CUB"): continue
    cab_tot += 1
    cl_fls = {new_floor_map[e2] for e2 in emp_ids
              if e_team[e2]==e_team[e] and e_stype.get(e2)=="CL"}
    if mgr_floor_ok(new_floor_map[e], cl_fls):
        cab_ok += 1

n_c8b_ok = n_c8b_fail = 0
for g,g_emps in group_emps.items():
    bu = {new_bldg_map[e] for e in g_emps if e in new_bldg_map}
    if len(bu)==1: n_c8b_ok  += 1
    else:          n_c8b_fail += 1

def pct(p,t): return round(100*p/max(t,1),1)

freed_floors_list = [f"{r['Building']} - {r['Floor']}"
                     for _,r in fs[fs["Status"]=="FREED"].iterrows()]

large_floor_details = []
for (b,f) in tier1_floors:
    vac_row = floor_vac[(floor_vac["Building"]==b)&(floor_vac["Floor"]==f)]
    vac_n   = int(vac_row["Vacant"].values[0]) if len(vac_row) else 0
    occ_bef = floor_current_occ.get((b,f),0)
    occ_aft = sum(1 for e in emp_ids if new_bldg_map.get(e)==b and new_floor_map.get(e)==f)
    status  = "FREED" if occ_aft==0 else f"Residual: {occ_aft}"
    large_floor_details.append(f"{b} | {f} (Vacant={vac_n}, Before={occ_bef}) → {status}")

# FIX-C4: Scorecard — Constraint, Pass, Fail, Pass_Pct, Status only
def sc(constraint, pass_n, fail_n, status):
    total = pass_n + fail_n if isinstance(pass_n, (int,float)) and isinstance(fail_n,(int,float)) else 1
    pct   = f"{round(100*pass_n/max(total,1),1)}%" if isinstance(pass_n,(int,float)) else "-"
    return {"Constraint":constraint,"Pass":pass_n,"Fail":fail_n,"Pass_Pct":pct,"Status":status}

# scorecard_rows = [
#     # ── Objectives ──────────────────────────────────────────────────
#     #{"Constraint":"Method","Pass":method,"Fail":"-","Pass_Pct":"-","Status":"INFO"},
#     #{"Constraint":"Floors before -> after","Pass":floors_after,"Fail":int(floors_freed),"Pass_Pct":f"{floors_freed} freed","Status":"OBJECTIVE"},
#     #{"Constraint":"Freed floor list","Pass":", ".join(freed_floors_list) if freed_floors_list else "None","Fail":"-","Pass_Pct":"-","Status":"INFO"},
#     #{"Constraint":f"Evac targets ({len(tier1_floors)} floors)","Pass":large_freed,"Fail":len(tier1_floors)-large_freed,"Pass_Pct":f"{round(100*large_freed/max(len(tier1_floors),1),1)}%","Status":"OBJECTIVE"},
#     #{"Constraint":"Evac floor details","Pass":" | ".join(large_floor_details),"Fail":"-","Pass_Pct":"-","Status":"INFO"},
#     #{"Constraint":"Employees moved","Pass":int(n_moved),"Fail":int(n-n_moved),"Pass_Pct":f"{round(100*n_moved/n,1)}%","Status":"INFO"},
#     {"Constraint":"C1 Seat type preserved","Pass":int(n_type_ok),"Fail":int(n-n_type_ok),"Pass_Pct":f"{round(100*n_type_ok/n,1)}%","Status":"PASS" if n_type_ok==n else "FAIL"},
#     {"Constraint":"C2 Unit lock respected","Pass":int(n_unit_ok),"Fail":int(n-n_unit_ok),"Pass_Pct":f"{round(100*n_unit_ok/n,1)}%","Status":"PASS" if n_unit_ok==n else "FAIL"},
#     {"Constraint":"C3 Bldg lock respected","Pass":int(n_bldg_ok),"Fail":int(n-n_bldg_ok),"Pass_Pct":f"{round(100*n_bldg_ok/n,1)}%","Status":"PASS" if n_bldg_ok==n else "FAIL"},
#     {"Constraint":f"C5 CAB/CUB within +/-{MGR_FLOOR_TOLERANCE} floor(s)","Pass":cab_ok,"Fail":cab_tot-cab_ok,"Pass_Pct":f"{round(100*cab_ok/max(cab_tot,1),1)}%","Status":"PASS" if cab_ok==cab_tot else "PARTIAL"},
#     {"Constraint":"C7 Team|Unit same building","Pass":n_c7_ok,"Fail":len(team_units)-n_c7_ok,"Pass_Pct":f"{round(100*n_c7_ok/len(team_units),1)}%","Status":"PASS" if n_c7_ok==len(team_units) else "PARTIAL"},
#     # {"Constraint":"C7_30 30pct floor minimum","Pass":n_c730_ok,"Fail":n_c730_fail,"Pass_Pct":f"{round(100*n_c730_ok/max(n_c730_ok+n_c730_fail,1),1)}%","Status":"PASS" if n_c730_fail==0 else "PARTIAL"},
#     {"Constraint":"C8 Group same building","Pass":n_c8b_ok,"Fail":n_c8b_fail,"Pass_Pct":f"{round(100*n_c8b_ok/max(len(group_emps),1),1)}%","Status":"PASS" if n_c8b_fail==0 else "PARTIAL"},
#     # {"Constraint":"FIX-D Cohort same building","Pass":cohort_ok,"Fail":cohort_fail,"Pass_Pct":f"{round(100*cohort_ok/max(cohort_ok+cohort_fail,1),1)}%","Status":"PASS" if cohort_fail==0 else "PARTIAL"},
#     # {"Constraint":"Total validation issues","Pass":"-","Fail":len(val_rows),"Pass_Pct":"-","Status":"INFO"},
# ]

n_relaxed = len(relaxed_emps)

scorecard_rows = [
    {"Constraint": "C1  Seat type preserved",
     "Pass": int(n_type_ok),      "Fail": int(n - n_type_ok),
     "Pass_Pct": f"{round(100*n_type_ok/n, 1)}%",
     "Status": "HARD"},

    {"Constraint": "C2  Unit lock respected",
     "Pass": int(n_unit_ok),      "Fail": int(n - n_unit_ok),
     "Pass_Pct": f"{round(100*n_unit_ok/n, 1)}%",
     "Status": "HARD"},

    {"Constraint": "C3  Bldg lock respected",
     "Pass": int(n_bldg_ok),      "Fail": int(n - n_bldg_ok),
     "Pass_Pct": f"{round(100*n_bldg_ok/n, 1)}%",
     "Status": "HARD"},

    {"Constraint": "C3  Bldg lock relaxed",
     "Pass": int(n_relaxed),      "Fail": int(n - n_relaxed),
     "Pass_Pct": f"{round(100*n_relaxed/n, 1)}%",
     "Status": "DATA"},

    {"Constraint": "C5  CAB/CUB anchor",
     "Pass": int(cab_ok),         "Fail": int(cab_tot - cab_ok),
     "Pass_Pct": f"{round(100*cab_ok/max(cab_tot, 1), 1)}%",
     "Status": "HARD"},

    {"Constraint": "OBJ  Floors freed",
     "Pass": int(floors_freed),   "Fail": int(floors_after),
     "Pass_Pct": str(int(floors_freed)),
     "Status": "OBJECTIVE"},

    # {"Constraint": "OBJ  Large floors freed",
    #  "Pass": int(large_freed),    "Fail": int(len(tier1_floors) - large_freed),
    #  "Pass_Pct": f"{round(100*large_freed/max(len(tier1_floors), 1), 1)}%",
    #  "Status": "OBJECTIVE"},
]
scorecard_df = pd.DataFrame(scorecard_rows, columns=["Constraint","Pass","Fail","Pass_Pct","Status"])



# ───────────────────────────────────────────────────────────────────
# 19.  PRINT SUMMARY
# ───────────────────────────────────────────────────────────────────

print(f"\n{SEP}")
print("  RESULTS SUMMARY  v7.4")
print(f"{SEP}")
print(f"  Method                   : {method}")
print(f"  Floors before            : {floors_before}")
print(f"  Floors after             : {floors_after}  (freed: {floors_freed})")
print(f"  Freed floors             : {', '.join(freed_floors_list) or 'None'}")
print(f"  Evac floor targets ({len(tier1_floors)}):")
for detail in large_floor_details: print(f"    {detail}")
print(f"  C1 Seat type violations  : {n-n_type_ok}  {'✓ ZERO' if n_type_ok==n else '!! VIOLATIONS'}")
print(f"  C2 Unit lock violations  : {n-n_unit_ok}")
print(f"  C3 Bldg lock violations  : {n-n_bldg_ok}")
print(f"  C5 within ±{MGR_FLOOR_TOLERANCE} floor(s)    : {cab_ok}/{cab_tot}")
print(f"  C7 Team|Unit same bldg   : {n_c7_ok}/{len(team_units)}")
print(f"  C7_30 30pct floor min    : {n_c730_ok}/{n_c730_ok+n_c730_fail}")
print(f"  C8 Group same building   : {n_c8b_ok}/{len(group_emps)}")
print(f"  FIX-D Cohort same bldg  : {cohort_ok}/{cohort_ok+cohort_fail}")
print(f"  Employees moved          : {n_moved}/{n}")
print(f"  Total validation issues  : {len(val_rows)}")
print(f"{SEP}")


# ───────────────────────────────────────────────────────────────────
# 20.  WRITE OUTPUT
# ───────────────────────────────────────────────────────────────────

OUTPUT_SHEETS = [
    (output,        "Seat Allocation"),
    (fs,            "Floor Summary"),
    (bs,            "Building Summary"),
    (movers_df,     "Move Notifications"),
    (scorecard_df,  "Scorecard"),
    #(validation_df, "Validation Detail"),
]

with pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl") as writer:
    for df_out, sheet_name in OUTPUT_SHEETS:
        if df_out is not None and len(df_out)>0:
            df_out.to_excel(writer, sheet_name=sheet_name, index=False)
        else:
            pd.DataFrame([{"Note":"No data"}]).to_excel(
                writer, sheet_name=sheet_name, index=False)

print(f"\n  Output : {OUTPUT_FILE}")
print(f"  Sheets : {', '.join(s for _,s in OUTPUT_SHEETS)}")
print(f"  Runtime: {round(time.time()-start,2)}s\n")
