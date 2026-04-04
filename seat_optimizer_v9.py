# seat_optimizer_v9.py
# Enhanced objective: maximize capacity freed per floor (favor large floor evacuation)
# All original constraints preserved

from collections import defaultdict

TOP_K_FLOORS = 3
W_TEAM_FLOOR_SPREAD = 300_000
W_NEW_FLOOR = 50_000
W_PARTIAL_LARGE = 200_000

LARGE_FLOOR_THRESHOLD = 100
EVAC_MIN_FREE_RATIO = 0.2
EVAC_MAX_FLOORS = 10

def restrict_emp_valid(emp_valid, s_floor):
    for e in emp_valid:
        valid = emp_valid[e]
        floor_counts = defaultdict(int)
        for ri in valid:
            floor_counts[s_floor[ri]] += 1

        top_floors = set(sorted(floor_counts, key=floor_counts.get, reverse=True)[:TOP_K_FLOORS])
        filtered = {ri for ri in valid if s_floor[ri] in top_floors}

        if filtered:
            emp_valid[e] = filtered
    return emp_valid

def greedy_assign(emp_ids, emp_valid, s_floor, s_bldg, floor_capacity):
    available = set([ri for v in emp_valid.values() for ri in v])
    assignment = {}
    floor_usage_count = defaultdict(int)

    def score(e, ri):
        sc = 0
        sc += 2500 * floor_usage_count[s_floor[ri]]

        cap = floor_capacity[(s_bldg[ri], s_floor[ri])]
        if cap > LARGE_FLOOR_THRESHOLD and floor_usage_count[s_floor[ri]] < 10:
            sc -= 5000

        return sc

    for e in emp_ids:
        candidates = [ri for ri in emp_valid[e] if ri in available]
        if not candidates:
            continue

        best = max(candidates, key=lambda ri: score(e, ri))
        assignment[e] = best
        available.remove(best)
        floor_usage_count[s_floor[best]] += 1

    return assignment, floor_usage_count

def compute_evacuation_candidates(floor_stats):
    floor_stats["TotalCapacity"] = floor_stats["Occupied"] + floor_stats["Vacant"]

    floor_stats["EvacScore"] = (
        floor_stats["TotalCapacity"] * 0.7 +
        floor_stats["Vacant"] * 0.3
    )

    evac_candidates = floor_stats[
        (floor_stats["Occupied"] > 0) &
        (floor_stats["FreeRatio"] >= EVAC_MIN_FREE_RATIO)
    ].sort_values("EvacScore", ascending=False).head(EVAC_MAX_FLOORS)

    return evac_candidates

def can_evacuate_floor(b, f, emp_ids, emp_valid, s_bldg, s_floor):
    emps = [e for e in emp_ids if any((s_bldg[ri], s_floor[ri]) == (b, f) for ri in emp_valid[e])]

    if not emps:
        return False

    movable = 0
    for e in emps:
        alt = [ri for ri in emp_valid[e] if (s_bldg[ri], s_floor[ri]) != (b, f)]
        if alt:
            movable += 1

    return movable / len(emps) >= 0.8

def optimize(emp_ids, emp_valid, s_floor, s_bldg, floor_capacity, floor_stats):
    emp_valid = restrict_emp_valid(emp_valid, s_floor)

    greedy_assignment, floor_usage = greedy_assign(
        emp_ids, emp_valid, s_floor, s_bldg, floor_capacity
    )

    evac_candidates = compute_evacuation_candidates(floor_stats)

    return {
        "emp_valid": emp_valid,
        "greedy_assignment": greedy_assignment,
        "evac_candidates": evac_candidates
    }
