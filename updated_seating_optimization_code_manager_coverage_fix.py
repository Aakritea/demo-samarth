# UPDATED VERSION WITH MANAGER COVERAGE LOGIC
# Pairwise manager proximity REMOVED

from ortools.sat.python import cp_model
from collections import defaultdict

model = cp_model.CpModel()

# -------------------------------
# EXISTING VARIABLES (ASSUMED)
# -------------------------------
# emp_ids, emp_valid, s_floor, floors
# e_report, name_to_emp, e_team

# -------------------------------
# DECISION VARIABLES
# -------------------------------

x = {}
for e in emp_ids:
    for ri in emp_valid[e]:
        x[(e, ri)] = model.NewBoolVar(f"x_{e}_{ri}")

# each employee gets exactly one seat
for e in emp_ids:
    model.Add(sum(x[(e, ri)] for ri in emp_valid[e]) == 1)

# -------------------------------
# TEAM 20% CLUSTERING
# -------------------------------

team_on_fl = {}
teams = list(set(e_team.values()))

for t in teams:
    members = [e for e in emp_ids if e_team[e] == t]
    min_req = max(1, int(0.2 * len(members)))

    for f in floors:
        v = model.NewBoolVar(f"team_{t}_floor_{f}")
        team_on_fl[(t, f)] = v

        assigned = []
        for e in members:
            assigned += [
                x[(e, ri)] for ri in emp_valid[e]
                if s_floor[ri] == f
            ]

        if assigned:
            model.Add(sum(assigned) >= min_req).OnlyEnforceIf(v)
            model.Add(sum(assigned) <= min_req - 1).OnlyEnforceIf(v.Not())
        else:
            model.Add(v == 0)

# -------------------------------
# BUILD MANAGER HIERARCHY
# -------------------------------

manager_to_all_reports = defaultdict(set)

# direct reports
for e in emp_ids:
    mgr = name_to_emp.get(e_report[e])
    if mgr and mgr in emp_ids and mgr != e:
        manager_to_all_reports[mgr].add(e)

# propagate hierarchy
changed = True
while changed:
    changed = False
    for m in list(manager_to_all_reports.keys()):
        current = set(manager_to_all_reports[m])
        for r in current:
            if r in manager_to_all_reports:
                before = len(manager_to_all_reports[m])
                manager_to_all_reports[m] |= manager_to_all_reports[r]
                if len(manager_to_all_reports[m]) > before:
                    changed = True

# -------------------------------
# MANAGER 20% COVERAGE LOGIC
# -------------------------------

mgr_bad = {}

for m, reports in manager_to_all_reports.items():

    total = len(reports)
    if total == 0:
        continue

    min_req = max(1, int(0.2 * total))

    valid_floor_vars = []

    for f in floors:

        cov = model.NewBoolVar(f"mgr_cov_{m}_{f}")

        assigned = []
        for e in reports:
            assigned += [
                x[(e, ri)] for ri in emp_valid[e]
                if s_floor[ri] == f and (e, ri) in x
            ]

        if assigned:
            model.Add(sum(assigned) >= min_req).OnlyEnforceIf(cov)
            model.Add(sum(assigned) <= min_req - 1).OnlyEnforceIf(cov.Not())
        else:
            model.Add(cov == 0)

        mgr_on_f = [
            x[(m, ri)] for ri in emp_valid[m]
            if s_floor[ri] == f and (m, ri) in x
        ]

        valid = model.NewBoolVar(f"mgr_valid_{m}_{f}")

        if mgr_on_f:
            model.Add(valid <= sum(mgr_on_f))
            model.Add(valid <= cov)
            model.Add(valid >= sum(mgr_on_f) + cov - 1)
        else:
            model.Add(valid == 0)

        valid_floor_vars.append(valid)

    bad = model.NewBoolVar(f"mgr_bad_{m}")
    mgr_bad[m] = bad

    if valid_floor_vars:
        model.Add(sum(valid_floor_vars) >= 1).OnlyEnforceIf(bad.Not())
        model.Add(sum(valid_floor_vars) == 0).OnlyEnforceIf(bad)
    else:
        model.Add(bad == 1)

# -------------------------------
# FLOOR USAGE
# -------------------------------

floor_used = {}

for f in floors:
    v = model.NewBoolVar(f"floor_used_{f}")
    floor_used[f] = v

    assigned = []
    for e in emp_ids:
        assigned += [
            x[(e, ri)] for ri in emp_valid[e]
            if s_floor[ri] == f
        ]

    if assigned:
        model.Add(sum(assigned) >= 1).OnlyEnforceIf(v)
        model.Add(sum(assigned) == 0).OnlyEnforceIf(v.Not())
    else:
        model.Add(v == 0)

# -------------------------------
# OBJECTIVE
# -------------------------------

W_FLOOR = 100000
W_MGR = 15000

model.Minimize(
    sum(W_FLOOR * floor_used[f] for f in floors) +
    sum(W_MGR * mgr_bad[m] for m in mgr_bad)
)

# -------------------------------
# SOLVE
# -------------------------------

solver = cp_model.CpSolver()
solver.parameters.max_time_in_seconds = 120
solver.parameters.num_search_workers = 8

result = solver.Solve(model)

# -------------------------------
# OUTPUT
# -------------------------------

if result in (cp_model.OPTIMAL, cp_model.FEASIBLE):
    print("\nSolution Found\n")

    for e in emp_ids:
        for ri in emp_valid[e]:
            if solver.Value(x[(e, ri)]) == 1:
                print(f"Employee {e} -> Seat {ri}, Floor {s_floor[ri]}")

    print("\nFloors Used:")
    for f in floors:
        if solver.Value(floor_used[f]):
            print(f"Floor {f}")

    print("\nManager Violations:")
    for m in mgr_bad:
        if solver.Value(mgr_bad[m]) == 1:
            print(f"Manager {m} not aligned with 20% of org")

else:
    print("No solution found")