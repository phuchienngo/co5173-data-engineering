"""Checks that two BR5 analysis runs gave the same answer.

Used to verify the engines against each other (C13), determinism and the
multiplier (plan-pipeline.md section 10). Figures are rounded to 2 decimals:
floating-point sums can differ in the last digits between engines.

    python3 compare_runs.py RUN_ID_A RUN_ID_B      (inside br5-app)
"""

import sys

import runs

SKIP = {"run_id"}


def normalized(rows):
    out = []
    for row in rows:
        out.append({k: round(v, 2) if isinstance(v, float) else v for k, v in row.items() if k not in SKIP})
    return out


def settings_diff(a, b):
    """[(key, value in a, value in b)] for the snapshot keys that differ
    (None = missing). Settings first, then Spark's effective values, the
    cluster and where the settings came from."""
    order = {"effective": 1, "cluster": 2, "launch": 3}
    keys = sorted(set(a) | set(b), key=lambda k: (order.get(k.split(".", 1)[0], 0), k))
    return [(k, a.get(k), b.get(k)) for k in keys if a.get(k) != b.get(k)]


def differences(client, run_a, run_b):
    months_a, cards_a = runs.result_rows(client, run_a)
    months_b, cards_b = runs.result_rows(client, run_b)
    problems = []
    for name, a, b in (("card", cards_a, cards_b), ("month", months_a, months_b)):
        a, b = normalized(a), normalized(b)
        if len(a) != len(b):
            problems.append(f"{len(a)} vs {len(b)} {name} rows")
            continue
        for row_a, row_b in zip(a, b):
            for key in row_a:
                if row_a[key] != row_b[key]:
                    where = f"rank {row_a['target_rank']}" + (f" month {row_a['month']}" if name == "month" else "")
                    problems.append(f"{name} {where} {key}: {row_a[key]!r} vs {row_b[key]!r}")
    return problems, len(cards_a), len(months_a)


def main():
    if len(sys.argv) != 3:
        sys.exit(__doc__)
    from clickhouse_io import get_client  # here, so the app and tests import this without it

    problems, cards, months = differences(get_client(), sys.argv[1], sys.argv[2])
    if problems:
        print("\n".join(problems[:50]))
        sys.exit(f"Different: {len(problems)} differences")
    print(f"Same result: {cards} cards and {months} month rows match")


if __name__ == "__main__":
    main()
