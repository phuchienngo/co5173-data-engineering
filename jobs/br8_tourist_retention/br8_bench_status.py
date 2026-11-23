"""What the BR8 benchmark has stored: the imported data and the latest runs."""

import br8_bench_runs as runs
from clickhouse_io import get_client


def main():
    client = get_client(autogenerate_session_id=False)
    if not runs.table_exists(client, runs.DB, "raw_checkin"):
        print("BR8 benchmark tables are missing: run `make br8-up`.")
        return
    counts = {t: runs.row_count(client, runs.DB, t) for t in runs.DATA_TABLES}
    if not counts["raw_checkin"]:
        print("No BR8 data imported yet: `make br8-import`.")
    else:
        print(f"Imported: {counts['raw_checkin']:,} check-ins, {counts['place']:,} places, {counts['region']:,} regions")
    rows = client.query(
        f"SELECT run_type, status, region, settings_from, engine, duration_s, visits, places, fingerprint, run_id "
        f"FROM {runs.DB}.run_history ORDER BY started_at DESC LIMIT 10").named_results()
    print("Latest runs:")
    for r in rows:
        print(f"  {r['run_type']:6} {r['status']:9} {r['region']:8} {r['settings_from']:30} {r['engine']:10} "
              f"{(r['duration_s'] or 0):7.1f}s  {r['visits']:>8,} visits  {r['places']:>6,} places  {r['run_id']}")


if __name__ == "__main__":
    main()
