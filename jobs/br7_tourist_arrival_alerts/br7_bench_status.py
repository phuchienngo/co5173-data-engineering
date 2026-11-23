"""What the BR7 benchmark has stored: the imported data and the latest runs."""

import br7_bench_runs as runs
from clickhouse_io import get_client


def main():
    client = get_client(autogenerate_session_id=False)
    if not runs.table_exists(client, runs.DB, "raw_trip"):
        print("BR7 benchmark tables are missing: run `make br7-up`.")
        return
    trips = runs.row_count(client, runs.DB, "raw_trip")
    if not trips:
        print("No BR7 data imported yet: `make br7-import`.")
    else:
        months = client.command(f"SELECT uniqExact(source_month) FROM {runs.DB}.raw_trip")
        print(f"Imported: {trips:,} trips in {months} months, {runs.row_count(client, runs.DB, 'zone')} zones")
    rows = client.query(
        f"SELECT run_type, status, input_name, settings_from, engine, duration_s, baseline_cells, replay_events, "
        f"run_id FROM {runs.DB}.run_history ORDER BY started_at DESC LIMIT 10").named_results()
    print("Latest runs:")
    for r in rows:
        print(f"  {r['run_type']:6} {r['status']:9} {r['input_name']:22} {r['settings_from']:30} {r['engine']:10} "
              f"{(r['duration_s'] or 0):7.1f}s  {r['baseline_cells']:>5,} baseline cells  "
              f"{r['replay_events']:>8,} replay events  {r['run_id']}")


if __name__ == "__main__":
    main()
