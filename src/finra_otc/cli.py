"""finra-otc: capture FINRA OTC market data, then build Parquet tables."""

import argparse
import sys
from collections.abc import Sequence
from datetime import date
from pathlib import Path

from tqdm import tqdm

from . import api, build, cdn, legacy
from .http import RequestsClient
from .raw import RawStore, data_dir


def _date(value: str) -> date:
    return date.fromisoformat(value)


def _month(value: str) -> date:
    return date.fromisoformat(value + "-01")


def get_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="finra-otc", description=__doc__)
    p.add_argument(
        "--data",
        type=Path,
        default=None,
        help="data folder (default: $FINRA_OTC_DATA or ./data)",
    )
    p.add_argument("-q", "--quiet", action="store_true", help="no progress output")
    sub = p.add_subparsers(dest="command", required=True)

    cap = sub.add_parser("capture", help="save raw data from FINRA").add_subparsers(
        dest="source", required=True
    )

    a = cap.add_parser("api", help="api.finra.org otcMarket datasets")
    a.add_argument(
        "datasets",
        nargs="*",
        metavar="DATASET",
        help="datasets to capture (default: all); one of " + ", ".join(api.BY_NAME),
    )
    _date_range(a, "partition date")
    a.add_argument(
        "--workers", type=int, default=4, help="partitions fetched at once (4)"
    )
    a.add_argument(
        "--refresh-since",
        type=_date,
        help="fetch again partitions dated on or after this (kept if changed)",
    )

    d = cap.add_parser("regsho-daily", help="daily short-sale volume files (CDN)")
    _date_range(d, "trade date")
    d.add_argument("--facility", action="append", choices=list(cdn.DAILY_FACILITIES))
    d.add_argument("--recheck-absent", action="store_true")

    m = cap.add_parser(
        "regsho-monthly",
        help="monthly short-sale transaction ZIPs (CDN; several GB a month)",
    )
    m.add_argument("--since", type=_month, required=True, help="first month, YYYY-MM")
    m.add_argument("--until", type=_month, help="last month, YYYY-MM")
    m.add_argument("--facility", action="append", choices=list(cdn.MONTHLY_FACILITIES))
    m.add_argument("--recheck-absent", action="store_true")
    m.add_argument(
        "--dry-run", action="store_true", help="list files and sizes; save nothing"
    )

    lg = cap.add_parser("legacy", help="import weekly CSVs from the old site")
    lg.add_argument("folder", type=Path, help="folder of <year>/*.csv files")

    al = cap.add_parser("all", help="every API dataset, then regsho-daily")
    _date_range(al, "partition or trade date")

    b = sub.add_parser("build", help="build Parquet from the raw store")
    b.add_argument(
        "parts",
        nargs="*",
        metavar="PART",
        help="api, regsho-daily, regsho-monthly or legacy (default: all)",
    )
    b.add_argument("--dataset", action="append", choices=list(api.BY_NAME))

    sub.add_parser("status", help="what is in the raw store and the Parquet tables")
    return p


def _date_range(p: argparse.ArgumentParser, what: str) -> None:
    p.add_argument("--since", type=_date, help=f"earliest {what}, YYYY-MM-DD")
    p.add_argument("--until", type=_date, help=f"latest {what}, YYYY-MM-DD")


BUILD_PARTS = ["api", "regsho-daily", "regsho-monthly", "legacy"]


def main(argv: Sequence[str] | None = None) -> int:
    parser = get_parser()
    args = parser.parse_args(argv)
    for name in getattr(args, "datasets", None) or []:
        if name not in api.BY_NAME:
            parser.error(f"unknown dataset {name!r}")
    for part in getattr(args, "parts", None) or []:
        if part not in BUILD_PARTS:
            parser.error(f"unknown build part {part!r}; choose from {BUILD_PARTS}")
    store = RawStore(args.data or data_dir())

    def progress(message: str) -> None:
        if not args.quiet:
            tqdm.write(message, file=sys.stderr)

    failures = 0
    if args.command == "capture":
        client = RequestsClient()
        if args.source in ("api", "all"):
            names = getattr(args, "datasets", None) or list(api.BY_NAME)
            for name in names:
                r = api.capture_dataset(
                    client,
                    store,
                    api.BY_NAME[name],
                    since=args.since,
                    until=args.until,
                    refresh_since=getattr(args, "refresh_since", None),
                    workers=getattr(args, "workers", 4),
                    progress=progress,
                )
                print(
                    f"{name}: {r.planned} to fetch, {r.saved} saved, "
                    f"{r.unchanged} unchanged, {r.failed} failed"
                )
                failures += _report(r.errors)
        if args.source in ("regsho-daily", "all"):
            d = cdn.capture_daily(
                client,
                store,
                since=args.since,
                until=args.until,
                facilities=getattr(args, "facility", None),
                recheck_absent=getattr(args, "recheck_absent", False),
                progress=progress,
            )
            print(_cdn_summary(d))
            failures += _report(d.errors)
        if args.source == "regsho-monthly":
            m = cdn.capture_monthly(
                client,
                store,
                since=args.since,
                until=args.until,
                facilities=args.facility,
                recheck_absent=args.recheck_absent,
                dry_run=args.dry_run,
                progress=progress,
            )
            print(_cdn_summary(m, dry_run=args.dry_run))
            failures += _report(m.errors)
        if args.source == "legacy":
            saved, skipped = legacy.import_legacy(store, args.folder, progress=progress)
            print(f"legacy weekly: {saved} saved, {skipped} already captured")
    elif args.command == "build":
        parts = args.parts or BUILD_PARTS
        results: list[build.Result] = []
        if "api" in parts:
            results += build.build_api(store, args.dataset, progress=progress)
        if "regsho-daily" in parts:
            results.append(build.build_daily(store, progress=progress))
        if "regsho-monthly" in parts:
            results.append(build.build_monthly(store, progress=progress))
        if "legacy" in parts:
            results.append(build.build_legacy(store, progress=progress))
        for b in results:
            if b.built or b.current or b.failed:
                print(
                    f"{b.source}: {b.built} built ({b.rows:,} rows), "
                    f"{b.current} up to date, {b.failed} failed"
                )
            failures += _report(b.errors)
    elif args.command == "status":
        status(store)
    return 1 if failures else 0


def _cdn_summary(r: cdn.Result, dry_run: bool = False) -> str:
    size = f"{r.bytes / 1e9:,.2f} GB"
    if dry_run:
        return f"{r.source}: {size} to download; {r.absent} missing"
    return (
        f"{r.source}: {r.saved} saved ({size}), {r.unchanged} unchanged, "
        f"{r.absent} missing, {r.skipped} already done, {r.failed} failed"
    )


def _report(errors: list[str]) -> int:
    for e in errors:
        print(f"  error: {e}", file=sys.stderr)
    return len(errors)


def status(store: RawStore) -> None:
    print(f"{'source':38} {'keys':>7} {'captures':>9} {'stored MB':>10}")
    for source in store.sources():
        captures = list(store.captures(source))
        keys = {c.key for c in captures}
        stored = sum(f.stored_bytes for c in captures for f in c.files)
        print(f"{source:38} {len(keys):>7,} {len(captures):>9,} {stored / 1e6:>10,.1f}")
    out = build.parquet_dir(store)
    if out.exists():
        files = list(out.glob("**/*.parquet"))
        size = sum(f.stat().st_size for f in files)
        print(f"\nparquet: {len(files):,} files, {size / 1e6:,.1f} MB in {out}")


if __name__ == "__main__":
    sys.exit(main())
