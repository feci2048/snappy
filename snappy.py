#!/usr/bin/env python3
#
# snappy - tiered ZFS snapshot manager
# Single-file, zero-dependency implementation with sanoid.conf compatibility.
#
# Naming convention:
#   {prefix}-{YYYY-MM-DD}_{HH:MM:SS}_{tier}
#
# Tiers: daily, weekly, monthly, yearly
# Promotions only change the tier suffix; timestamp stays immutable.
#
# Verbosity:
#   -v   = verbose
#   -vv  = more verbose
#   -vvv = debug
#

import configparser
import subprocess
import argparse
import datetime
import re
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple


# ============================================================
# Verbosity helpers
# ============================================================

VERBOSITY = 0

def v1(msg):     # verbose (-v)
    if VERBOSITY >= 1:
        print(msg)

def v2(msg):     # very verbose (-vv)
    if VERBOSITY >= 2:
        print(msg)

def v3(msg):     # debug (-vvv)
    if VERBOSITY >= 3:
        print(msg)


# ============================================================
# Data structures
# ============================================================

@dataclass
class Snapshot:
    dataset: str
    full_name: str         # dataset@snapname
    prefix: str
    timestamp: datetime.datetime
    tier: str              # daily/weekly/monthly/yearly
    used: int              # ZFS "used" (unique bytes)


@dataclass
class RetentionSpec:
    daily: int
    weekly: int
    monthly: int
    yearly: int
    prefix: str


# ============================================================
# Config loading (sanoid.conf syntax)
# ============================================================

def load_sanoid_conf(path: str) -> Dict[str, Dict[str, str]]:
    """Load sanoid.conf into a simple dict[section][key]=value."""
    parser = configparser.ConfigParser(
        interpolation=None,
        strict=False,
        delimiters=('=',),
        comment_prefixes=('#'),
        inline_comment_prefixes=('#')
    )
    parser.optionxform = str  # preserve case

    with open(path, "r") as f:
        parser.read_file(f)

    cfg = {}
    for sec in parser.sections():
        cfg[sec] = {}
        for key, value in parser.items(sec):
            val = value.strip()
            if val.isdigit():
                val = int(val)
            cfg[sec][key] = val

    return cfg


def resolve_template(cfg: Dict[str, Dict[str, str]], section: str) -> Dict[str, str]:
    """Merge template into the dataset section if use_template is set."""
    sec = cfg.get(section, {})
    if "use_template" not in sec:
        return sec

    tpl = sec["use_template"]
    tpl_sec = f"template_{tpl}"
    if tpl_sec not in cfg:
        raise ValueError(f"Template '{tpl}' referenced by '{section}' not found")

    merged = cfg[tpl_sec].copy()
    merged.update(sec)
    return merged


# ============================================================
# Timestamp parsing
# ============================================================

def parse_timestamp(ts: str) -> datetime.datetime:
    """Parse our canonical timestamp YYYY-MM-DD_HH:MM:SS."""
    for fmt in ("%Y-%m-%d_%H:%M:%S",):
        try:
            return datetime.datetime.strptime(ts, fmt)
        except ValueError:
            pass
    raise ValueError(f"Unsupported timestamp: {ts}")


# ============================================================
# Snapshot enumeration
# ============================================================

def list_snapshots(dataset: str, prefix: str) -> List[Snapshot]:
    """
    List ZFS snapshots for a dataset, filtering only our snapshots:
    {prefix}-{timestamp}_{tier}
    """

    cmd = [
        "zfs", "list", "-t", "snapshot",
        "-o", "name,used", "-H",
        "-s", "creation", dataset
    ]
    v3(f"Running zfs list: {' '.join(cmd)}")

    raw = subprocess.check_output(cmd, text=True)

    # naming pattern: prefix-YYYY-MM-DD_HH:MM:SS_tier
    regex = re.compile(
        rf"^{re.escape(prefix)}-(\d{{4}}-\d{{2}}-\d{{2}}_\d{{2}}:\d{{2}}:\d{{2}})_(daily|weekly|monthly|yearly)$"
    )

    snaps = []
    for line in raw.splitlines():
        full, used = line.split("\t")
        if "@" not in full:
            continue

        ds, snapname = full.split("@", 1)

        m = regex.match(snapname)
        if not m:
            v3(f"Ignoring foreign snapshot: {full}")
            continue

        ts_str, tier = m.groups()
        ts = parse_timestamp(ts_str)

        snap = Snapshot(
            dataset=ds,
            full_name=full,
            prefix=prefix,
            timestamp=ts,
            tier=tier,
            used=int(used)
        )
        snaps.append(snap)

    v2(f"Found {len(snaps)} managed snapshots on {dataset}")
    return snaps

# ============================================================
# ZFS operations
# ============================================================

def zfs_destroy(full: str, dry: bool):
    """Delete a snapshot."""
    if dry:
        print(f"[DRY] DELETE {full}")
    else:
        print(f"DELETE {full}")
        subprocess.check_call(["zfs", "destroy", full])


def zfs_rename(old: str, new: str, dry: bool):
    """Rename (promote) snapshot."""
    if dry:
        print(f"[DRY] RENAME {old} → {new}")
    else:
        print(f"RENAME {old} → {new}")
        subprocess.check_call(["zfs", "rename", old, new])


def zfs_create(full: str, dry: bool):
    """Create snapshot."""
    if dry:
        print(f"[DRY] CREATE {full}")
    else:
        print(f"CREATE {full}")
        subprocess.check_call(["zfs", "snapshot", full])


# ============================================================
# Snapshot creation logic
# ============================================================

def create_daily_snapshot(dataset: str, prefix: str, snaps: List[Snapshot], dry: bool):
    """Create a daily snapshot, only if:
       - we haven't created a daily snapshot today
       - last snapshot represents a different state (used > 0)
    """

    now = datetime.datetime.now()
    today = now.date()

    # find newest snapshot (if any)
    last = snaps[-1] if snaps else None

    if last:
        # daily snapshot for today already exists?
        if last.timestamp.date() == today and last.tier == "daily":
            v1("Skipped creation: daily snapshot for today already exists.")
            return

        # if no change since last snapshot, avoid creating duplicate
        if last.used == 0:
            v1("Skipped creation: dataset unchanged since last snapshot.")
            return

    # create snapshot name
    ts = now.strftime("%Y-%m-%d_%H:%M:%S")
    name = f"{dataset}@{prefix}-{ts}_daily"

    zfs_create(name, dry)


# ============================================================
# Helpers for retention logic
# ============================================================

def age_days(ts: datetime.datetime, now: datetime.datetime) -> int:
    return (now - ts).days


def group_by_day(snaps: List[Snapshot]):
    buckets = {}
    for s in snaps:
        buckets.setdefault(s.timestamp.date(), []).append(s)
    return buckets


def group_by_week(snaps: List[Snapshot]):
    buckets = {}
    for s in snaps:
        y, w, _ = s.timestamp.isocalendar()
        buckets.setdefault((y, w), []).append(s)
    return buckets


def group_by_month(snaps: List[Snapshot]):
    buckets = {}
    for s in snaps:
        ym = (s.timestamp.year, s.timestamp.month)
        buckets.setdefault(ym, []).append(s)
    return buckets


def pick_best(group: List[Snapshot]) -> Snapshot:
    """Pick snapshot with largest 'used', tie-broken by newest timestamp."""
    return sorted(group, key=lambda s: (s.used, s.timestamp))[-1]


# ============================================================
# Tiered retention engine
# ============================================================

def tiered_retention(snaps: List[Snapshot], spec: RetentionSpec, dry: bool):
    """Main retention logic. Implements:
       - daily collapse
       - weekly selection
       - monthly selection
       - yearly selection
       - tier promotion (rename)
       - removal of redundant snapshots
    """

    if not snaps:
        v2("No snapshots to retain/prune.")
        return

    now = datetime.datetime.now()
    snaps = sorted(snaps, key=lambda s: s.timestamp)

    v3(f"Sorted {len(snaps)} snapshots.")

    # Calculate age windows
    daily_window   = spec.daily
    weekly_window  = spec.daily + spec.weekly * 7
    monthly_window = weekly_window + spec.monthly * 30
    yearly_window  = monthly_window + spec.yearly * 365

    v3(f"Daily window:   0–{daily_window} days")
    v3(f"Weekly window:  {daily_window+1}–{weekly_window} days")
    v3(f"Monthly window: {weekly_window+1}–{monthly_window} days")
    v3(f"Yearly window:  {monthly_window+1}+ days")

    keep = set()
    deletes = set()
    renames: List[Tuple[str, str]] = []

    # ------------------------------
    # DAILY TIER
    # ------------------------------
    v1("Processing DAILY tier…")

    daily_zone = [s for s in snaps if age_days(s.timestamp, now) <= daily_window]
    v2(f"Daily zone contains {len(daily_zone)} snapshots.")

    daily_buckets = group_by_day(daily_zone)

    for day, group in daily_buckets.items():
        best = pick_best(group)
        v2(f"Daily {day}: {len(group)} snaps → keeping {best.full_name}")

        keep.add(best.full_name)

        # promote if wrong tier
        if best.tier != "daily":
            new = best.full_name.replace("_" + best.tier, "_daily")
            v1(f"Promote {best.full_name} → {new}")
            renames.append((best.full_name, new))
            keep.add(new)

        # delete the rest
        for s in group:
            if s.full_name != best.full_name:
                deletes.add(s.full_name)

    # ------------------------------
    # WEEKLY TIER
    # ------------------------------
    v1("Processing WEEKLY tier…")

    weekly_zone = [s for s in snaps
                   if daily_window < age_days(s.timestamp, now) <= weekly_window]

    weekly_buckets = group_by_week(weekly_zone)

    for wk, group in weekly_buckets.items():
        best = pick_best(group)
        v2(f"Weekly {wk}: {len(group)} snaps → keeping {best.full_name}")

        keep.add(best.full_name)

        if best.tier != "weekly":
            new = best.full_name.replace("_" + best.tier, "_weekly")
            v1(f"Promote {best.full_name} → {new}")
            renames.append((best.full_name, new))
            keep.add(new)

        for s in group:
            if s.full_name != best.full_name:
                deletes.add(s.full_name)

    # ------------------------------
    # MONTHLY TIER
    # ------------------------------
    v1("Processing MONTHLY tier…")

    monthly_zone = [s for s in snaps
                    if weekly_window < age_days(s.timestamp, now) <= monthly_window]

    month_buckets = group_by_month(monthly_zone)

    for ym, group in month_buckets.items():
        best = pick_best(group)
        v2(f"Monthly {ym}: {len(group)} snaps → keeping {best.full_name}")

        keep.add(best.full_name)

        if best.tier != "monthly":
            new = best.full_name.replace("_" + best.tier, "_monthly")
            v1(f"Promote {best.full_name} → {new}")
            renames.append((best.full_name, new))
            keep.add(new)

        for s in group:
            if s.full_name != best.full_name:
                deletes.add(s.full_name)

    # ------------------------------
    # YEARLY TIER
    # ------------------------------
    v1("Processing YEARLY tier…")

    yearly_zone = [s for s in snaps
                   if age_days(s.timestamp, now) > monthly_window]

    year_buckets = {}
    for s in yearly_zone:
        year = s.timestamp.year
        year_buckets.setdefault(year, []).append(s)

    for year, group in year_buckets.items():
        best = pick_best(group)
        v2(f"Yearly {year}: {len(group)} snaps → keeping {best.full_name}")

        keep.add(best.full_name)

        if best.tier != "yearly":
            new = best.full_name.replace("_" + best.tier, "_yearly")
            v1(f"Promote {best.full_name} → {new}")
            renames.append((best.full_name, new))
            keep.add(new)

        for s in group:
            if s.full_name != best.full_name:
                deletes.add(s.full_name)

    # ------------------------------
    # EXECUTE OPERATIONS
    # ------------------------------

    v1("Executing promotions…")
    for old, new in renames:
        if old != new and old not in deletes:
            zfs_rename(old, new, dry)

    v1("Executing deletions…")
    for full in deletes:
        if full not in keep:
            zfs_destroy(full, dry)

    v1("Retention processing finished.")

# ============================================================
# CLI / main entrypoint
# ============================================================

def main():
    global VERBOSITY

    ap = argparse.ArgumentParser(
        description="snappy — tiered ZFS snapshot manager (daily/weekly/monthly/yearly)"
    )
    ap.add_argument(
        "--conf",
        default="/etc/sanoid/sanoid.conf",
        help="Path to sanoid.conf (default: /etc/sanoid/sanoid.conf)"
    )
    ap.add_argument(
        "--dry",
        action="store_true",
        help="Dry-run mode (show what would happen, but do nothing)"
    )
    ap.add_argument(
        "--create",
        action="store_true",
        help="Create today's daily snapshot (state-aware)"
    )
    ap.add_argument(
        "-v",
        action="count",
        default=0,
        help="Increase verbosity (-v, -vv, -vvv)"
    )
    ap.add_argument(
        "dataset",
        help="ZFS dataset to manage (e.g. tank/data)"
    )

    args = ap.parse_args()
    VERBOSITY = args.v

    # --------------------------------------------------------
    # Load and resolve dataset config
    # --------------------------------------------------------
    cfg = load_sanoid_conf(args.conf)
    sec = resolve_template(cfg, args.dataset)

    prefix = sec.get("snapshot_prefix", "snappy")

    spec = RetentionSpec(
        daily=int(sec.get("daily", 0)),
        weekly=int(sec.get("weekly", 0)),
        monthly=int(sec.get("monthly", 0)),
        yearly=int(sec.get("yearly", 0)),
        prefix=prefix
    )

    v1(f"Using prefix: {prefix}")
    v1(f"Retention: daily={spec.daily}, weekly={spec.weekly}, "
       f"monthly={spec.monthly}, yearly={spec.yearly}")

    # --------------------------------------------------------
    # Load existing snapshots
    # --------------------------------------------------------
    snaps = list_snapshots(args.dataset, prefix)

    # --------------------------------------------------------
    # Snapshot creation
    # --------------------------------------------------------
    if args.create:
        v1("Creating daily snapshot (if necessary)...")
        create_daily_snapshot(args.dataset, prefix, snaps, args.dry)
        # refresh after creation
        snaps = list_snapshots(args.dataset, prefix)

    # --------------------------------------------------------
    # Apply retention rules
    # --------------------------------------------------------
    v1("Applying retention policy...")
    tiered_retention(snaps, spec, dry=args.dry)


# ============================================================
# Entry point
# ============================================================

if __name__ == "__main__":
    main()
