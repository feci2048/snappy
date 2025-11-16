# Snappy — Sane, Tiered ZFS Snapshot Retention
### *A single-file, zero-dependency snapshot manager using suffix-tier naming and meaningful retention logic.*

---

## **Why Snappy Exists**

ZFS is an exceptional filesystem. Its snapshot model is elegant: snapshots represent **actual historical states** of the dataset using copy-on-write, block-level deltas, and mathematically clean semantics.

Unfortunately, the tooling layered on top of ZFS often ignores this elegance completely.

Most snapshot managers — especially Sanoid-based workflows — treat snapshots like:

```
folders with date names and labels
```

They:

- create snapshots blindly on schedule  
- keep M “dailies”, N “weeklies”, P “monthlies”  
- create *multiple snapshots representing identical data*  
- store redundant, meaningless timeline points  
- bloat lists with hundreds of identical entries  
- promote/demote snapshots based on naming conventions rather than dataset state  
- and pretend this is “tiered retention”

It isn’t.

This mismatch between **ZFS’s actual design** and **snapshot managers’ naive time-rotation logic** results in:

- massive amounts of redundant snapshots  
- confusing retention semantics  
- users believing they have “365 days” of history when they actually have *1 state repeated 365 times*  
- unpredictable long-term retention  
- painful cleanup tooling built just to navigate the mess

**Snappy is the antidote to this.**

---

## **The Snappy Philosophy**

Snappy is founded on three principles:

### **1. Only meaningful snapshots should exist**  
If the dataset didn’t change, there is no point in creating another “daily” snapshot.  
If multiple snapshots exist in the same 24-hour window, they collapse to a single representative.

Snappy ensures that:

- one daily = one actual dataset state  
- one weekly = a summary of that week  
- one monthly = a summary of that month  
- one yearly = long-term historical anchor  

You never keep two snapshots that represent identical blocks.

### **2. Tiers should be non-overlapping, timeline-accurate, and state-aware**  
Snappy implements proper tiering:

- **Daily tier**: last M days, at most one per day  
- **Weekly tier**: next N weeks, at most one per week  
- **Monthly tier**: next P months, at most one per month  
- **Yearly tier**: everything older  

Snapshots are **promoted** through tiers as they age.  
Not duplicated.  
Not overlapped.  
Not competing with newer tiers.

This is how tiered retention actually should behave.

### **3. Snapshot naming must be human-readable, machine-parsable, and stable**  
Snappy uses a clean naming structure:

```
<prefix>-YYYY-MM-DD_HH:MM:SS_<tier>
```

Examples:

```
snappy-2025-11-14_14:45:02_daily
snappy-2025-11-14_14:45:02_weekly
snappy-2025-11-14_14:45:02_monthly
snappy-2025-11-14_14:45:02_yearly
```

During promotion, **only the tier suffix changes**.  
Timestamp and prefix remain fixed.

This makes:

- searching easy  
- sorting meaningful  
- state tracking trivial  
- promotions obvious  
- human scanning comfortable  
- deduplication safe  

This is a near-perfect UX for snapshot naming.

---

## **What Snappy Is**

- A sane retention engine  
- A safe, state-aware snapshot creator  
- A ZFS-native tool  
- A single-file Python script  
- With zero external dependencies  
- Fully compatible with sanoid.conf syntax  
- Designed to run **alongside** Sanoid or TrueNAS auto-snapshots  
- Fast, predictable, deterministic  
- Implemented exactly according to ZFS semantics  

---

## **What Snappy Is NOT**

- Not a Sanoid wrapper  
- Not a cron scheduler (you schedule it yourself)  
- Not a snapshot whale (it does not create meaningless snapshots)  
- Not a multi-file Python package  
- Not a fork of other tools  
- Not an automation daemon  
- Not tied to systemd or journald  
- Not dependent on any Python libraries  

Snappy is deliberately minimalistic and ZFS-focused.

---

## **Key Features**

### **Daily creation only when necessary**

Snappy examines the most recent snapshot:

- If the dataset hasn’t changed → **no new snapshot**
- If a daily for today already exists → **no new snapshot**
- Otherwise → *one* snapshot with the correct timestamp pattern

This prevents bloating your timeline with redundant “daily” entries.

### **Tiered snapshot retention (D/W/M/Y)**

Snappy determines:

- which snapshots belong to the daily window  
- which belong to weekly  
- which belong to monthly  
- which belong to yearly  

Old snapshots are **promoted**, not duplicated.  
Useless ones are removed.

### **Promotion by renaming**

```
snappy-2025-11-14_14:45:02_daily
→ snappy-2025-11-14_14:45:02_weekly
→ snappy-2025-11-14_14:45:02_monthly
→ snappy-2025-11-14_14:45:02_yearly
```

The snapshot stays the same set of blocks — only its tier classification changes.

### **Prefix filtering**

Safe to run alongside Sanoid / TrueNAS / manual snapshots.  
Snappy only touches snapshots whose naming matches the configured prefix.

### **Verbose and Debug Modes**

- `-v` → high-level decisions  
- `-vv` → grouping, pick rules  
- `-vvv` → internal debug (regex matches, raw lists)

### **Dry-run mode**

```
snappy --dry -vvv
```

---

## **Installation**

```
cp snappy /usr/local/sbin/snappy
chmod +x /usr/local/sbin/snappy
```

Requires:

- Python 3  
- ZFS CLI  

---

## **Configuration Through sanoid.conf**

Snappy reads:

- retention parameters  
- dataset sections  
- template inheritance  
- optional snapshot prefix  

Example:

```
[tank/data]
    use_template = default

[template_default]
    daily = 7
    weekly = 4
    monthly = 12
    yearly = 3
    snapshot_prefix = snappy
```

---

## **CLI Examples**

Create a daily snapshot:

```
snappy --dataset tank/data --create
```

Apply retention:

```
snappy --dataset tank/data
```

Dry-run:

```
snappy --dataset tank/data --dry -vv
```

Debug:

```
snappy --dataset tank/data --dry -vvv
```

---

## **Why Snappy Is Better Than Sanoid**

Sanoid:

- treats retention counts literally  
- creates redundant dailies  
- mixes tiers  
- keeps snapshots representing identical data  
- retains clutter  
- does not promote snapshots  
- does not collapse duplicates  

Snappy:

- state-aware  
- deterministic  
- collapses redundant snapshots  
- promotes snapshots across tiers  
- names snapshots uniformly  
- ignores foreign snapshots  
- implements real timeline-based tiering  

---

## License

MIT License.

---

## In One Sentence

**Snappy gives you the retention policy users *think* Sanoid provides — and the one ZFS was actually designed for.**
