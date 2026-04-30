# bsc-aws-exporter

> Export BSC on-chain data into daily Parquet files, with a layout aligned to the AWS Public Blockchain Datasets.

`Python 3.10+`  ·  `ethereum-etl`  ·  `pyarrow`  ·  `boto3`

---

The output directory layout matches the ETH dataset under AWS Public Blockchain Datasets:

```
s3://{bucket}/{prefix}/blocks/date=YYYY-MM-DD/blocks.parquet
s3://{bucket}/{prefix}/transactions/date=YYYY-MM-DD/transactions.parquet
```

The underlying export is performed by [`ethereum-etl`](https://github.com/blockchain-etl/ethereum-etl).
This project is responsible for: locating the block range for a given day, converting CSV → Parquet
(schema alignment + precise timestamp-based boundary trimming), uploading to S3, resuming from
checkpoints, and scheduled execution via systemd.

## Project structure

```
bsc-aws-exporter/
├── exporter.py               # Main program
├── config.yaml.example       # Configuration template
├── requirements.txt          # Python dependencies
├── README.md
├── .gitignore
└── systemd/                  # Scheduled deployment kit
    ├── bsc-exporter.service
    ├── bsc-exporter.timer
    └── install.sh
```

---

## Dependencies

```
ethereum-etl>=2.4.0
pyarrow>=14.0.0
boto3>=1.34.0
pyyaml>=6.0
web3>=5.31,<6.0      # ethereum-etl 2.x depends on the web3 v5 API
setuptools<70        # ethereum-etl 2.x uses pkg_resources, removed in setuptools>=70
```

Install:

```bash
pip install -r requirements.txt
```

A Python 3.10+ virtualenv is recommended for isolation.

---

## Configuration

Copy `config.yaml.example` to `config.yaml` and edit as needed:

```yaml
rpc_url: "https://bsc-dataseed.bnbchain.org"   # Recommended: use your own full node for backfills

s3:
  bucket: "aws-public-blockchain"
  prefix: "v1.1/bnb"
  region: "us-east-2"

export:
  ethereumetl_batch_size: 100   # Blocks per RPC batch call
  ethereumetl_workers: 5        # ethereum-etl concurrent workers
  row_group_size: 50000         # Parquet row group size
  work_dir: "/tmp/bsc-export"   # Local temp directory + progress file
```

AWS credentials follow the boto3 default chain: environment variables, `~/.aws/credentials`, or an IAM Role all work.

---

## Usage

```bash
# Export yesterday's data (recommended for cron / scheduled jobs)
python exporter.py --config config.yaml

# Export a single specified date
python exporter.py --config config.yaml --date 2024-01-15

# Backfill a date range (inclusive), with 4 parallel processes
python exporter.py --config config.yaml \
    --start 2020-08-29 --end 2026-04-29 -j 4

# Local dry-run: produce Parquet files only, do not upload
python exporter.py --config config.yaml --date 2024-01-15 --dry-run
```

Arguments:

| Argument | Description |
|------|------|
| `--config` | Path to the configuration file (required) |
| `--date` | Export a single date `YYYY-MM-DD` |
| `--start` / `--end` | Range backfill; must be used together |
| `-j`, `--parallel` | Number of parallel processes for backfill (default 1) |
| `--dry-run` | Produce Parquet files locally only; skip S3 upload |

If neither `--date` nor a range is provided, the tool defaults to exporting yesterday in UTC.

---

## Resume from checkpoints

`work_dir/progress.txt` records dates that have already been completed; reruns skip them
automatically. If both target Parquet files already exist on S3, the date is also skipped.

The skip decision order is:

1. Hit in `progress.txt` → skip
2. Both `blocks.parquet` and `transactions.parquet` exist on S3 → skip and backfill the
   `progress.txt` entry
3. Otherwise, export normally

In parallel mode, writes to `progress.txt` are protected with `flock`.

---

## Workflow

```
Bisect via RPC to find the day's start_block; end_block = estimate + margin (slight over-shoot)
        ↓
ethereumetl export_blocks_and_transactions  →  blocks.csv / transactions.csv
        ↓
pyarrow CSV → Parquet: filter by timestamp / block_timestamp to the exact day boundary
        ↓
boto3 multipart upload to S3, with head_object validation against ContentLength
        ↓
Mark done in progress.txt and clean up the temp directory
```

Boundary handling uses timestamp filtering rather than bisection on both ends: `start_block`
must be exact (we cannot filter blocks we never downloaded), while `end_block` can be loose
— estimate + `ESTIMATE_MARGIN` over-shoots by a few thousand blocks, and pyarrow then
filters by the `timestamp` / `block_timestamp` field to `[day_start_ts, day_end_ts)`,
naturally eliminating the "first block of the next day" off-by-one ambiguity.

The timestamp → block estimator is aware of BSC mainnet hardforks that have shortened the
block interval:

| Fork | UTC time | Block interval |
|------|---------|---------|
| Genesis | 2020-08-29 03:24:28 | 3000ms |
| Lorentz | 2025-04-29 05:05:00 | 1500ms |
| Maxwell | 2025-06-30 02:30:00 | 750ms |
| Fermi   | 2026-01-14 02:30:00 | 450ms |

If a future hardfork further shortens the block interval, append a new entry to the
`BSC_FORKS` list at the top of `exporter.py`. Even if you forget to update it, the binary
search acts as a safety net (a performance hit, but the result is still correct).

The S3 upload order is `transactions.parquet` first, then `blocks.parquet`, with the
latter serving as the completion marker: `_date_on_s3` only treats a date as done when
both files exist, so a mid-run crash never lets downstream consumers read a half-written
result.

---

## Deployment: systemd timer (recommended)

The `systemd/` directory ships a complete deployment kit:

```
systemd/
├── bsc-exporter.service   # One-shot unit; runs once and exits
├── bsc-exporter.timer     # Triggers daily at 01:00 UTC; missed runs are caught up
└── install.sh             # Creates the user, installs dependencies, installs unit files
```

### Install

```bash
sudo ./systemd/install.sh
sudo vi /etc/bsc-exporter/config.yaml         # Edit your own rpc_url / S3 bucket
sudo systemctl start bsc-exporter.service     # Run once manually to verify
sudo journalctl -u bsc-exporter -f            # Tail logs
sudo systemctl enable --now bsc-exporter.timer  # Enable the daily schedule
```

### Key design points

- **`Type=oneshot`**: the service runs to completion and exits — it is not a long-running
  daemon. Crash recovery and upgrades simply rely on the next scheduled run.
- **`Persistent=true`**: runs that the machine missed while powered off are caught up on
  the next boot.
- **`RandomizedDelaySec=600`**: random delay of 0–10 minutes to avoid multiple nodes
  hitting the RPC at exactly the same time.
- **`ConcurrencyPolicy` equivalent**: a systemd `oneshot` unit cannot trigger itself
  concurrently by design.
- **Sandboxing**: hardening options like `ProtectSystem=strict` and `PrivateTmp=true`
  are enabled.

### Path conventions

| Path | Purpose |
|------|------|
| `/opt/bsc-exporter/` | Code + venv |
| `/etc/bsc-exporter/config.yaml` | Configuration |
| `/var/lib/bsc-exporter/` | progress.txt + temporary CSV/Parquet (matches `export.work_dir`) |
| `/var/log/bsc-exporter/` | Reserved; logs currently go through journald |

The environment variables at the top of `install.sh` can override the default paths.

### Inspect / troubleshoot

```bash
systemctl list-timers bsc-exporter.timer    # Next scheduled run
journalctl -u bsc-exporter --since "2 days ago"
journalctl -u bsc-exporter -p err           # Errors only
systemctl status bsc-exporter.service       # Last run result + exit code
```

---

## Other operational notes

- **Backfill**: for one-off bulk backfills outside the regular schedule, log into the
  machine and run it manually — do not go through the timer:
  ```bash
  sudo -u bsc /opt/bsc-exporter/venv/bin/python /opt/bsc-exporter/exporter.py \
      --config /etc/bsc-exporter/config.yaml \
      --start 2020-08-29 --end 2026-04-29 -j 4
  ```
- **Failure investigation**: log lines are prefixed with the date as `[YYYY-MM-DD]`; on
  failure the temp directory is cleaned up, while in `--dry-run` mode files are kept in
  `work_dir/{date}/` for manual inspection.
- **Disk**: `work_dir` peaks at a few GB per day (CSV + Parquet coexist briefly); a
  successful run cleans up automatically. With `-j N` parallelism the peak is N times
  larger.
- **AWS credentials**: on EC2, prefer an instance profile (IAM Role) — no extra
  configuration is needed in the service file. In other environments, write the keys
  to `/etc/bsc-exporter/aws.env`, uncomment the `EnvironmentFile=` line, and
  `chmod 600` the file.
