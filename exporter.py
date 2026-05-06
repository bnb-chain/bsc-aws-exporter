#!/usr/bin/env python3
"""
BSC Data Exporter for AWS Public Blockchain Datasets.

Uses ethereum-etl to export blocks & transactions, converts to Parquet,
and uploads to S3 with partition structure:
    {prefix}/blocks/date=YYYY-MM-DD/blocks.parquet
    {prefix}/transactions/date=YYYY-MM-DD/transactions.parquet

Usage:
    python exporter.py --config config.yaml                                          # yesterday
    python exporter.py --config config.yaml --date 2024-01-15                        # single date
    python exporter.py --config config.yaml --start 2020-08-29 --end 2026-04-29 -j4  # backfill
    python exporter.py --config config.yaml --date 2024-01-15 --dry-run              # local only
"""

import argparse
import fcntl
import logging
import os
import shutil
import subprocess
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

import boto3
from boto3.s3.transfer import TransferConfig
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pv
import pyarrow.parquet as pq
import yaml
from web3 import Web3
from web3.middleware import geth_poa_middleware

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ── BSC constants ────────────────────────────────────────────────────
# Block interval timeline: (fork_timestamp, interval_ms).
# Update when new hardforks change the block interval.
BSC_FORKS = [
    (1598671468, 3000),  # genesis  — 2020-08-29
    (1745903100, 1500),  # Lorentz  — 2025-04-29
    (1751250600,  750),  # Maxwell  — 2025-06-30
    (1768357800,  450),  # Fermi    — 2026-01-14
]
BSC_GENESIS_TS = BSC_FORKS[0][0]
ESTIMATE_MARGIN = 4000   # blocks; ≈30min at 450ms
LATEST_BLOCK_TTL = 300   # seconds

# ── Parquet schemas (matches ethereum-etl CSV output) ────────────────
# Large integers use string to avoid precision loss.

BLOCK_SCHEMA = pa.schema([
    ("number", pa.int64()),
    ("hash", pa.string()),
    ("parent_hash", pa.string()),
    ("nonce", pa.string()),
    ("sha3_uncles", pa.string()),
    ("logs_bloom", pa.string()),
    ("transactions_root", pa.string()),
    ("state_root", pa.string()),
    ("receipts_root", pa.string()),
    ("miner", pa.string()),
    ("difficulty", pa.string()),
    ("total_difficulty", pa.string()),
    ("size", pa.int64()),
    ("extra_data", pa.string()),
    ("gas_limit", pa.int64()),
    ("gas_used", pa.int64()),
    ("timestamp", pa.int64()),
    ("transaction_count", pa.int32()),
    ("base_fee_per_gas", pa.int64()),
    ("withdrawals_root", pa.string()),
    ("withdrawals", pa.string()),
    ("blob_gas_used", pa.int64()),
    ("excess_blob_gas", pa.int64()),
])

TRANSACTION_SCHEMA = pa.schema([
    ("hash", pa.string()),
    ("nonce", pa.int64()),
    ("block_hash", pa.string()),
    ("block_number", pa.int64()),
    ("transaction_index", pa.int32()),
    ("from_address", pa.string()),
    ("to_address", pa.string()),
    ("value", pa.string()),
    ("gas", pa.int64()),
    ("gas_price", pa.int64()),
    ("input", pa.string()),
    ("block_timestamp", pa.int64()),
    ("max_fee_per_gas", pa.int64()),
    ("max_priority_fee_per_gas", pa.int64()),
    ("transaction_type", pa.int32()),
    ("max_fee_per_blob_gas", pa.int64()),
    ("blob_versioned_hashes", pa.string()),
])


def _make_csv_convert(schema: pa.Schema) -> pv.ConvertOptions:
    """ConvertOptions matching schema — every column gets an explicit type so
    pyarrow doesn't try to infer (which can mishandle huge integers like
    `value`/`difficulty` that we keep as string to avoid precision loss)."""
    return pv.ConvertOptions(
        column_types={f.name: f.type for f in schema},
        strings_can_be_null=True,
    )


BLOCK_CSV_CONVERT = _make_csv_convert(BLOCK_SCHEMA)
TX_CSV_CONVERT = _make_csv_convert(TRANSACTION_SCHEMA)


# ── Helpers ──────────────────────────────────────────────────────────

def load_config(path: str) -> dict:
    with open(path) as f:
        cfg = yaml.safe_load(f) or {}
    missing = [k for k in ("rpc_url", "s3") if k not in cfg]
    if missing:
        raise SystemExit(f"config.yaml missing required keys: {missing}")
    if "bucket" not in cfg["s3"]:
        raise SystemExit("config.yaml: s3.bucket is required")
    return cfg


def estimate_block(target_ts: int) -> int:
    """Rough block number from timestamp, walking BSC fork segments."""
    if target_ts <= BSC_GENESIS_TS:
        return 0
    blocks = 0
    for i, (seg_start, interval_ms) in enumerate(BSC_FORKS):
        if target_ts <= seg_start:
            break
        seg_end = BSC_FORKS[i + 1][0] if i + 1 < len(BSC_FORKS) else target_ts
        seg_end = min(seg_end, target_ts)
        blocks += (seg_end - seg_start) * 1000 // interval_ms
    return blocks


def csv_to_parquet(csv_path: str, parquet_path: str,
                   schema: pa.Schema, convert_opts: pv.ConvertOptions,
                   row_group_size: int,
                   ts_column: str | None = None,
                   ts_range: tuple[int, int] | None = None):
    """Convert ethereum-etl CSV to Parquet, aligned with schema.

    Optionally filters rows to `start <= ts_column < end` for exact day boundaries.
    """
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV not found: {csv_path}")
    table = pv.read_csv(csv_path, convert_options=convert_opts)

    # Align columns to schema: keep existing, fill missing with null
    target_cols = [f.name for f in schema]
    for field in schema:
        if field.name not in table.column_names:
            table = table.append_column(field.name,
                                        pa.nulls(len(table), type=field.type))
    table = table.select(target_cols).cast(schema)

    # Timestamp filter for precise day boundaries
    if ts_column and ts_range:
        lo, hi = ts_range
        mask = pc.and_(pc.greater_equal(table[ts_column], lo),
                       pc.less(table[ts_column], hi))
        before = len(table)
        table = table.filter(mask)
        if len(table) != before:
            log.info("Filtered %s: %d → %d rows",
                     os.path.basename(csv_path), before, len(table))

    pq.write_table(table, parquet_path, compression="snappy",
                    row_group_size=row_group_size)
    log.info("Wrote %s (%.2f MB, %d rows)",
             parquet_path, os.path.getsize(parquet_path) / 1e6, len(table))


# ── Progress tracker ─────────────────────────────────────────────────

class ProgressFile:
    """File-based progress with flock for multi-process safety."""

    def __init__(self, path: str):
        self.path = path
        self._cache: set[str] | None = None

    def _load(self) -> set[str]:
        if self._cache is None:
            if os.path.exists(self.path):
                with open(self.path) as f:
                    self._cache = {l.strip() for l in f if l.strip()}
            else:
                self._cache = set()
        return self._cache

    def is_done(self, key: str) -> bool:
        return key in self._load()

    def mark_done(self, key: str):
        with open(self.path, "a") as f:
            fcntl.flock(f, fcntl.LOCK_EX)
            try:
                f.write(key + "\n")
            finally:
                fcntl.flock(f, fcntl.LOCK_UN)
        self._load().add(key)


# ── Core exporter ────────────────────────────────────────────────────

class BSCExporter:
    def __init__(self, config: dict, parallel_mode: bool = False):
        self._parallel = parallel_mode
        self.rpc_url = config["rpc_url"]
        self.w3 = Web3(Web3.HTTPProvider(
            self.rpc_url, request_kwargs={"timeout": 30}))
        # BSC's Parlia (PoA) consensus puts validator signatures in extraData,
        # exceeding the 32-byte limit web3.py enforces by default. Inject the
        # geth-style PoA middleware to bypass that validation.
        self.w3.middleware_onion.inject(geth_poa_middleware, layer=0)
        if not self.w3.is_connected():
            raise ConnectionError(f"Cannot connect to RPC: {self.rpc_url}")
        log.info("Connected to BSC node, chain_id=%d", self.w3.eth.chain_id)

        s3_cfg = config["s3"]
        self.bucket = s3_cfg["bucket"]
        self.prefix = s3_cfg.get("prefix", "v1.1/bnb")
        s3_options = {}
        if "addressing_style" in s3_cfg:
            s3_options["addressing_style"] = s3_cfg["addressing_style"]
        if s3_cfg.get("endpoint_url"):
            # Custom S3-compatible endpoints such as Cloudflare R2 cannot be
            # combined with S3 Accelerate inherited from the AWS config file.
            s3_options.setdefault("addressing_style", "path")
            s3_options["use_accelerate_endpoint"] = False

        boto_config_args = {
            "max_pool_connections": 10,
            "retries": {"max_attempts": 3, "mode": "adaptive"},
        }
        if s3_options:
            boto_config_args["s3"] = s3_options
        boto_config = BotoConfig(**boto_config_args)

        # Optional inline credentials (handy for R2 / other S3-compatible
        # services where you don't want to mess with ~/.aws/). If absent,
        # boto3 falls back to the standard chain (env vars, ~/.aws/, IAM role).
        ak = s3_cfg.get("access_key_id")
        sk = s3_cfg.get("secret_access_key")
        if bool(ak) != bool(sk):
            raise SystemExit(
                "config.yaml: access_key_id and secret_access_key must be set together")
        creds = {}
        if ak and sk:
            creds["aws_access_key_id"] = ak
            creds["aws_secret_access_key"] = sk

        self.s3 = boto3.client(
            "s3", region_name=s3_cfg.get("region", "us-east-2"),
            endpoint_url=s3_cfg.get("endpoint_url"),
            config=boto_config,
            **creds)
        self.s3_transfer = TransferConfig(
            multipart_threshold=64 * 1024 * 1024,
            multipart_chunksize=64 * 1024 * 1024,
            max_concurrency=4)

        exp = config.get("export", {})
        self.etl_batch_size = exp.get("ethereumetl_batch_size", 100)
        self.etl_workers = exp.get("ethereumetl_workers", 5)
        self.row_group_size = exp.get("row_group_size", 50000)
        self.work_dir = exp.get("work_dir", "/tmp/bsc-export")

        os.makedirs(self.work_dir, exist_ok=True)
        self.progress = ProgressFile(os.path.join(self.work_dir, "progress.txt"))
        self._latest_block: int | None = None
        self._latest_block_at: float = 0.0

    @property
    def latest_block(self) -> int:
        now = time.monotonic()
        if self._latest_block is None or now - self._latest_block_at > LATEST_BLOCK_TTL:
            self._latest_block = self.w3.eth.block_number
            self._latest_block_at = now
        return self._latest_block

    # ── Block range ──────────────────────────────────────────────────

    def _bisect_block(self, lo: int, hi: int, target_ts: int) -> int | None:
        """First block with timestamp >= target_ts, or None."""
        result = None
        while lo <= hi:
            mid = (lo + hi) // 2
            ts = self.w3.eth.get_block(mid)["timestamp"]
            if ts >= target_ts:
                result, hi = mid, mid - 1
            else:
                lo = mid + 1
        return result

    def find_block_range(self, date: datetime) -> tuple[int, int, int, int]:
        """Returns (start_block, end_block, day_start_ts, day_end_ts).

        start_block is exact (bisect). end_block is over-estimated;
        csv_to_parquet's ts filter trims to exact day boundary.
        """
        day_start_ts = int(date.replace(
            hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc,
        ).timestamp())
        day_end_ts = day_start_ts + 86400
        latest = self.latest_block

        # Bisect start_block with estimated range, fallback to full range
        est = estimate_block(day_start_ts)
        lo, hi = max(0, est - ESTIMATE_MARGIN), min(latest, est + ESTIMATE_MARGIN)
        start = self._bisect_block(lo, hi, day_start_ts)
        if start is None:
            start = self._bisect_block(0, latest, day_start_ts)
        if start is None:
            raise ValueError(f"No blocks found for {date.date()}")

        # Defensive check: cumulative drift between estimate_block (theoretical
        # 3s/1.5s/750ms/450ms timeline) and reality can exceed ESTIMATE_MARGIN
        # over multi-year backfills, especially for 2021 Q1 BSC where blocks
        # averaged ~5s under congestion. If the bisect bottomed out at our
        # lower bound, the bound itself may have been past the true day-start;
        # widen the search downward until block[start-1].ts < day_start_ts.
        while start > 0:
            prev_ts = self.w3.eth.get_block(start - 1)["timestamp"]
            if prev_ts < day_start_ts:
                break
            log.warning("start_block bound too high (block %d ts=%d >= day_start=%d); widening",
                        start - 1, prev_ts, day_start_ts)
            earlier = self._bisect_block(0, start - 1, day_start_ts)
            if earlier is None or earlier >= start:
                break
            start = earlier

        # Over-estimate end; ts filter handles precision
        end = min(latest, estimate_block(day_end_ts) + ESTIMATE_MARGIN)

        log.info("Date %s: blocks %d..%d (%d blocks)",
                 date.strftime("%Y-%m-%d"), start, end, end - start + 1)
        return start, end, day_start_ts, day_end_ts

    # ── ethereum-etl ─────────────────────────────────────────────────

    def _run_etl(self, start: int, end: int, blocks_csv: str, txs_csv: str):
        cmd = [
            "ethereumetl", "export_blocks_and_transactions",
            "--start-block", str(start), "--end-block", str(end),
            "--provider-uri", self.rpc_url,
            "--blocks-output", blocks_csv, "--transactions-output", txs_csv,
            "--batch-size", str(self.etl_batch_size),
            "--max-workers", str(self.etl_workers),
        ]
        log.info("ethereum-etl: blocks %d..%d", start, end)
        if self._parallel:
            proc = subprocess.run(cmd, capture_output=True, text=True)
            # Show summary even on success
            if proc.stderr:
                lines = proc.stderr.strip().splitlines()
                for line in (lines[-2:] if proc.returncode == 0 else lines):
                    log.info("  etl| %s", line.strip())
        else:
            proc = subprocess.run(cmd, stdout=sys.stdout, stderr=sys.stderr)
        if proc.returncode != 0:
            raise RuntimeError(f"ethereum-etl exited with code {proc.returncode}")

    # ── S3 ───────────────────────────────────────────────────────────

    def _s3_key(self, table: str, date_str: str) -> str:
        return f"{self.prefix}/{table}/date={date_str}/{table}.parquet"

    def _upload(self, local_path: str, s3_key: str):
        local_size = os.path.getsize(local_path)
        log.info("Uploading s3://%s/%s (%.2f MB)",
                 self.bucket, s3_key, local_size / 1e6)
        self.s3.upload_file(local_path, self.bucket, s3_key,
                            Config=self.s3_transfer)
        remote_size = self.s3.head_object(
            Bucket=self.bucket, Key=s3_key)["ContentLength"]
        if remote_size != local_size:
            raise RuntimeError(
                f"Size mismatch {s3_key}: local={local_size} remote={remote_size}")
        log.info("Verified: %s", s3_key)

    def _s3_exists(self, s3_key: str) -> bool:
        try:
            self.s3.head_object(Bucket=self.bucket, Key=s3_key)
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] in ("404", "NoSuchKey"):
                return False
            raise

    def _date_on_s3(self, date_str: str) -> bool:
        return (self._s3_exists(self._s3_key("blocks", date_str))
                and self._s3_exists(self._s3_key("transactions", date_str)))

    # ── Export one date ──────────────────────────────────────────────

    def export_date(self, date: datetime, dry_run: bool = False,
                    force: bool = False):
        date_str = date.strftime("%Y-%m-%d")
        t0 = time.monotonic()

        if not dry_run and not force:
            if self.progress.is_done(date_str):
                log.info("[%s] Already done, skipping.", date_str)
                return
            if self._date_on_s3(date_str):
                log.info("[%s] Already on S3, skipping.", date_str)
                self.progress.mark_done(date_str)
                return

        tmp_dir = os.path.join(self.work_dir, date_str)
        os.makedirs(tmp_dir, exist_ok=True)

        try:
            start, end, ts_lo, ts_hi = self.find_block_range(date)
            ts_range = (ts_lo, ts_hi)

            blocks_csv = os.path.join(tmp_dir, "blocks.csv")
            txs_csv = os.path.join(tmp_dir, "transactions.csv")
            self._run_etl(start, end, blocks_csv, txs_csv)

            blocks_pq = os.path.join(tmp_dir, "blocks.parquet")
            txs_pq = os.path.join(tmp_dir, "transactions.parquet")
            csv_to_parquet(blocks_csv, blocks_pq, BLOCK_SCHEMA,
                           BLOCK_CSV_CONVERT, self.row_group_size,
                           "timestamp", ts_range)
            csv_to_parquet(txs_csv, txs_pq, TRANSACTION_SCHEMA,
                           TX_CSV_CONVERT, self.row_group_size,
                           "block_timestamp", ts_range)

            if dry_run:
                log.info("[%s] Dry run — files in %s", date_str, tmp_dir)
                return

            # txs first, blocks last as commit marker
            self._upload(txs_pq, self._s3_key("transactions", date_str))
            self._upload(blocks_pq, self._s3_key("blocks", date_str))
            self.progress.mark_done(date_str)

            log.info("[%s] Done in %.0fs", date_str, time.monotonic() - t0)
        except Exception:
            log.error("[%s] Failed", date_str, exc_info=True)
            raise
        finally:
            if not dry_run:
                shutil.rmtree(tmp_dir, ignore_errors=True)


# ── Parallel entry point ─────────────────────────────────────────────

def _export_one(config_path: str, date_str: str, dry_run: bool, force: bool):
    BSCExporter(load_config(config_path), parallel_mode=True).export_date(
        datetime.strptime(date_str, "%Y-%m-%d"),
        dry_run=dry_run, force=force)
    return date_str


# ── CLI ──────────────────────────────────────────────────────────────

def build_date_list(args) -> list[str]:
    if args.date and (args.start or args.end):
        raise SystemExit("Error: --date cannot be used with --start/--end")
    if bool(args.start) != bool(args.end):
        raise SystemExit("Error: --start and --end must be used together")
    if args.start:
        start = datetime.strptime(args.start, "%Y-%m-%d")
        end = datetime.strptime(args.end, "%Y-%m-%d")
        if start > end:
            raise SystemExit(f"Error: --start {args.start} is after --end {args.end}")
        dates, cur = [], start
        while cur <= end:
            dates.append(cur.strftime("%Y-%m-%d"))
            cur += timedelta(days=1)
        return dates
    if args.date:
        return [args.date]
    return [(datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")]


def main():
    p = argparse.ArgumentParser(
        description="BSC → Parquet → S3 exporter for AWS Public Blockchain",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python exporter.py --config config.yaml
  python exporter.py --config config.yaml --start 2020-08-29 --end 2026-04-29 -j4
  python exporter.py --config config.yaml --date 2024-06-15 --dry-run
        """,
    )
    p.add_argument("--config", required=True)
    p.add_argument("--date", help="Single date (YYYY-MM-DD)")
    p.add_argument("--start", help="Range start (YYYY-MM-DD)")
    p.add_argument("--end", help="Range end (YYYY-MM-DD)")
    p.add_argument("-j", "--parallel", type=int, default=1,
                   help="Parallel workers (default: 1)")
    p.add_argument("--dry-run", action="store_true",
                   help="Export locally only, skip S3")
    p.add_argument("--force", action="store_true",
                   help="Re-export and overwrite even if progress.txt or S3 says done")
    args = p.parse_args()

    dates = build_date_list(args)
    log.info("Exporting %d date(s): %s → %s", len(dates), dates[0], dates[-1])
    config_path = os.path.abspath(args.config)

    if args.parallel > 1 and len(dates) > 1:
        log.info("Parallel mode: %d workers", args.parallel)
        failed = []
        with ProcessPoolExecutor(max_workers=args.parallel) as pool:
            futs = {pool.submit(_export_one, config_path, d,
                                args.dry_run, args.force): d
                    for d in dates}
            for fut in as_completed(futs):
                d = futs[fut]
                try:
                    fut.result()
                    log.info("Completed: %s", d)
                except Exception as e:
                    log.error("Failed: %s — %s", d, e)
                    failed.append(d)
        if failed:
            failed.sort()
            log.error("Failed %d date(s): %s", len(failed), ", ".join(failed))
            sys.exit(1)
    else:
        exporter = BSCExporter(load_config(config_path))
        for d in dates:
            exporter.export_date(datetime.strptime(d, "%Y-%m-%d"),
                                 dry_run=args.dry_run, force=args.force)

    log.info("All done.")


if __name__ == "__main__":
    main()
