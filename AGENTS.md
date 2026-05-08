# AGENTS.md

Single-file Python tool. Edit `exporter.py`, run dry-run, verify Parquet, ship.

## Setup

```bash
python -m venv venv && source venv/bin/activate   # use Python 3.10
pip install -r requirements.txt
# On Python 3.11+: also `pip install --no-deps 'parsimonious>=0.10.4'`
cp config.yaml.example config.yaml && chmod 600 config.yaml
# Edit rpc_url, s3.bucket, s3.endpoint_url, s3.access_key_id/secret_access_key
```

## Verifying any change

```bash
# 1. Syntax
python -c "import ast; ast.parse(open('exporter.py').read())"

# 2. Dry-run on three eras (catches most regressions: pre-Lorentz drift, post-Lorentz, post-Fermi)
python exporter.py --config config.yaml --date 2021-04-03 --dry-run
python exporter.py --config config.yaml --date 2025-05-15 --dry-run
python exporter.py --config config.yaml --date 2026-02-01 --dry-run

# 3. Inspect output: rows ≈ 86400 / interval_ms * 1000, first_ts ≈ midnight UTC
python -c "
import pyarrow.parquet as pq, pyarrow.compute as pc
from datetime import datetime, timezone
t = pq.read_table('work/2021-04-03/blocks.parquet')
mn = pc.min(t['timestamp']).as_py()
mx = pc.max(t['timestamp']).as_py()
print(f'rows={len(t)} first={datetime.fromtimestamp(mn, tz=timezone.utc)} last={datetime.fromtimestamp(mx, tz=timezone.utc)}')
"
```

There is no test suite. The above three commands ARE the test suite.

## Code map (`exporter.py`)

| Symbol | Lines | Purpose |
|--------|-------|---------|
| `BSC_FORKS` | top of file | `(activation_ts, interval_ms)` per BSC mainnet hardfork. Append when new fork changes interval. |
| `BLOCK_SCHEMA` / `TRANSACTION_SCHEMA` | top of file | Parquet schemas. Match AWS Public Blockchain ETH dataset. Large ints as `string`. |
| `_make_csv_convert(schema)` | helper | Auto-derives pyarrow CSV ConvertOptions from schema. Includes ALL columns (don't filter strings). |
| `estimate_block(ts)` | helper | ts → block#, walks `BSC_FORKS` segments. Theoretical only; real intervals drift. |
| `csv_to_parquet(...)` | helper | CSV → Parquet + optional ts-range filter for exact day boundaries. |
| `ProgressFile` | class | `progress.txt` with in-memory cache + flock for parallel safety. |
| `BSCExporter.__init__` | class | Web3 + PoA middleware + boto3 (handles R2 endpoint_url + inline creds). |
| `BSCExporter._bisect_block` | method | Binary search by ts via RPC. |
| `BSCExporter.find_block_range` | method | Day → block range. **Has a defensive verify loop — see Invariant #2.** |
| `BSCExporter.export_date` | method | Full pipeline. Skip checks → fetch → filter → upload (txs first, blocks last) → mark done. |
| `main()` | bottom | Argparse + ProcessPool dispatch. |

## Invariants — DO NOT REMOVE / REVERT

### #1 PoA middleware injection
```python
self.w3.middleware_onion.inject(geth_poa_middleware, layer=0)
```
BSC `extraData` is ~500 bytes (validator sigs); web3 v5 default rejects >32 bytes.
Removing this breaks every `eth.get_block` call.

### #2 Defensive bisect verify in `find_block_range`
The `while start > 0: prev_ts = ... if prev_ts < day_start_ts: break ...` loop is **correctness-critical**.

`estimate_block` walks fork segments at theoretical intervals. Real BSC drifted (e.g., 2021 Q1 ran ~5s/block under congestion vs 3s target). Drift can exceed `ESTIMATE_MARGIN` (4000 blocks), making the bisect lower bound `est - MARGIN` itself past the true day-start. Bisect then bottoms at `lo` and returns silently wrong answer; earlier-but-still-in-day blocks are missed.

Symptom of regression: Parquet for a historic date has `first_ts` hours after midnight UTC and far fewer rows than expected.

### #3 `end_block` is over-fetched, not bisected
```python
end = min(latest, estimate_block(day_end_ts) + ESTIMATE_MARGIN)
```
The timestamp filter in `csv_to_parquet` trims the over-fetch. **Don't replace with bisect** — this is intentional and avoids the off-by-one ambiguity at "first block of next day".

### #4 Upload order: transactions first, blocks last
`blocks.parquet` is the commit marker. `_date_on_s3` requires both files. A crashed mid-upload leaves the date "incomplete" so `--force`-less reruns redo it; downstream consumers keying off `blocks.parquet` never see a half-written partition.

### #5 String types for big integers
`value`, `difficulty`, `total_difficulty`, `nonce`, `input` are `pa.string()` in the schemas. BSC values can exceed int64. Don't change to numeric.

### #6 `_make_csv_convert` covers all columns
```python
return pv.ConvertOptions(column_types={f.name: f.type for f in schema}, ...)
```
Don't filter to non-string columns. pyarrow's CSV inference can mishandle huge integer-shaped strings.

## Common edits and how to do them safely

### Adding a new BSC hardfork
1. Append `(timestamp, interval_ms)` to `BSC_FORKS`. Source the timestamp from upstream `bsc/params/config.go` (BSCChainConfig).
2. Run the three dry-runs above + one date past the new fork.
3. No other change needed; `estimate_block` walks the list.

### Adding a new column to the Parquet schema
1. Edit `BLOCK_SCHEMA` or `TRANSACTION_SCHEMA`.
2. `_make_csv_convert` auto-picks up the type. CSV missing the column → filled with null in `csv_to_parquet`.
3. Coordinate with downstream consumers BEFORE merging. AWS dataset schema is the canonical reference for naming/typing.

### Adding a new S3-compatible backend
Already supported: `endpoint_url`, `addressing_style`, inline `access_key_id`/`secret_access_key`. If a new backend needs more boto3 client kwargs, extend the `creds` / `s3_options` dict path in `BSCExporter.__init__` — keep AWS S3 working without those kwargs.

### Speeding up backfill
Don't add async/aiohttp — `ethereum-etl` is the bottleneck and is sync. Levers that work:
- `-j N` parallelism (each worker is a separate process)
- `export.ethereumetl_workers` (RPC concurrency per worker)
- `export.ethereumetl_batch_size` — lower if BSC node returns `response too large (-32003)`

## Things agents commonly try and shouldn't

- **Replacing bisect with `eth.get_block_by_timestamp`** — that JSON-RPC method does not exist on geth/bsc-geth.
- **Removing `--force`** — it's needed when correctness fixes invalidate previously-uploaded data; without it, the S3 skip check defeats the rerun.
- **Adding async / asyncio** — pulls in `web3>=6` which conflicts with ethereum-etl 2.x's pin on `web3<6`.
- **Caching `latest_block` forever** — currently caches with `LATEST_BLOCK_TTL=300`. Removing the TTL breaks long-running single-process backfills (tip drifts past the cached value, dates near "today" misclassify).
- **Tightening `load_config` to require new fields** — config files are deployed across machines; backwards-compat matters.
- **Adding write-time SHA256 checksum verification** — boto3 multipart uploads produce composite checksums that don't equal a full-file SHA. Size check is intentional minimal verification.
- **Refactoring single-file `exporter.py` into a package** — operational simplicity (one file, one venv, one systemd unit) is a feature.

## Failure-mode → action lookup

| Log line | Cause | Fix |
|----------|-------|-----|
| `extraData is N bytes, but should be 32` | Invariant #1 broken | Restore `geth_poa_middleware.inject` |
| `Filtered ... → very few rows` for healthy day | Invariant #2 broken | Restore defensive verify loop |
| `Cannot connect to RPC` mid-run | Node crash or `-j × etl_workers` overload | Lower parallelism; check node |
| `response too large (-32003)` | Node's batch response cap | Lower `ethereumetl_batch_size` |
| `(400) HeadObject Bad Request` on R2 | Missing `endpoint_url`/`addressing_style`, or wrong region | Fix config |
| `cannot import name 'getargspec'` | Python 3.11+ with old parsimonious | `pip install --no-deps 'parsimonious>=0.10.4'` |
| `Size mismatch` on upload | S3 object got truncated mid-upload | Re-run; multipart will reupload cleanly |

## Out of scope

- Decoding events / ABIs (use a separate consumer downstream)
- Receipts / state / traces (only blocks + transactions)
- Non-BSC chains (BSC_FORKS is mainnet-specific; fork timestamps differ on Chapel testnet)
- Daemon / always-running mode (oneshot + systemd timer is the design)
