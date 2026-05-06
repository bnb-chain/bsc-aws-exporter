#!/usr/bin/env python3
"""
Delete date partitions from the export bucket whose date is BEFORE a cutoff.

Use case: re-running historical backfill after a fix. Defaults to deleting
everything < 2025-04-29 (Lorentz fork) under bnb/blocks/ and bnb/transactions/.

Usage:
    python scripts/delete_bucket_dates.py --config config.yaml \
        [--cutoff 2025-04-29] [--prefix bnb] [--yes]

Without --yes it prints what would be deleted and asks for confirmation.
"""

import argparse
import sys
import yaml
import boto3
from botocore.config import Config as BotoConfig


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--config", required=True)
    p.add_argument("--cutoff", default="2025-04-29",
                   help="Delete dates < this (YYYY-MM-DD). Default: 2025-04-29 (Lorentz)")
    p.add_argument("--prefix", default=None,
                   help="Override s3.prefix from config (e.g. 'bnb')")
    p.add_argument("--tables", default="blocks,transactions",
                   help="Comma-separated table names to clean")
    p.add_argument("--yes", action="store_true",
                   help="Skip confirmation prompt")
    args = p.parse_args()

    with open(args.config) as f:
        cfg = yaml.safe_load(f)
    s3_cfg = cfg["s3"]
    bucket = s3_cfg["bucket"]
    prefix = args.prefix or s3_cfg.get("prefix", "v1.1/bnb")
    tables = [t.strip() for t in args.tables.split(",") if t.strip()]

    # Build boto3 client (mirrors exporter.py logic)
    s3_options = {}
    if "addressing_style" in s3_cfg:
        s3_options["addressing_style"] = s3_cfg["addressing_style"]
    if s3_cfg.get("endpoint_url"):
        s3_options.setdefault("addressing_style", "path")
        s3_options["use_accelerate_endpoint"] = False
    boto_args = {"max_pool_connections": 10,
                 "retries": {"max_attempts": 3, "mode": "adaptive"}}
    if s3_options:
        boto_args["s3"] = s3_options

    creds = {}
    ak, sk = s3_cfg.get("access_key_id"), s3_cfg.get("secret_access_key")
    if ak and sk:
        creds["aws_access_key_id"] = ak
        creds["aws_secret_access_key"] = sk

    s3 = boto3.client(
        "s3", region_name=s3_cfg.get("region", "us-east-2"),
        endpoint_url=s3_cfg.get("endpoint_url"),
        config=BotoConfig(**boto_args),
        **creds,
    )

    # List candidate keys
    keys = []
    for table in tables:
        list_prefix = f"{prefix}/{table}/"
        print(f"Scanning s3://{bucket}/{list_prefix}...", file=sys.stderr)
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=list_prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                # extract YYYY-MM-DD from path component "date=YYYY-MM-DD"
                date = next((p[5:] for p in key.split("/") if p.startswith("date=")), None)
                if date and date < args.cutoff:
                    keys.append(key)

    if not keys:
        print(f"Nothing to delete (no keys with date < {args.cutoff}).")
        return

    keys.sort()
    print(f"\nFound {len(keys)} object(s) to delete (date < {args.cutoff}):")
    print(f"  first: {keys[0]}")
    print(f"  last:  {keys[-1]}")

    if not args.yes:
        ans = input(f"\nDelete {len(keys)} objects from s3://{bucket}/? (type 'yes'): ")
        if ans.strip() != "yes":
            print("Aborted.")
            return

    # Batch delete (1000 keys / call)
    deleted = 0
    failed = 0
    for i in range(0, len(keys), 1000):
        batch = keys[i:i + 1000]
        resp = s3.delete_objects(
            Bucket=bucket,
            Delete={"Objects": [{"Key": k} for k in batch], "Quiet": True},
        )
        errors = resp.get("Errors", [])
        for err in errors[:3]:
            print(f"  ERROR: {err.get('Key')}: {err.get('Message')}", file=sys.stderr)
        deleted += len(batch) - len(errors)
        failed += len(errors)
        print(f"  deleted {deleted}/{len(keys)}", file=sys.stderr)

    print(f"\nDone. deleted={deleted} failed={failed}")
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
