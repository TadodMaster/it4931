"""Create the S3 buckets listed in config (idempotent). Waits for the object store to accept requests."""
from __future__ import annotations

import os
import sys
import time

import boto3
from botocore.config import Config

from .config import load_config


def main() -> int:
    cfg = load_config()["s3"]
    s3 = boto3.client("s3", endpoint_url=cfg["endpoint"], region_name=cfg["region"],
                      aws_access_key_id=os.environ["S3_ACCESS_KEY"], aws_secret_access_key=os.environ["S3_SECRET_KEY"],
                      config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 3}))
    deadline = time.time() + 120
    while True:
        try:
            have = {b["Name"] for b in s3.list_buckets()["Buckets"]}
            break
        except Exception as e:  # noqa: BLE001 - startup race, retry
            if time.time() > deadline:
                print(f"object store not reachable: {e}", file=sys.stderr)
                return 1
            time.sleep(2)
    for b in cfg["buckets"]:
        if b not in have:
            s3.create_bucket(Bucket=b)
            print(f"[init-buckets] created s3://{b}", flush=True)
        else:
            print(f"[init-buckets] exists  s3://{b}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
