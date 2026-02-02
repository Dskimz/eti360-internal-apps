from __future__ import annotations

import os
from dataclasses import dataclass

import boto3


@dataclass(frozen=True)
class S3Config:
    region: str
    bucket: str
    prefix: str


def get_s3_config() -> S3Config:
    region = os.environ.get("AWS_REGION", "").strip()
    bucket = os.environ.get("S3_BUCKET", "").strip()
    prefix = os.environ.get("S3_PREFIX", "").strip()

    if not region:
        raise RuntimeError("AWS_REGION is not set")
    if not bucket:
        raise RuntimeError("S3_BUCKET is not set")
    if prefix and not prefix.endswith("/"):
        prefix = prefix + "/"

    return S3Config(region=region, bucket=bucket, prefix=prefix)


def s3_client(*, region: str):
    return boto3.client("s3", region_name=region)


def put_png(*, region: str, bucket: str, key: str, body: bytes) -> None:
    client = s3_client(region=region)
    client.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType="image/png",
        CacheControl="public, max-age=31536000",
    )


def presign_get(*, region: str, bucket: str, key: str, expires_in: int = 3600) -> str:
    client = s3_client(region=region)
    return client.generate_presigned_url(
        ClientMethod="get_object",
        Params={"Bucket": bucket, "Key": key},
        ExpiresIn=expires_in,
    )
