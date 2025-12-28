import csv
import io
import boto3
from typing import List, Dict

from app.core.config import settings

s3 = boto3.client(
    "s3",
    region_name=settings.AWS_REGION,
)


def upload_csv(s3_key: str, rows: List[Dict]):
    """
    Upload a list of dict rows as CSV to S3.
    """
    if not rows:
        raise ValueError("No rows to upload")

    buffer = io.StringIO()
    writer = csv.DictWriter(
        buffer,
        fieldnames=rows[0].keys(),
    )
    writer.writeheader()
    writer.writerows(rows)

    s3.put_object(
        Bucket=settings.S3_BUCKET,
        Key=s3_key,
        Body=buffer.getvalue().encode("utf-8"),
        ContentType="text/csv",
    )


def download_csv(s3_key: str) -> List[Dict]:
    """
    Download a CSV from S3 and return rows as dicts.
    """
    response = s3.get_object(
        Bucket=settings.S3_BUCKET,
        Key=s3_key,
    )

    body = response["Body"].read().decode("utf-8")
    stream = io.StringIO(body)
    reader = csv.DictReader(stream)

    return list(reader)
