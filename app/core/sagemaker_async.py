import csv
import io
import json
import math
import boto3
from typing import List, Dict

from app.core.config import settings
from app.core.s3_io import download_csv, upload_csv


runtime = boto3.client(
    "sagemaker-runtime",
    region_name=settings.AWS_REGION,
)


def _chunks(items: List[Dict], size: int):
    for i in range(0, len(items), size):
        yield items[i:i + size]


def trigger_inference_via_endpoint(
    job_id: str,
    input_s3_key: str,
    output_s3_key: str,
    batch_size: int = 100,
):
    """
    Endpoint-based inference with S3 IO.

    Flow:
    S3 predict.csv
      → endpoint inference
      → S3 predictions.csv
    """

    # ----------------------------------
    # Load predict rows from S3
    # ----------------------------------
    predict_rows = download_csv(input_s3_key)
    if not predict_rows:
        raise RuntimeError("Predict CSV is empty")

    # ----------------------------------
    # Build feature payload
    # ----------------------------------
    features = []
    for r in predict_rows:
        features.append({
            "x": float(r["x"]),
            "y": float(r["y"]),
        })

    # ----------------------------------
    # Call endpoint in chunks
    # ----------------------------------
    predictions: List[float] = []

    for batch in _chunks(features, batch_size):
        payload = json.dumps({"instances": batch})

        response = runtime.invoke_endpoint(
            EndpointName=settings.SAGEMAKER_ENDPOINT_NAME,
            ContentType="application/json",
            Body=payload,
        )

        result = json.loads(
            response["Body"].read().decode("utf-8")
        )

        predictions.extend(result["predictions"])

    if len(predictions) != len(predict_rows):
        raise RuntimeError("Prediction count mismatch")

    # ----------------------------------
    # Attach predictions
    # ----------------------------------
    output_rows = []

    for row, value in zip(predict_rows, predictions):
        row_out = dict(row)
        row_out["value"] = value
        row_out["measured"] = 0
        output_rows.append(row_out)

    # ----------------------------------
    # Write predictions to S3
    # ----------------------------------
    upload_csv(output_s3_key, output_rows)
