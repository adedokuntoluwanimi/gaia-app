# app/core/inference.py

import json
import boto3
from typing import List, Dict
from app.core.config import settings

runtime = boto3.client(
    "sagemaker-runtime",
    region_name=settings.AWS_REGION,
)


def infer_values(feature_rows: List[Dict]) -> List[float]:
    """
    Inference-only call.
    - No S3
    - No files
    - No job IDs
    """

    payload = {
        "instances": feature_rows
    }

    response = runtime.invoke_endpoint(
        EndpointName=settings.SAGEMAKER_ENDPOINT_NAME,
        ContentType="application/json",
        Body=json.dumps(payload),
    )

    result = json.loads(response["Body"].read().decode("utf-8"))
    return result["predictions"]
