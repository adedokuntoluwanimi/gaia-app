# app/core/sagemaker_async.py

import boto3
from app.core.config import settings

runtime = boto3.client(
    "sagemaker-runtime",
    region_name=settings.AWS_REGION,
)

ASYNC_ENDPOINT_NAME = "gaia-magnetics-async"


def trigger_inference(job_id: str) -> str:
    """
    Invoke SageMaker async endpoint.
    Input is already in S3.
    Output will be written by SageMaker to S3.
    """

    input_s3 = (
        f"s3://{settings.S3_BUCKET}/jobs/{job_id}/sagemaker/inference_input.csv"
    )

    response = runtime.invoke_endpoint_async(
        EndpointName=ASYNC_ENDPOINT_NAME,
        InputLocation=input_s3,
        ContentType="text/csv",
    )

    # SageMaker async returns an inference ID
    inference_id = response["InferenceId"]

    return inference_id
