from pathlib import Path
import boto3
from app.core.config import settings

s3 = boto3.client("s3")
sagemaker_runtime = boto3.client("sagemaker-runtime")

ENDPOINT_NAME = "gaia-xgb-async-endpoint"


def upload_job_inputs(job_id: str) -> str:
    job_dir = Path("data") / job_id

    original_path = job_dir / "original.csv"
    train_path = job_dir / "train.csv"
    predict_path = job_dir / "predict.csv"

    for p in (original_path, train_path, predict_path):
        if not p.exists():
            raise RuntimeError(f"Missing required file: {p.name}")

    bucket = settings.S3_BUCKET

    try:
        print(f"[S3] Upload start | job_id={job_id}")

        s3.upload_file(
            str(original_path),
            bucket,
            f"jobs/{job_id}/input/original.csv",
        )

        s3.upload_file(
            str(train_path),
            bucket,
            f"jobs/{job_id}/input/train.csv",
        )

        s3.upload_file(
            str(predict_path),
            bucket,
            f"jobs/{job_id}/input/predict.csv",
        )

        s3_key = f"jobs/{job_id}/sagemaker/inference_input.csv"

        s3.upload_file(
            str(predict_path),
            bucket,
            s3_key,
        )

        print(f"[S3] Upload complete | job_id={job_id}")

        # -----------------------------
        # Async SageMaker invocation
        # -----------------------------
        input_location = f"s3://{bucket}/{s3_key}"

        response = sagemaker_runtime.invoke_endpoint_async(
            EndpointName=ENDPOINT_NAME,
            InputLocation=input_location,
            ContentType="text/csv",
        )

        inference_id = response["InferenceId"]

        print(f"[SM] Async inference submitted | job_id={job_id} | inference_id={inference_id}")

        return inference_id

    except Exception as e:
        print(f"[S3] Upload failed | job_id={job_id}")
        print(str(e))
        raise
