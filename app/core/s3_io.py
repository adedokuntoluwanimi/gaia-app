import boto3
from pathlib import Path
from app.core.config import settings

# --------------------------------------------------
# S3 client
# --------------------------------------------------
s3 = boto3.client(
    "s3",
    region_name=settings.AWS_REGION,
)

# --------------------------------------------------
# Upload job inputs to S3
# --------------------------------------------------
def upload_job_inputs(job_id: str):
    """
    Upload all job inputs to S3.

    Expected local structure:
    data/<job_id>/
      ├── original.csv
      ├── train.csv
      └── predict.csv
    """

    job_dir = Path("data") / job_id

    original_path = job_dir / "original.csv"
    train_path = job_dir / "train.csv"
    predict_path = job_dir / "predict.csv"

    assert original_path.exists(), "original.csv missing before S3 upload"
    assert train_path.exists(), "train.csv missing before S3 upload"
    assert predict_path.exists(), "predict.csv missing before S3 upload"

    bucket = settings.S3_BUCKET

    print(f"[S3] Upload start | job_id={job_id}")
    print(f"[S3] Bucket={bucket}, Region={settings.AWS_REGION}")

    # -----------------------------
    # Raw inputs (traceability)
    # -----------------------------
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

    # -----------------------------
    # SageMaker inference contract
    # -----------------------------
    # SageMaker reads ONLY this file
    s3.upload_file(
        str(predict_path),
        bucket,
        f"jobs/{job_id}/sagemaker/inference_input.csv",
    )

    print(f"[S3] Upload complete | job_id={job_id}")


# --------------------------------------------------
# Download SageMaker predictions
# --------------------------------------------------
def download_predictions(job_id: str) -> Path:
    """
    Download SageMaker inference output.

    Expected S3 object:
    jobs/<job_id>/sagemaker/inference_output.csv
    """

    job_dir = Path("data") / job_id
    job_dir.mkdir(parents=True, exist_ok=True)

    local_path = job_dir / "predictions.csv"

    s3.download_file(
        settings.S3_BUCKET,
        f"jobs/{job_id}/sagemaker/inference_output.csv",
        str(local_path),
    )

    print(f"[S3] Downloaded predictions | job_id={job_id}")

    return local_path
