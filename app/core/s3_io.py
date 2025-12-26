import boto3
from pathlib import Path
from app.core.config import settings

s3 = boto3.client("s3", region_name=settings.AWS_REGION)


def upload_job_inputs(job_id: str):
    """
    Upload train.csv and predict.csv for a job to S3.
    """
    job_dir = Path("data") / job_id

    train_path = job_dir / "train.csv"
    predict_path = job_dir / "predict.csv"

    if not train_path.exists():
        raise FileNotFoundError(train_path)

    if not predict_path.exists():
        raise FileNotFoundError(predict_path)

    s3.upload_file(
        str(train_path),
        settings.S3_BUCKET,
        f"jobs/{job_id}/input/train.csv",
    )

    s3.upload_file(
        str(predict_path),
        settings.S3_BUCKET,
        f"jobs/{job_id}/input/predict.csv",
    )


def download_predictions(job_id: str):
    """
    Download predictions.csv from S3 into local job directory.
    """
    job_dir = Path("data") / job_id
    job_dir.mkdir(parents=True, exist_ok=True)

    local_path = job_dir / "predictions.csv"

    s3.download_file(
        settings.S3_BUCKET,
        f"jobs/{job_id}/output/predictions.csv",
        str(local_path),
    )

    return local_path
