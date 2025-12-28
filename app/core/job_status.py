from pathlib import Path
import json
import boto3

s3 = boto3.client("s3")

ASYNC_OUTPUT_PREFIX = "jobs/async-output/"
BUCKET = "gaia-ml-dev"


def job_status(job_id: str) -> str:
    job_dir = Path("data") / job_id

    if not job_dir.exists():
        return "not_found"

    if (job_dir / "error.json").exists():
        return "failed"

    if (job_dir / "final.csv").exists():
        return "complete"

    if (job_dir / "predictions.csv").exists():
        return "merging"

    # -------------------------
    # Async inference handling
    # -------------------------
    inference_file = job_dir / "inference.json"
    if inference_file.exists():
        with open(inference_file) as f:
            inference_id = json.load(f)["inference_id"]

        resp = s3.list_objects_v2(
            Bucket=BUCKET,
            Prefix=ASYNC_OUTPUT_PREFIX,
        )

        if "Contents" in resp:
            keys = [obj["Key"] for obj in resp["Contents"]]

            if f"{ASYNC_OUTPUT_PREFIX}{inference_id}.error" in keys:
                return "failed"

            if f"{ASYNC_OUTPUT_PREFIX}{inference_id}.out" in keys:
                return "completed_inference"

        return "inferencing"

    if (job_dir / "train.csv").exists():
        return "processing"

    return "accepted"
