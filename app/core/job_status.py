from pathlib import Path


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

    if (job_dir / "inference.json").exists():
        return "inferencing"

    if (job_dir / "train.csv").exists():
        return "processing"

    return "accepted"
