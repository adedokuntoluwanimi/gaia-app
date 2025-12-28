from fastapi import APIRouter, UploadFile, File, HTTPException
import csv
import io
import uuid

from app.core.s3_io import upload_csv
from app.core.sagemaker_async import trigger_batch_inference

router = APIRouter(prefix="/jobs", tags=["jobs"])


@router.post("")
async def create_job(file: UploadFile = File(...)):
    job_id = f"gaia-{uuid.uuid4().hex[:12]}"

    # ----------------------------------
    # Read CSV
    # ----------------------------------
    content = await file.read()
    stream = io.StringIO(content.decode("utf-8"))
    reader = csv.DictReader(stream)

    rows = list(reader)
    if not rows:
        raise HTTPException(status_code=400, detail="Empty CSV")

    # ----------------------------------
    # Split train vs predict
    # ----------------------------------
    train_rows = []
    predict_rows = []

    for r in rows:
        if r.get("value") not in (None, "", "NaN"):
            train_rows.append(r)
        else:
            predict_rows.append(r)

    if not train_rows:
        raise HTTPException(
            status_code=400,
            detail="No training rows found",
        )

    if not predict_rows:
        raise HTTPException(
            status_code=400,
            detail="No rows require prediction",
        )

    # ----------------------------------
    # Write inputs to S3
    # ----------------------------------
    train_key = f"jobs/{job_id}/input/train.csv"
    predict_key = f"jobs/{job_id}/input/predict.csv"

    upload_csv(train_key, train_rows)
    upload_csv(predict_key, predict_rows)

    # ----------------------------------
    # Trigger SageMaker endpoints inference
    # ----------------------------------
    trigger_batch_inference(
    job_id=job_id,
    input_s3_key=predict_key,
    output_s3_key=f"jobs/{job_id}/output/predictions.csv",
)


    # ----------------------------------
    # Return job reference
    # ----------------------------------
    return {
        "job_id": job_id,
        "status": "submitted",
    }
