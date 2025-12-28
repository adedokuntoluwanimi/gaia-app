# --------------------------------------------------
# Standard library imports
# --------------------------------------------------
import uuid
import csv
import json
from pathlib import Path
from typing import Optional

# --------------------------------------------------
# FastAPI imports
# --------------------------------------------------
from fastapi import (
    APIRouter,
    UploadFile,
    File,
    Form,
    HTTPException,
    Path as ApiPath,
)
from fastapi.responses import FileResponse

# --------------------------------------------------
# Geometry engine imports
# --------------------------------------------------
from app.core.geometry import (
    build_canonical_stations_sparse,
    split_train_predict,
)

# --------------------------------------------------
# Core pipeline imports
# --------------------------------------------------
from app.core.s3_io import upload_job_inputs, download_predictions
from app.core.sagemaker_async import trigger_inference
from app.core.merge import merge_job_results
from app.core.job_status import job_status

# --------------------------------------------------
# Internal schema imports
# --------------------------------------------------
from app.schemas.job import JobResponse, Scenario

router = APIRouter()


# ==================================================
# POST /jobs
# ==================================================
@router.post("", response_model=JobResponse)
async def create_job(
    scenario: Scenario = Form(...),
    x_column: str = Form(...),
    y_column: str = Form(...),
    value_column: Optional[str] = Form(None),
    output_spacing: Optional[float] = Form(None),
    csv_file: UploadFile = File(...),
):
    # --------------------------------------------------
    # 1. Validate scenario
    # --------------------------------------------------
    if scenario == Scenario.sparse_only:
        if value_column is None or output_spacing is None:
            raise HTTPException(
                status_code=400,
                detail="sparse_only requires value_column and output_spacing",
            )

    if scenario == Scenario.explicit_geometry and output_spacing is not None:
        raise HTTPException(
            status_code=400,
            detail="explicit_geometry must not define output_spacing",
        )

    # --------------------------------------------------
    # 2. Create job workspace
    # --------------------------------------------------
    job_id = f"gaia-{uuid.uuid4().hex[:12]}"
    job_dir = Path("data") / job_id
    job_dir.mkdir(parents=True, exist_ok=True)

    # --------------------------------------------------
    # 3. Save uploaded CSV (MANDATORY NAME)
    # --------------------------------------------------
    original_csv = job_dir / "original.csv"
    with original_csv.open("wb") as f:
        f.write(await csv_file.read())

    # --------------------------------------------------
    # 4. Normalize headers
    # --------------------------------------------------
    with original_csv.open("r", encoding="utf-8", errors="ignore") as f:
        reader = csv.reader(f)
        header = next(reader)

    normalized = {h.strip().lower(): h for h in header}

    def norm(s: str) -> str:
        return s.strip().lower()

    required = {norm(x_column), norm(y_column)}
    if scenario == Scenario.sparse_only:
        required.add(norm(value_column))

    missing = required - normalized.keys()
    if missing:
        raise HTTPException(
            status_code=400,
            detail=f"Missing column(s): {', '.join(missing)}",
        )

    x_col = normalized[norm(x_column)]
    y_col = normalized[norm(y_column)]
    v_col = normalized[norm(value_column)] if value_column else None

    # --------------------------------------------------
    # 5. Load rows
    # --------------------------------------------------
    with original_csv.open("r", encoding="utf-8", errors="ignore") as f:
        rows = list(csv.DictReader(f))

    if not rows:
        raise HTTPException(status_code=400, detail="CSV has no data")

    train_path = job_dir / "train.csv"
    predict_path = job_dir / "predict.csv"

    # ==================================================
    # 6. Sparse-only scenario
    # ==================================================
    if scenario == Scenario.sparse_only:
        measured = []

        for r in rows:
            try:
                measured.append({
                    "x": float(r[x_col]),
                    "y": float(r[y_col]),
                    "value": float(r[v_col]),
                })
            except (TypeError, ValueError):
                continue

        if not measured:
            raise HTTPException(
                status_code=400,
                detail="No valid measured points found",
            )

        canonical = build_canonical_stations_sparse(
            measured_points=measured,
            spacing=output_spacing,
        )

        train_rows, predict_rows = split_train_predict(canonical)

        with train_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(
                f, fieldnames=["x", "y", "d_along", "value"]
            )
            writer.writeheader()
            writer.writerows(train_rows)

        with predict_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(
                f, fieldnames=["x", "y", "d_along", "value"]
            )
            writer.writeheader()
            writer.writerows(predict_rows)

    # ==================================================
    # 7. Explicit geometry scenario
    # ==================================================
    else:
        if v_col is None:
            raise HTTPException(
                status_code=400,
                detail="explicit_geometry requires value_column",
            )

        train_rows = []
        predict_rows = []

        for r in rows:
            try:
                x = float(r[x_col])
                y = float(r[y_col])
            except (TypeError, ValueError):
                continue

            val = r.get(v_col)
            if val in ("", None):
                predict_rows.append({"x": x, "y": y, "value": None})
            else:
                try:
                    train_rows.append({
                        "x": x,
                        "y": y,
                        "value": float(val),
                    })
                except ValueError:
                    continue

        with train_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["x", "y", "value"])
            writer.writeheader()
            writer.writerows(train_rows)

        with predict_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["x", "y", "value"])
            writer.writeheader()
            writer.writerows(predict_rows)

    # --------------------------------------------------
    # 8. Upload inputs to S3
    # --------------------------------------------------
    upload_job_inputs(job_id)

    # --------------------------------------------------
    # 9. Trigger SageMaker inference
    # --------------------------------------------------
    try:
        sm_job_name = trigger_inference(job_id)
    except Exception as e:
        with (job_dir / "error.json").open("w") as f:
            json.dump(
                {"stage": "inference_trigger", "error": str(e)},
                f,
                indent=2,
            )
        raise HTTPException(
            status_code=500,
            detail="Failed to trigger SageMaker inference",
        )

    with (job_dir / "inference.json").open("w") as f:
        json.dump(
            {
                "job_id": job_id,
                "sagemaker_job_name": sm_job_name,
            },
            f,
            indent=2,
        )

    return JobResponse(job_id=job_id, status="inferencing")


# ==================================================
# GET /jobs/{job_id}/preview
# ==================================================
@router.get("/{job_id}/preview")
def preview_geometry(job_id: str = ApiPath(...)):
    job_dir = Path("data") / job_id
    train_path = job_dir / "train.csv"
    predict_path = job_dir / "predict.csv"

    if not train_path.exists():
        raise HTTPException(status_code=404, detail="Job not found")

    def read_points(path: Path):
        pts = []
        with path.open("r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for i, r in enumerate(reader):
                pts.append({
                    "x": float(r["x"]),
                    "y": float(r["y"]),
                    "station_index": i,
                })
        return pts

    return {
        "measured": read_points(train_path),
        "generated": read_points(predict_path) if predict_path.exists() else [],
    }


# ==================================================
# Auto-merge helper
# ==================================================
def try_merge(job_id: str):
    job_dir = Path("data") / job_id

    if (job_dir / "final.csv").exists():
        return

    if (job_dir / "error.json").exists():
        return

    try:
        download_predictions(job_id)
        merge_job_results(job_id)
    except Exception:
        return


# ==================================================
# GET /jobs/{job_id}/status
# ==================================================
@router.get("/{job_id}/status")
def get_job_status(job_id: str):
    try_merge(job_id)
    return {
        "job_id": job_id,
        "status": job_status(job_id),
    }


# ==================================================
# GET /jobs/{job_id}/result
# ==================================================
@router.get("/{job_id}/result")
def get_final_result(job_id: str):
    final_path = Path("data") / job_id / "final.csv"

    if not final_path.exists():
        raise HTTPException(status_code=404, detail="Result not ready")

    return FileResponse(
        path=final_path,
        filename="final.csv",
        media_type="text/csv",
    )
