# --------------------------------------------------
# Standard library imports
# --------------------------------------------------
import uuid
from pathlib import Path
import csv
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

# --------------------------------------------------
# Geometry engine imports
# --------------------------------------------------
from app.core.geometry import (
    build_canonical_stations_sparse,
    split_train_predict,
)

# --------------------------------------------------
# Internal schema imports
# --------------------------------------------------
from app.schemas.job import JobResponse, Scenario

# --------------------------------------------------
# Router definition
# --------------------------------------------------
router = APIRouter()


# ==================================================
# POST /jobs  → create a GAIA job
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
    # 1. Scenario enforcement
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
    # 2. Job ID + workspace
    # --------------------------------------------------
    job_id = f"gaia-{uuid.uuid4().hex[:12]}"
    job_dir = Path("data") / job_id
    job_dir.mkdir(parents=True, exist_ok=True)

    # --------------------------------------------------
    # 3. Save raw CSV
    # --------------------------------------------------
    raw_csv_path = job_dir / csv_file.filename
    with open(raw_csv_path, "wb") as f:
        f.write(await csv_file.read())

    # --------------------------------------------------
    # 4. Read CSV header (encoding tolerant)
    # --------------------------------------------------
    try:
        with open(raw_csv_path, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            header = next(reader)
    except UnicodeDecodeError:
        with open(raw_csv_path, "r", encoding="latin-1") as f:
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
            detail=f"Missing required column(s): {', '.join(missing)}",
        )

    x_col = normalized[norm(x_column)]
    y_col = normalized[norm(y_column)]
    v_col = normalized[norm(value_column)] if value_column else None

    selected_fields = [x_col, y_col]
    if v_col:
        selected_fields.append(v_col)

    # --------------------------------------------------
    # 5. Load selected columns only
    # --------------------------------------------------
    rows = []

    try:
        with open(raw_csv_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for r in reader:
                rows.append({k: r[k] for k in selected_fields})
    except UnicodeDecodeError:
        with open(raw_csv_path, "r", encoding="latin-1") as f:
            reader = csv.DictReader(f)
            for r in reader:
                rows.append({k: r[k] for k in selected_fields})

    if not rows:
        raise HTTPException(status_code=400, detail="CSV has no data rows")

    # --------------------------------------------------
    # 6. Write selected input snapshot
    # --------------------------------------------------
    input_selected = job_dir / "input_selected.csv"
    with open(input_selected, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=selected_fields)
        writer.writeheader()
        writer.writerows(rows)

    train_path = job_dir / "train.csv"
    predict_path = job_dir / "predict.csv"

    # --------------------------------------------------
    # 7. Sparse-only geometry + train/predict split
    # --------------------------------------------------
    if scenario == Scenario.sparse_only:
        measured_points = []

        for r in rows:
            measured_points.append({
                "x": float(r[x_col]),
                "y": float(r[y_col]),
                "value": float(r[v_col]),
            })

        canonical = build_canonical_stations_sparse(
            measured_points=measured_points,
            spacing=output_spacing,
        )

        train_rows, predict_rows = split_train_predict(canonical)

        with open(train_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=["x", "y", "d_along", "value"],
            )
            writer.writeheader()
            writer.writerows(train_rows)

        with open(predict_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=["x", "y", "d_along", "value"],
            )
            writer.writeheader()
            writer.writerows(predict_rows)

    # --------------------------------------------------
    # 8. Explicit-geometry split (no geometry generation)
    # --------------------------------------------------
    else:
        train_rows = []
        predict_rows = []

        for r in rows:
            if r[v_col] in ("", None):
                predict_rows.append({
                    "x": float(r[x_col]),
                    "y": float(r[y_col]),
                    "value": None,
                })
            else:
                train_rows.append({
                    "x": float(r[x_col]),
                    "y": float(r[y_col]),
                    "value": float(r[v_col]),
                })

        for path, data in [(train_path, train_rows), (predict_path, predict_rows)]:
            with open(path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(
                    f,
                    fieldnames=["x", "y", "value"],
                )
                writer.writeheader()
                writer.writerows(data)

    return JobResponse(job_id=job_id, status="accepted")


# ==================================================
# GET /jobs/{job_id}/preview
# ==================================================
@router.get("/{job_id}/preview")
def preview_geometry(job_id: str = ApiPath(...)):
    job_dir = Path("data") / job_id
    train = job_dir / "train.csv"
    predict = job_dir / "predict.csv"

    if not train.exists():
        raise HTTPException(status_code=404, detail="Job not found")

    def read_xy(path):
        pts = []
        with open(path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for r in reader:
                pts.append({
                    "x": float(r["x"]),
                    "y": float(r["y"]),
                })
        return pts

    return {
        "measured": read_xy(train),
        "generated": read_xy(predict) if predict.exists() else [],
    }
