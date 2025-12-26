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
    # 1. Scenario validation
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
    # 2. Job workspace
    # --------------------------------------------------
    job_id = f"gaia-{uuid.uuid4().hex[:12]}"
    job_dir = Path("data") / job_id
    job_dir.mkdir(parents=True, exist_ok=True)

    # --------------------------------------------------
    # 3. Save uploaded CSV
    # --------------------------------------------------
    raw_csv_path = job_dir / csv_file.filename
    with open(raw_csv_path, "wb") as f:
        f.write(await csv_file.read())

    # --------------------------------------------------
    # 4. Read CSV header
    # --------------------------------------------------
    with open(raw_csv_path, "r", encoding="utf-8", errors="ignore") as f:
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
    rows = []
    with open(raw_csv_path, "r", encoding="utf-8", errors="ignore") as f:
        reader = csv.DictReader(f)
        for r in reader:
            rows.append(r)

    if not rows:
        raise HTTPException(status_code=400, detail="CSV has no data")

    train_path = job_dir / "train.csv"
    predict_path = job_dir / "predict.csv"

    # ==================================================
    # 6. Sparse-only geometry
    # ==================================================
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

        # ---- strip station_index before CSV write ----
        clean_train = []
        for r in train_rows:
            clean_train.append({
                "x": r["x"],
                "y": r["y"],
                "d_along": r["d_along"],
                "value": r["value"],
            })

        clean_predict = []
        for r in predict_rows:
            clean_predict.append({
                "x": r["x"],
                "y": r["y"],
                "d_along": r["d_along"],
                "value": r["value"],
            })

        with open(train_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=["x", "y", "d_along", "value"],
            )
            writer.writeheader()
            writer.writerows(clean_train)

        with open(predict_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=["x", "y", "d_along", "value"],
            )
            writer.writeheader()
            writer.writerows(clean_predict)

    # ==================================================
    # 7. Explicit geometry (no geometry generation)
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
            value = r.get(v_col)
            if value in ("", None):
                predict_rows.append({
                    "x": float(r[x_col]),
                    "y": float(r[y_col]),
                    "value": None,
                })
            else:
                train_rows.append({
                    "x": float(r[x_col]),
                    "y": float(r[y_col]),
                    "value": float(value),
                })

        with open(train_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(
                f, fieldnames=["x", "y", "value"]
            )
            writer.writeheader()
            writer.writerows(train_rows)

        with open(predict_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(
                f, fieldnames=["x", "y", "value"]
            )
            writer.writeheader()
            writer.writerows(predict_rows)

    return JobResponse(job_id=job_id, status="accepted")


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

    def read_points(path):
        points = []
        with open(path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for idx, row in enumerate(reader):
                points.append({
                    "x": float(row["x"]),
                    "y": float(row["y"]),
                    "station_index": idx,
                })
        return points

    measured = read_points(train_path)
    generated = read_points(predict_path) if predict_path.exists() else []

    return {
        "measured": measured,
        "generated": generated,
    }

