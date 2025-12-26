# --------------------------------------------------
# Standard library imports
# --------------------------------------------------
import uuid
from pathlib import Path
import csv
from math import hypot

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
    value_column: str | None = Form(None),
    output_spacing: float | None = Form(None),
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
    base_dir = Path("data") / job_id
    base_dir.mkdir(parents=True, exist_ok=True)

    # --------------------------------------------------
    # 3. Save raw CSV
    # --------------------------------------------------
    csv_path = base_dir / csv_file.filename
    with open(csv_path, "wb") as f:
        f.write(await csv_file.read())

    # --------------------------------------------------
    # 4. Read CSV header (encoding tolerant)
    # --------------------------------------------------
    try:
        with open(csv_path, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            header = next(reader)
    except UnicodeDecodeError:
        with open(csv_path, "r", encoding="latin-1") as f:
            reader = csv.reader(f)
            header = next(reader)

    normalized = {h.strip().lower(): h for h in header}

    def norm(x): 
        return x.strip().lower()

    required = {norm(x_column), norm(y_column)}
    if scenario == Scenario.sparse_only:
        required.add(norm(value_column))

    missing = required - normalized.keys()
    if missing:
        raise HTTPException(
            status_code=400,
            detail=f"Missing required column(s): {', '.join(missing)}",
        )

    # --------------------------------------------------
    # 5. Load selected columns only
    # --------------------------------------------------
    selected = [
        normalized[norm(x_column)],
        normalized[norm(y_column)],
    ]

    if scenario == Scenario.sparse_only:
        selected.append(normalized[norm(value_column)])

    rows = []

    try:
        with open(csv_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for r in reader:
                rows.append({k: r[k] for k in selected})
    except UnicodeDecodeError:
        with open(csv_path, "r", encoding="latin-1") as f:
            reader = csv.DictReader(f)
            for r in reader:
                rows.append({k: r[k] for k in selected})

    if not rows:
        raise HTTPException(status_code=400, detail="CSV has no data rows")

    # --------------------------------------------------
    # 6. Write canonical CSV
    # --------------------------------------------------
    canonical_path = base_dir / "input_selected.csv"
    with open(canonical_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=selected)
        writer.writeheader()
        writer.writerows(rows)

    # --------------------------------------------------
    # 7. Train / Predict split
    # --------------------------------------------------
    train_path = base_dir / "train.csv"
    predict_path = base_dir / "predict.csv"

    if scenario == Scenario.sparse_only:
        with open(train_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=selected)
            writer.writeheader()
            writer.writerows(rows)

        with open(predict_path, "w", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=selected).writeheader()

    else:
        value_col = normalized[norm(value_column)]
        train, predict = [], []

        for r in rows:
            if r[value_col] in ("", None):
                predict.append(r)
            else:
                train.append(r)

        for path, data in [(train_path, train), (predict_path, predict)]:
            with open(path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=selected)
                writer.writeheader()
                writer.writerows(data)

    # --------------------------------------------------
    # 8. Geometry generation (sparse_only)
    # --------------------------------------------------
    if scenario == Scenario.sparse_only:
        x_col, y_col, v_col = selected
        pts = [(float(r[x_col]), float(r[y_col])) for r in rows]

        pts.sort()
        dist = [0.0]

        for i in range(1, len(pts)):
            dx = pts[i][0] - pts[i-1][0]
            dy = pts[i][1] - pts[i-1][1]
            dist.append(dist[-1] + hypot(dx, dy))

        gen = []
        d = output_spacing

        while d < dist[-1]:
            for i in range(1, len(dist)):
                if dist[i] >= d:
                    t = (d - dist[i-1]) / (dist[i] - dist[i-1])
                    x = pts[i-1][0] + t * (pts[i][0] - pts[i-1][0])
                    y = pts[i-1][1] + t * (pts[i][1] - pts[i-1][1])
                    gen.append({x_col: x, y_col: y, v_col: ""})
                    break
            d += output_spacing

        with open(predict_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=selected)
            writer.writeheader()
            writer.writerows(gen)

    return JobResponse(job_id=job_id, status="accepted")


# ==================================================
# GET /jobs/{job_id}/preview
# ==================================================
@router.get("/{job_id}/preview")
def preview_geometry(job_id: str = ApiPath(...)):
    base_dir = Path("data") / job_id
    train = base_dir / "train.csv"
    predict = base_dir / "predict.csv"

    if not train.exists():
        raise HTTPException(status_code=404, detail="Job not found")

    def read_xy(path):
        pts = []
        try:
            with open(path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                keys = reader.fieldnames[:2]
                for r in reader:
                    pts.append({"x": float(r[keys[0]]), "y": float(r[keys[1]])})
        except UnicodeDecodeError:
            with open(path, "r", encoding="latin-1") as f:
                reader = csv.DictReader(f)
                keys = reader.fieldnames[:2]
                for r in reader:
                    pts.append({"x": float(r[keys[0]]), "y": float(r[keys[1]])})
        return pts

    return {
        "measured": read_xy(train),
        "generated": read_xy(predict) if predict.exists() else [],
    }
