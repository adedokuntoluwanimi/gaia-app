# --------------------------------------------------
# Standard library imports
# --------------------------------------------------
import uuid
import os
from pathlib import Path
import csv

# --------------------------------------------------
# FastAPI imports
# --------------------------------------------------
from fastapi import (
    APIRouter,
    UploadFile,
    File,
    Form,
    HTTPException,
)

# --------------------------------------------------
# Internal schema imports
# --------------------------------------------------
from app.schemas.job import JobResponse, Scenario

# --------------------------------------------------
# Router definition
# --------------------------------------------------
router = APIRouter()


@router.post("", response_model=JobResponse)
async def create_job(
    # --------------------------------------------------
    # User-declared scenario
    # --------------------------------------------------
    scenario: Scenario = Form(...),

    # --------------------------------------------------
    # CSV column mappings
    # --------------------------------------------------
    x_column: str = Form(...),
    y_column: str = Form(...),
    value_column: str | None = Form(None),

    # --------------------------------------------------
    # Output control (used only for sparse_only)
    # --------------------------------------------------
    output_spacing: float | None = Form(None),

    # --------------------------------------------------
    # Uploaded CSV file
    # --------------------------------------------------
    csv_file: UploadFile = File(...),
):
    # ==================================================
    # 1. Scenario enforcement and contract validation
    # ==================================================
    if scenario == Scenario.sparse_only:
        # Sparse-only requires magnetic values and spacing
        if value_column is None:
            raise HTTPException(
                status_code=400,
                detail="value_column is required for sparse_only scenario",
            )

        if output_spacing is None:
            raise HTTPException(
                status_code=400,
                detail="output_spacing is required for sparse_only scenario",
            )

    if scenario == Scenario.explicit_geometry:
        # Explicit geometry forbids synthetic spacing
        if output_spacing is not None:
            raise HTTPException(
                status_code=400,
                detail="output_spacing must not be provided for explicit_geometry",
            )

    # ==================================================
    # 2. Job ID generation
    # ==================================================
    job_id = f"gaia-{uuid.uuid4().hex[:12]}"

    # ==================================================
    # 3. Job workspace initialization
    # ==================================================
    # Each job gets an isolated directory under /data
    base_dir = Path("data") / job_id
    base_dir.mkdir(parents=True, exist_ok=True)

    # ==================================================
    # 4. Persist raw uploaded CSV (verbatim)
    # ==================================================
    csv_path = base_dir / csv_file.filename

    with open(csv_path, "wb") as f:
        content = await csv_file.read()
        f.write(content)
        # ==================================================
    # 4b. CSV header validation (user-defined, non-strict)
    # ==================================================
       # ==================================================
    # 4b. CSV header validation (encoding-tolerant)
    # ==================================================
    try:
        with open(csv_path, "r", newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            raw_header = next(reader)
    except UnicodeDecodeError:
        # Fallback for common non-UTF8 CSVs (Excel, instruments)
        with open(csv_path, "r", newline="", encoding="latin-1") as f:
            reader = csv.reader(f)
            raw_header = next(reader)
    except StopIteration:
        raise HTTPException(
            status_code=400,
            detail="Uploaded CSV is empty",
        )


    # Normalize CSV header names
    normalized_header = {
        h.strip().lower(): h for h in raw_header
    }

    # Normalize user-declared column names
    def normalize(name: str) -> str:
        return name.strip().lower()

    required_columns = {
        normalize(x_column),
        normalize(y_column),
    }

    if scenario == Scenario.sparse_only:
        required_columns.add(normalize(value_column))

    missing = required_columns - normalized_header.keys()

    if missing:
        raise HTTPException(
            status_code=400,
            detail=f"Missing required column(s): {', '.join(missing)}",
        )


    # ==================================================
    # 5. Return job acceptance response
    # ==================================================
    return JobResponse(
        job_id=job_id,
        status="accepted",
    )
