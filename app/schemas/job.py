# --------------------------------------------------
# Standard library imports
# --------------------------------------------------
from enum import Enum
from typing import Optional

# --------------------------------------------------
# Pydantic imports
# --------------------------------------------------
from pydantic import BaseModel, Field


# ==================================================
# Scenario enum
# ==================================================
class Scenario(str, Enum):
    """
    Defines how the backend should interpret
    the uploaded CSV and geometry.
    """

    # User provides only measured stations.
    # Geometry and missing stations are inferred.
    sparse_only = "sparse_only"

    # User provides full geometry explicitly.
    # Backend must not infer or generate stations.
    explicit_geometry = "explicit_geometry"


# ==================================================
# Job creation request schema
# ==================================================
class JobRequest(BaseModel):
    """
    Contract for job creation.
    This model defines how user intent is expressed
    when submitting a new GAIA job.
    """

    # ----------------------------------------------
    # Scenario selector
    # ----------------------------------------------
    scenario: Scenario = Field(
        ...,
        description="Defines how the backend interprets the uploaded CSV",
        example="sparse_only",
    )

    # ----------------------------------------------
    # Spatial column mappings
    # ----------------------------------------------
    x_column: str = Field(
        ...,
        description="Column name for longitude or X coordinate",
        example="Longitude",
    )

    y_column: str = Field(
        ...,
        description="Column name for latitude or Y coordinate",
        example="Latitude",
    )

    # ----------------------------------------------
    # Magnetic value column (optional by scenario)
    # ----------------------------------------------
    value_column: Optional[str] = Field(
        None,
        description="Column name for magnetic values",
        example="Mag",
    )

    # ----------------------------------------------
    # Output geometry control
    # ----------------------------------------------
    output_spacing: Optional[float] = Field(
        None,
        description="Desired station spacing for generated points (meters)",
        example=10.0,
    )


# ==================================================
# Job creation response schema
# ==================================================
class JobResponse(BaseModel):
    """
    Minimal response returned after a job
    has been accepted by the backend.
    """

    job_id: str
    status: str
