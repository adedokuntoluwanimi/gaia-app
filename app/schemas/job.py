from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class Scenario(str, Enum):
    sparse_only = "sparse_only"
    explicit_geometry = "explicit_geometry"


class JobRequest(BaseModel):
    scenario: Scenario = Field(
        ...,
        description="Defines how the backend interprets the uploaded CSV",
    )

    x_column: str = Field(
        ...,
        description="Column name for longitude or X coordinate",
    )

    y_column: str = Field(
        ...,
        description="Column name for latitude or Y coordinate",
    )

    value_column: Optional[str] = Field(
        None,
        description="Column name for magnetic values",
    )

    output_spacing: Optional[float] = Field(
        None,
        description="Desired station spacing for generated points (meters)",
    )


class JobResponse(BaseModel):
    job_id: str
    status: str
