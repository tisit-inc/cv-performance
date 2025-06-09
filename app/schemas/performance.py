from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import datetime


class Landmark(BaseModel):
    x: float
    y: float
    z: Optional[float] = None
    vis: Optional[float] = None


class AngleMetric(BaseModel):
    name: str
    value: float
    point1: Landmark
    point2: Landmark
    ref_point: Landmark


class DominantSide(BaseModel):
    side: str
    probability: Optional[float]


class Metrics(BaseModel):
    angles: List[AngleMetric]
    dominant_side: DominantSide


class PerformanceInput(BaseModel):
    frame_track_uuid: str
    timestamp: datetime
    exercise: str
    video_duration: float
    session_uuid: str
    landmarks: list[Landmark] = Field(
        ..., description="Detected pose landmarks (COCO format)"
    )
    metrics: Metrics = Field(..., description="Calculated exercise metrics")

    # TODO: set to non optional after adding to the pipeline
    video_duration: float | None
    current_time: float | None
