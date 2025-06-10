from pydantic import BaseModel, Field
from datetime import datetime


class Landmark(BaseModel):
    x: float
    y: float
    z: float | None = None
    vis: float | None = None


class AngleMetric(BaseModel):
    name: str
    value: float
    point1: Landmark
    point2: Landmark
    ref_point: Landmark


class DominantSide(BaseModel):
    side: str
    probability: float | None


class Metrics(BaseModel):
    angles: list[AngleMetric]
    dominant_side: DominantSide


class PerformanceInput(BaseModel):
    frame_track_uuid: str
    timestamp: datetime
    exercise: str
    video_duration: float
    session_uuid: str
    landmarks: list[Landmark] = Field(..., description="Detected pose landmarks (COCO format)")
    metrics: Metrics = Field(..., description="Calculated exercise metrics")

    # TODO: set to non optional after adding to the pipeline
    video_duration: float | None
    current_time: float | None
