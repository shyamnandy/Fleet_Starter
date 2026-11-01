"""Request and event models."""
import datetime as dt
from typing import Literal, Optional

from pydantic import BaseModel, Field, field_validator


class HeartbeatRequest(BaseModel):
    """Payload accepted by POST /v1/heartbeats."""

    vin: str
    ts: dt.datetime
    speed_kmph: float = Field(ge=0, le=300)
    odometer: float = Field(ge=0)
    odometer_unit: Literal["km", "mi"] = "km"
    lat: float = Field(ge=-90, le=90)
    lon: float = Field(ge=-180, le=180)
    heading: Optional[int] = Field(default=None, ge=0, le=359)
    soc_pct: Optional[float] = Field(default=None, ge=0, le=100)
    ignition: bool = True
    source_oem: str

    @field_validator("vin", mode="before")
    @classmethod
    def validate_vin(cls, value: object) -> str:
        if not isinstance(value, str):
            raise ValueError("must be a string")
        value = value.upper()
        allowed = "ABCDEFGHJKLMNPRSTUVWXYZ0123456789"
        if len(value) != 17:
            raise ValueError("must contain exactly 17 characters")
        if any(character not in allowed for character in value):
            raise ValueError("must use A-Z and 0-9, excluding I, O, and Q")
        return value

    @field_validator("ts")
    @classmethod
    def validate_timestamp(cls, value: dt.datetime) -> dt.datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("must include a timezone")
        if value > dt.datetime.now(dt.timezone.utc) + dt.timedelta(minutes=5):
            raise ValueError("must not be more than 5 minutes in the future")
        return value


class TelemetryEvent(BaseModel):
    """Normalized event written to telemetry.raw."""

    schema_version: Literal["v1"] = Field(default="v1", alias="schema")
    vin: str
    ts_utc: dt.datetime
    speed_kmph: float
    odometer_km: float
    lat: float
    lon: float
    heading: Optional[int] = None
    soc_pct: Optional[float] = None
    ignition: bool = True
    source_oem: str

    model_config = {"populate_by_name": True}

    @field_validator("ts_utc")
    @classmethod
    def validate_event_timestamp(cls, value: dt.datetime) -> dt.datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("must include a timezone")
        return value

    def to_message(self) -> dict:
        return self.model_dump(mode="json", by_alias=True)
