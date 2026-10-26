from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator


class Event(BaseModel):
    model_config = ConfigDict(extra="forbid")
    event_id: UUID
    user_id: str = Field(min_length=1, max_length=100)
    event_type: Literal["page_view", "search", "click", "purchase", "error"]
    page: str = Field(min_length=1, max_length=500)
    response_time_ms: int = Field(ge=0, le=3_600_000, strict=True)
    status_code: int = Field(ge=100, le=599, strict=True)
    timestamp: AwareDatetime
    run_id: UUID | None = None

    @field_validator("timestamp", mode="before")
    @classmethod
    def timestamp_is_iso(cls, value):
        if not isinstance(value, (str, datetime)):
            raise ValueError("timestamp must be an ISO-8601 string with timezone")  # noqa: TRY004 -- Pydantic validators require ValueError
        return value
