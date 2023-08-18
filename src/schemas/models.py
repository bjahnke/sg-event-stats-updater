import numpy
from pydantic import BaseModel
from pymongo.collection import ObjectId
import typing as t
from pydantic import validator

class WatchListResponse(BaseModel):
    """
    required:
    _id: ObjectId
    username: str

    at least one of the following is required:
    venue_id: t.List[numpy.int32]
    performer_id: t.List[numpy.int32]
    event_id: t.List[numpy.int32]
    """
    _id: ObjectId
    username: str
    venue_id: t.Optional[t.List[str]] = None
    performer_id:  t.Optional[t.List[str]] = None
    event_id:  t.Optional[t.List[str]] = None

    # @validator('venue_id', 'performer_id', 'event_id', pre=True, always=True)
    # def at_least_one_field_required(cls, values):
    #     if not any(values.get(field) for field in ['venue_id', 'performer_id', 'event_id']):
    #         raise ValueError("At least one of 'venue_id', 'performer_id', or 'event_id' is required")
    #     return values
