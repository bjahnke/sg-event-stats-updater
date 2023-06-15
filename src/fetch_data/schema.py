from pydantic import BaseModel
from dataclasses import dataclass
import typing
from abc import ABC

@dataclass
class Pair:
    key: str
    value: typing.Any


class BaseField(ABC):
    @property
    def name(self) -> str:
        """field name"""
        raise NotImplementedError

    @property
    def value(self):
        """field value"""
        raise NotImplementedError


class BaseTableField(BaseField, ABC):
    table: str
    field: BaseField


class PerformerID(BaseModel, BaseField):
    """
    Represents a model with a performer ID.
    """
    performer_id: int

    @property
    def name(self) -> str:
        """
        Returns the ID field
        :return:
        """
        return "performer_id"

    @property
    def value(self):
        return self.performer_id


class EventID(BaseModel, BaseField):
    """
    Represents a model with an event ID.
    """
    event_id: int

    @property
    def name(self) -> str:
        """
        Returns the ID field
        :return:
        """
        return "event_id"

    @property
    def value(self):
        return self.event_id


class VenueID(BaseModel):
    """
    Represents a model with a venue ID.
    """
    venue_id: int

    @property
    def name(self) -> str:
        """
        Returns the ID field
        :return:
        """
        return "venue_id"

    @property
    def value(self):
        return self.venue_id


class ForeignKey(BaseModel):
    """
    Represents a model with fields for performer, event, and venue IDs.
    Only one of the fields should be defined (not None).
    """
    fk: PerformerID | EventID | VenueID

class PerformerSlug(BaseModel, BaseField):
    """
    Represents a model with a performer slug.
    """
    performer: str

    @property
    def name(self) -> str:
        return "performer"

    @property
    def value(self):
        return self.performer


class VenueSlug(BaseModel, BaseField):
    """
    Represents a model with a venue slug.
    """
    venue: str

    @property
    def name(self) -> str:
        return "venue"

    @property
    def value(self):
        return self.venue


class SlugReq(BaseModel):
    """
    Represents a model with a slug.
    """
    slug: PerformerSlug | VenueSlug
