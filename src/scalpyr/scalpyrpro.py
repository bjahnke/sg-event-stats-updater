import typing

import pandas as pd

from src.scalpyr.scalpyr import Scalpyr
import pandas as pd
import pydantic


# pydantic class that represents events, performers, stats, and venue columns to be kept from the Seatgeek API
class SeatGeekDataStructure(pydantic.BaseModel):
    events: typing.List[str]
    performers: typing.List[str]
    stats: typing.List[str]
    venues: typing.List[str]


class ApiException(Exception):
    """thrown when an API request fails to give valid data"""


class ScalpyrPro(Scalpyr):
    """
    ScalpyrPro is a subclass of Scalpyr
    """
    def __init__(self, dev_key, sg_data_structure: SeatGeekDataStructure = None):
        super().__init__(dev_key)
        self.sg_data_structure = sg_data_structure
        if self.sg_data_structure is None:
            self.sg_data_structure = SeatGeekDataStructure(
                events=['type', 'id', 'datetime_utc', 'venue', 'performers', 'short_title', 'stats', 'url',
                        'score', 'announce_date', 'status', 'access_method', 'visible_at'],
                performers=['type', 'name', 'id', 'has_upcoming_events', 'primary',
                            'url', 'score', 'slug', 'num_upcoming_events'],
                stats=['listing_count', 'average_price', 'median_price', 'lowest_price', 'highest_price'],
                venues=['state', 'postal_code', 'name', 'timezone', 'url', 'score', 'location', 'country',
                        'num_upcoming_events', 'city', 'slug', 'id', 'access_method', 'metro_code', 'capacity']
            )

    def _send_request(self, req_type=None, req_args=None, req_id=None) -> pd.DataFrame:
        response = super()._send_request(req_type, req_args, req_id)
        # 'errors' in response keys, return response
        if req_type not in response.keys():
            raise ApiException(response)
        try:
            columns = getattr(self.sg_data_structure, req_type)
            df = pd.DataFrame.from_dict(response[req_type])
            df = df[columns]
        except KeyError:
            raise ApiException(response)
        return df

    def get_events_by_performers(self, performers: pd.DataFrame) -> pd.DataFrame:
        """
        Returns a dataframe of events for a given list of performer ids
        :param performers:
        :return:
        """
        return self.get_events({
            'performers.id': ','.join(performers.id.astype(str)),
            'per_page': performers.num_upcoming_events.sum(),
            'type': 'concert'
        })
