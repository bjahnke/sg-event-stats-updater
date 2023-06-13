from datetime import datetime
import pandas as pd
from src.scalpyr import Scalpyr, ScalpyrPro


def get_performer_events(
    performer_name: str, performers_df: pd.DataFrame, performers_events: pd.DataFrame
) -> pd.Series:
    """
    Returns a dataframe of events for a given performer name
    :param performers_events:
    :param performers_df:
    :param performer_name:
    :return:
    """
    performer_id = performers_df.loc[performers_df["name"] == performer_name][
        "id"
    ].values[0]
    return performers_events.loc[
        performers_events["performer_id"] == performer_id, "event_id"
    ]


def get_performer_events_df(
    performer_name: str,
    performers_df: pd.DataFrame,
    events_df: pd.DataFrame,
    performer_events: pd.DataFrame,
) -> pd.DataFrame:
    """
    Returns a dataframe of events for a given performer name
    :param performer_events:
    :param events_df:
    :param performers_df:
    :param performer_name:
    :return:
    """
    event_ids = get_performer_events(performer_name, performers_df, performer_events)
    return select_events_by_event_id(events_df, event_ids)


def select_events_by_event_id(
    events_df: pd.DataFrame, event_ids: pd.Series
) -> pd.DataFrame:
    """
    Returns a dataframe of events for a given event id
    :param events_df:
    :param event_ids:
    :return:
    """
    return events_df.loc[events_df["id"].isin(event_ids)]


def build_stats_df(events_df: pd.DataFrame) -> pd.DataFrame:
    """
    moves stats from events_df to its own dataframe, with id from
    events_df added as a foreign key
    Returns a dataframe of stats for a given events dataframe
    :param events_df:
    :return:
    """
    stats_df = events_df[["id", "stats"]].copy()
    stats_df = stats_df.rename(columns={"id": "event_id"})
    # expand stats column dict keys into columns, keep event_id, drop stats column
    stats_df = pd.concat(
        [stats_df.drop(["stats"], axis=1), stats_df["stats"].apply(pd.Series)], axis=1
    )
    # keep only event_id, average_price, lowest_price, highest_price, listing_count, visible_listing_count, median_price
    stats_df = stats_df[
        [
            "event_id",
            "average_price",
            "lowest_price",
            "highest_price",
            "median_price",
            "listing_count",
            "visible_listing_count",
        ]
    ]
    # add utc timestamp as column to stats_df, without seconds
    stats_df["utc_read_time"] = pd.to_datetime("now").round("min")
    stats_df = stats_df.dropna()
    return stats_df


def build_performer_events_df(events: pd.DataFrame) -> pd.DataFrame:
    """
    Returns a dataframe of performer ids mapped to event ids
    :param events:
    :return:
    """
    # map performers to event id and explode
    performers_events = events[
        ["id", "venue_id", "performers", 'datetime_utc', 'announce_date', 'visible_at']
    ].explode("performers")
    # rename id to event_id
    performers_events = performers_events.rename(columns={"id": "event_id"})
    # extract id from performers, mapping event ids to performer ids
    performers_events["performer_id"] = performers_events["performers"].apply(
        lambda x: x["id"]
    )
    # drop performers column
    performers_events = performers_events.drop(columns=["performers"])
    return performers_events


def get_performer_stats_df(
    performer_name: str,
    performers_df: pd.DataFrame,
    stats_df: pd.DataFrame,
    performer_events_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Returns a dataframe of stats for a given performer name
    :param stats_df:
    :param performer_events_df:
    :param performers_df:
    :param events_df:
    :param performer_name:
    :return:
    """
    event_ids = get_performer_events(performer_name, performers_df, performer_events_df)
    return stats_df.loc[stats_df["event_id"].isin(event_ids)]


def build_df_from_series_of_dicts(
    series_of_dicts: pd.Series,
) -> pd.DataFrame:
    """
    Returns a dataframe from a series of dicts
    :param series_of_dicts:
    :return:
    """
    df = pd.DataFrame(series_of_dicts.tolist())
    return df


def get_event_venue_ids(events: pd.DataFrame) -> pd.DataFrame:
    """
    return venue ids from events
    :param events:
    :return:
    """
    venue_df = build_df_from_series_of_dicts(events["venue"])
    return venue_df.rename(columns={"id": "venue_id"})["venue_id"]


class SeatgeekData:
    """
    # class which, given event data from seatgeek, builds dataframes using
    # the table builder functions above
    """

    def __init__(
        self, events: pd.DataFrame, performers_df: pd.DataFrame
    ):
        print(f"building seatgeek data {datetime.now()}")
        self.events = events
        self.performers_df = performers_df
        self.stats_df = build_stats_df(events)
        self.events["venue_id"] = get_event_venue_ids(self.events)
        self.performer_events_df = build_performer_events_df(events)

    @classmethod
    def from_api(cls, client: ScalpyrPro):
        """
        Returns a SeatgeekData object from the Seatgeek API
        :param request_args:
        :param client:
        :return:
        """
        print(f"getting events from seatgeek {datetime.now()}")
        performers = client.get_performers(
            {'type': 'band', 'per_page': 100, 'has_upcoming_events': 'true'}
        )

        events = client.get_events({
            'performers.id': ','.join(performers.id.astype(str)),
            'per_page': performers.num_upcoming_events.sum(),
            'type': 'concert'
        })
        return cls(events, performers)

    def get_performer_events(self, performer_name: str) -> pd.Series:
        return get_performer_events(
            performer_name, self.performers_df, self.performer_events_df
        )

    def get_performer_stats(self, performer_name: str) -> pd.DataFrame:
        return get_performer_stats_df(performer_name, self.performers_df, self.events, self.performer_events_df)

    def get_performer_events_df(self, performer_name: str) -> pd.DataFrame:
        return get_performer_events_df(
            performer_name, self.performers_df, self.events, self.performer_events_df
        )

    # function that pushes all tables to a database using sqlalchemy via pandas
    def push_to_db(self, engine):
        """
        pushes all data to a database using sqlalchemy via pandas
        :param engine:
        :return:
        """
        self.stats_df.to_sql("stat", engine, if_exists="append", index=False)

        try:
            stored_event_ids = pd.read_sql(
                f"SELECT event_id FROM performer_event_venue", engine
            )["event_id"].tolist()
        except Exception as e:
            stored_event_ids = []
        # select rows in performer_events_df where event_id is not in db list of event_ids
        new_performer_events_df = self.performer_events_df.loc[
            ~self.performer_events_df["event_id"].isin(stored_event_ids)
        ]
        # then push the result to the db
        new_performer_events_df.to_sql(
            "performer_event_venue", engine, if_exists="append", index=False
        )
