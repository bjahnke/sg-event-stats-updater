import typing
from dataclasses import dataclass
from datetime import datetime

import numpy as np
import pandas as pd
from pydantic import BaseModel, validator

from src.scalpyr import ScalpyrPro
import src.fetch_data.schema as schema


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
            self,
            events: pd.DataFrame,
            performers: pd.DataFrame,
            stats: pd.DataFrame,
            performer_event_venue: pd.DataFrame,
            venue: pd.DataFrame
    ):
        self.event = events
        self.performer = performers
        self.stat = stats
        self.performer_event_venue = performer_event_venue
        self.venue = venue

    @classmethod
    def from_events(cls, events: pd.DataFrame):
        """
        Returns a SeatgeekData object from events
        :param events:
        :return:
        """
        performers = build_df_from_series_of_dicts(events["performers"].explode())
        return cls._build_tables(events, performers)

    @classmethod
    def from_watchlist(
            cls,
            client: ScalpyrPro,
            venue_id: typing.Union[typing.List[str], None],
            performer_id: typing.Union[typing.List[str], None],
            event_id: typing.Union[typing.List[str], None],
    ):
        """
        Returns a SeatgeekData object from a watchlist
        :param client:
        :param venue_id:
        :param performer_id:
        :param event_id:
        :return:
        """
        events = []
        if venue_id:
            events.append(client.get_events_by('venue', venue_id))
        if performer_id:
            events.append(client.get_events_by('performers', performer_id))
        if event_id:
            events.append(client.get_by_id('events', event_id))
        # concat events, keep first instance of each event id
        events = pd.concat(events).drop_duplicates(subset=['id'])
        # get performers from events
        performers = build_df_from_series_of_dicts(events["performers"].explode())
        performers = performers.drop_duplicates(subset=['id'])

        return cls._build_tables(events, performers)

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
        events = client.get_events_by_performers(performers)
        return cls._build_tables(events, performers)

    @classmethod
    def from_db(cls, engine, client: ScalpyrPro, stats_query: str = ""):
        """
        Returns a SeatgeekData object from a database
        :param stats_query:
        :param client:
        :param engine:
        :return:
        """
        print(f"getting events from database {datetime.now()}")
        performer_event_venue = pd.read_sql_table("performer_event_venue", engine)
        if stats_query:
            stat = pd.read_sql(stats_query, engine)
        else:
            stat = pd.read_sql_table("stat", engine)
        # get performers from api by unique performer_id in performer_event_venue table
        performer_ids = performer_event_venue.performer_id.astype(str).unique()
        performers = client.get_performers(
            {
                'id': ','.join(performer_ids),
                'per_page': len(performer_ids),
            }
        )
        event_ids = performer_event_venue.event_id.astype(str).unique()
        # split event ids into ten lists
        event_ids = np.array_split(event_ids, 5)
        events_list = []

        for i in event_ids:
            evnts = client.get_events(
                {
                    'id': ','.join(i),
                }
            )
            events_list.append(evnts)
        events = pd.concat(events_list)
        local_data = cls._build_tables(events, performers)
        return cls(local_data.event, local_data.performer, stat, performer_event_venue, local_data.venue)

    @classmethod
    def _build_tables(
        cls, events: pd.DataFrame, performers_df: pd.DataFrame
    ):
        print(f"building seatgeek data {datetime.now()}")
        events = events
        performers_df = performers_df
        stats_df = build_stats_df(events)
        events["venue_id"] = get_event_venue_ids(events)
        performer_events_venue = build_performer_events_df(events)
        venue = build_df_from_series_of_dicts(events["venue"]).drop_duplicates('id')
        return cls(events, performers_df, stats_df, performer_events_venue, venue)

    def get_performer_events(self, performer_name: str) -> pd.Series:
        return get_performer_events(
            performer_name, self.performer, self.performer_event_venue
        )

    def get_performer_stats(self, performer_name: str) -> pd.DataFrame:
        return get_performer_stats_df(performer_name, self.performer, self.event, self.performer_event_venue)

    def get_performer_events_df(self, performer_name: str) -> pd.DataFrame:
        return get_performer_events_df(
            performer_name, self.performer, self.event, self.performer_event_venue
        )

    # function that pushes all tables to a database using sqlalchemy via pandas
    def push_to_db(self, engine):
        """
        pushes all data to a database using sqlalchemy via pandas
        new stat rows are appended to the stat table
        new performer_event_venue rows are appended to the performer_event_venue table
        :param engine:
        :return:
        """
        self.stat.to_sql("stat", engine, if_exists="append", index=False)

        try:
            stored_performer_event_venue = pd.read_sql_table("performer_event_venue", engine)
            # select
        except Exception as e:
            # then push the result to the db
            new_table = self.performer_event_venue
        else:
            # select rows in performer_events_df where event_id is not in db list of event_ids
            new_table = pd.concat(
                [stored_performer_event_venue, self.performer_event_venue]
            )
            new_table = new_table.drop_duplicates()
            # then push the result to the db
        new_table.to_sql(
            "performer_event_venue", engine, if_exists="replace", index=False
        )

    def get_ids_by_slug(self, query: schema.SlugReq) -> pd.DataFrame:
        """
        Returns a dataframe of performer_event_venue for a given slug
        :param query:
        :return:
        """
        table = getattr(self, query.slug.name)
        # select id from table where slug = query.slug.value, should be unique
        id_ = table.loc[table['slug'] == query.slug.value, "id"].values[0]
        return self.get_pev_by_id(schema.ForeignKey(fk={f"{query.slug.name}_id": id_}))

    def get_pev_by_id(self, query: schema.ForeignKey) -> pd.DataFrame:
        """
        Returns a dataframe of performer_event_venue for a given id
        :param query:
        :return:
        """
        # select rows in self.performer_event_venue where query.id.key = query.id.value
        return self.performer_event_venue.loc[
            self.performer_event_venue[query.fk.name] == query.fk.value
            ]

    def _get_stats(self, event_ids: pd.Series) -> pd.DataFrame:
        return self.stat.loc[self.stat.event_id.isin(event_ids)]

    def get_stats_by_slug(self, query: schema.SlugReq) -> pd.DataFrame:
        event_ids = self.get_ids_by_slug(query).event_id
        return self._get_stats(event_ids)

    def get_stats_by_id(self, query: schema.ForeignKey) -> pd.DataFrame:
        event_ids = self.get_pev_by_id(query).event_id
        return self._get_stats(event_ids)


