import env
from src.fetch_data import SeatgeekData, SlugReq
from src.scalpyr import ScalpyrPro
from sqlalchemy import create_engine
import pandas as pd
from pprint import pprint

from src.scalpyr.scalpyrpro import ApiException


def get_performer_id(slug):
    """
    # function that gets a performers id from Scalpyr api
    # and looks up id in stats database
    :param slug:
    :return:
    """
    client = ScalpyrPro(env.SEATGEEK_CLIENT_ID)
    performer = client.get_performers({'slug': slug})
    performer_id = performer.id.values[0]
    return performer_id


def get_performer_stats(slug):
    # get performer id from scalpyr
    performer_id = get_performer_id(slug)
    engine = create_engine(env.PLANETSCALE_URL)
    stats = pd.read_sql(
        f'SELECT s.* '
        f'FROM stat s '
        f'JOIN performer_event_venue pev ON s.event_id = pev.event_id '
        f'WHERE pev.performer_id = {performer_id}'
        , engine
    )
    return stats


def get_performer_event_ids(slug):
    performer_id = get_performer_id(slug)
    engine = create_engine(env.PLANETSCALE_URL)
    ids = pd.read_sql(
        f'SELECT * '
        f'FROM performer_event_venue '
        f'WHERE performer_id = {performer_id}'
        , engine
    )
    return ids


def plot_stats(performer_stats: pd.DataFrame, title, include_highest=False):
    """
    # function, given performer stats df, plot average_price, lowest_price, highest_price, median_price, listing_count
    :param performer_stats:
    :param title:
    :param include_highest:
    :return:
    """
    stats = ['average_price', 'lowest_price', 'median_price', 'listing_count']
    stats += ['highest_price'] if include_highest else []
    # plot average_price, lowest_price, median_price, listing_count on left-y. plot highest_price on right-y
    plot_df = performer_stats.set_index('utc_read_time', drop=True)
    plot_df[stats].plot(secondary_y=['listing_count'], title=title)


def plot_all_stats(performer_stats, performer_events):
    """
    plots all stats for a given performer
    :param performer_events:
    :param performer_stats:
    :return:
    """
    client = ScalpyrPro(env.SEATGEEK_CLIENT_ID)
    for event_id in performer_stats.event_id.unique():
        ref_data = performer_events.loc[performer_events.event_id == event_id].iloc[0]
        venue_id = ref_data.venue_id
        venue_name = client.get_venues({'id': venue_id}).name.values[0]
        performer_slug = client.get_performers({'id': ref_data.performer_id}).slug.values[0]
        title = f'{performer_slug}, {venue_name}, {ref_data.datetime_utc}'
        data = performer_stats.loc[performer_stats.event_id == event_id].copy()
        plot_stats(data, title)


def plot_every_stat():
    engine = create_engine(env.PLANETSCALE_URL)
    stats = pd.read_sql(
        f'SELECT * '
        f'FROM stat'
        , engine
    )
    performer_events = pd.read_sql(
        f'SELECT * '
        f'FROM performer_event_venue'
        , engine
    )
    plot_all_stats(stats, performer_events)


def plot_by_slug(slug):
    performer_stats = get_performer_stats(slug)
    performer_event_ids = get_performer_event_ids(slug)
    plot_all_stats(performer_stats, performer_event_ids)
    return performer_event_ids


class DataPlotter(SeatgeekData):
    def plot_by_slug(self, slug):
        slug = SlugReq(slug={'performer': slug})
        stats = self.get_stats_by_slug(slug)
        ids = self.get_ids_by_slug(slug)
        plot_all_stats(stats, ids)
        return ids

    def stats_info_view(self, slug):
        """
        gets the stats for a given slug and joins the stats with the performer_event_venue table
        :param slug:
        :return:
        """
        slug = SlugReq(slug={'performer': slug})
        stats = self.get_stats_by_slug(slug)
        # merge stats with performer_event_venue on event_id
        stats_joined = stats.merge(self.get_ids_by_slug(slug), on='event_id')
        client = ScalpyrPro(env.SEATGEEK_CLIENT_ID)
        # get venue name from scalpyr by concating all unique venue_ids into comma separated string
        venues = client.get_by_id(
            'venues', stats_joined.venue_id.unique()
        )[['id', 'slug']].rename(columns={'slug': 'venue_slug'})
        # merge stats with venues on left on venue_id right on id, drop id column
        stats_joined = stats_joined.merge(venues, left_on='venue_id', right_on='id').drop(columns='id')
        # get performer name from scalpyr by concating all unique performer_ids into comma separated string
        performers = client.get_by_id(
            'performers', stats_joined.performer_id.unique()
        )[['id', 'slug']].rename(columns={'slug': 'performer_slug'})
        # merge stats with performers on left on performer_id right on id, drop id column
        stats_joined = stats_joined.merge(performers, left_on='performer_id', right_on='id').drop(columns='id')
        # create a new column that is a concatenation of performer_slug, venue_slug, and datetime_utc
        stats_joined['title'] = stats_joined.apply(
            lambda x: f'{x.performer_slug},{x.venue_slug},{x.datetime_utc}', axis=1)
        # drop all columns except utc_read_time, average_price, event_id, title
        # stats_joined = stats_joined[['utc_read_time', 'average_price', 'event_id', 'title']]
        return stats_joined


if __name__ == '__main__':
    artist = 'Avenged Sevenfold'
    _performer_stats = get_performer_stats(artist)
    _performer_event_ids = get_performer_event_ids(artist)
    plot_all_stats(_performer_stats, _performer_event_ids)

