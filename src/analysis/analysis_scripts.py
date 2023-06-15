import env
from src.fetch_data import SeatgeekData, SlugReq
from src.scalpyr import ScalpyrPro
from sqlalchemy import create_engine
import pandas as pd
from pprint import pprint


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
        try:
            info = client.get_events({'id': event_id}).iloc[0]
            slug = info.performers[0]['slug']
            title = f'{slug}, {venue_name}, {info.datetime_utc}'
        except AttributeError:
            title = f'Event {event_id} (old show?)'
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


if __name__ == '__main__':
    artist = 'Avenged Sevenfold'
    _performer_stats = get_performer_stats(artist)
    _performer_event_ids = get_performer_event_ids(artist)
    plot_all_stats(_performer_stats, _performer_event_ids)

