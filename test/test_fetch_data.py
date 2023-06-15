import pytest
import pandas as pd
import src.fetch_data as fetch_data
import env
from src.fetch_data import ForeignKey, SlugReq


# fixture the creates mock data for SeatgeekData
def mock_seatgeek_data():
    """
    returns sql engine and SeatgeekData object
    :return:
    """
    mock_performer_events_venue = pd.DataFrame(
        {
            'event_id': [1, 2, 3],
            'performer_id': [1, 1, 1],
            'venue_id': [1, 2, 3],
            'datetime_utc': ['2019-01-01', '2019-01-02', '2019-01-03'],
            'announce_date': ['2019-01-01', '2019-01-02', '2019-01-03'],
            'visible_until_utc': ['2019-01-01', '2019-01-02', '2019-01-03'],
        }
    )
    mock_stats_df = pd.DataFrame(
        {
            'event_id': [1, 2, 3],
            'lowest_price': [1, 2, 3],
            'average_price': [1, 2, 3],
            'highest_price': [1, 2, 3],
            'listing_count': [1, 2, 3],
            'utc_read_time': ['2019-01-01', '2019-01-02', '2019-01-03'],
        }
    )
    mock_events = pd.DataFrame()
    mock_performers = pd.DataFrame(
        {
            'id': [1],
            'name': ['Test Performer'],
            'slug': ['test-performer'],
        }
    )
    venue = pd.DataFrame(
        {
            'id': [1],
            'name': ['Test Venue'],
            'slug': ['test-venue'],
        }
    )
    seatgeek_data = fetch_data.SeatgeekData(
        events=mock_events,
        performers=mock_performers,
        stats=mock_stats_df,
        performer_event_venue=mock_performer_events_venue,
        venue=venue
    )
    return seatgeek_data


@pytest.fixture
def seatgeek_data():
    """
    returns sql engine and SeatgeekData object
    :return:
    """
    client = fetch_data.ScalpyrPro(env.SEATGEEK_CLIENT_ID)
    seatgeek_data = mock_seatgeek_data()
    return client, seatgeek_data


# test_stats template, ById is passed in as a parameter, used to call get_stats,
# event_id in the resulting dataframe should equal the event_id in the 'performer_event_venue' table
# selected by the ById.id = value
def template_test_stats(seatgeek_data, id_info: ForeignKey):
    """
    # test_stats template, ById is passed in as a parameter, used to call get_stats,
    # event_id in the resulting dataframe should equal the event_id in the 'performer_event_venue' table
    # selected by the ById.id = value
    :param seatgeek_data:
    :param id_info:
    :return:
    """
    client, seatgeek_data = seatgeek_data
    stats = seatgeek_data.get_stats_by_id(id_info)
    # stats.event_id should be equal to seatgeek_data.performer_event_venue.event_id where id_ = value
    assert stats.event_id.equals(
        seatgeek_data.performer_event_venue.event_id[
            seatgeek_data.performer_event_venue[id_info.fk.name] == id_info.fk.value
            ]
    )


# write 3 tests that test the 3 different ways to call get_stats_by_id
# 1. supplying venue_id gets stats for that venue_id
# 2. supplying event_id gets stats for that event_id
# 3. supplying performer_id gets stats for that performer_id
def test_get_stats_by_id_2(seatgeek_data):
    """
    supplying venue_id gets stats for that venue_id
    :param seatgeek_data:
    :return:
    """
    template_test_stats(seatgeek_data, ForeignKey(fk={"venue_id": '1'}))


def test_get_stats_by_id_3(seatgeek_data):
    """
    supplying event_id gets stats for that event_id
    :param seatgeek_data:
    :return:
    """
    template_test_stats(seatgeek_data, ForeignKey(fk={"event_id": '1'}))
    template_test_stats(seatgeek_data, ForeignKey(fk={"event_id": '2'}))
    template_test_stats(seatgeek_data, ForeignKey(fk={"event_id": '3'}))


def test_get_stats_by_id_4(seatgeek_data):
    """
    supplying performer_id gets stats for that performer_id
    :param seatgeek_data:
    :return:
    """
    template_test_stats(seatgeek_data, ForeignKey(fk={'performer_id': '1'}))
    template_test_stats(seatgeek_data, ForeignKey(fk={'performer_id': '2'}))
    template_test_stats(seatgeek_data, ForeignKey(fk={'performer_id': '3'}))
    # this df should be empty
    template_test_stats(seatgeek_data, ForeignKey(fk={'performer_id': '4'}))


def test_get_stats_by_slug_1(seatgeek_data):
    """
    supplying venue_slug gets stats for that venue_slug
    :param seatgeek_data:
    :return:
    """
    slug_req = SlugReq(slug={'venue': 'test-venue'})
    client, seatgeek_data = seatgeek_data
    stats = seatgeek_data.get_stats_by_slug(slug_req)
    # assert that stats.event_id is equal to seatgeek_data.performer_event_venue.event_id where venue_id = 1
    assert stats.event_id.equals(
        seatgeek_data.performer_event_venue.loc[
            seatgeek_data.performer_event_venue['venue_id'] == 1, 'event_id'
        ]
    )


def test_get_stats_by_slug_2(seatgeek_data):
    """
    supplying performer_slug gets stats for that performer_slug
    :param seatgeek_data:
    :return:
    """
    slug_req = SlugReq(slug={'performer': 'test-performer'})
    client, seatgeek_data = seatgeek_data
    stats = seatgeek_data.get_stats_by_slug(slug_req)
    assert stats.event_id.equals(
        seatgeek_data.performer_event_venue.loc[
            seatgeek_data.performer_event_venue['performer_id'] == 1, 'event_id'
        ]
    )
