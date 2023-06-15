import env
from src.fetch_data.table_builders import SeatgeekData
from sqlalchemy import create_engine

from src.scalpyr import ScalpyrPro


# function: pull all data from 'performer_event_venue' table to df,
#           add datetime_utc, announce_date, visible_at columns
#           push df to 'performer_event_venue' table, replacing all data
def migrate():
    """
    migrate data from seatgeek to new schema
    :param engine:
    :return:
    """
    engine = create_engine(env.PLANETSCALE_URL, echo=True)
    client = ScalpyrPro(env.SEATGEEK_CLIENT_ID)
    data = SeatgeekData.from_db(engine, client)
    data.performer_event_venue.to_sql('performer_event_venue', engine, if_exists='replace', index=False)


def reformat():
    engine = create_engine(env.PLANETSCALE_URL, echo=True)
    client = ScalpyrPro(env.SEATGEEK_CLIENT_ID)
    data = SeatgeekData.from_api(client)
    data.performer_event_venue.to_sql('performer_event_venue', engine, if_exists='replace', index=False)


if __name__ == '__main__':
    reformat()
