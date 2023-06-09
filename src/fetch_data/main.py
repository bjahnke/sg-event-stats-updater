from sqlalchemy import create_engine
from src.scalpyr import Scalpyr
from src.fetch_data.table_builders import SeatgeekData
import env


def main():
    client = Scalpyr(env.SEATGEEK_CLIENT_ID)
    seatgeek_data = SeatgeekData.from_api(client)
    engine = create_engine(env.PLANETSCALE_URL, echo=True)
    seatgeek_data.push_to_db(engine)


if __name__ == '__main__':
    main()
