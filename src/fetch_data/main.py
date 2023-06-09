from sqlalchemy import create_engine
from src.scalpyr import Scalpyr
from src.fetch_data.table_builders import SeatgeekData
import env
import socket


def main():
    # with socket on host 0.0.0.0, port 8080, the app is compatible with Google Cloud Run
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("0.0.0.0", 8080))
        s.listen()
        client = Scalpyr(env.SEATGEEK_CLIENT_ID)
        seatgeek_data = SeatgeekData.from_api(client)
        engine = create_engine(env.PLANETSCALE_URL, echo=True)
        seatgeek_data.push_to_db(engine)


if __name__ == "__main__":
    main()
