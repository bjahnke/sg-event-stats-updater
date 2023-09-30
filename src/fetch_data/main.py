from sqlalchemy import create_engine
from src.scalpyr import ScalpyrPro
from src.fetch_data.table_builders import SeatgeekData
import env
from flask import Flask
import pydantic
from pymongo import MongoClient
from src.schemas.models import WatchListResponse
import certifi
import src.watchlist

ca = certifi.where()


# pydantic class that represents the incoming request body:
# event_ids: list[str] (optional)
class TrackEvents(pydantic.BaseModel):
    event_ids: list[str] = None


app = Flask(__name__)


@app.route('/', methods=['POST'])
def handle_request():
    client = ScalpyrPro(env.SEATGEEK_CLIENT_ID)
    watchlist_client = src.watchlist.MongoWatchlistClient(env.WATCHLIST_API_KEY)
    watchlist = watchlist_client.get_latest('event-tracking')
    seatgeek_data = SeatgeekData.from_watchlist(client, **watchlist)
    engine = create_engine(env.PLANETSCALE_URL)
    seatgeek_data.push_to_db(engine)
    return 'Updated database'


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
