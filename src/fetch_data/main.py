from sqlalchemy import create_engine
from src.scalpyr import ScalpyrPro
from src.fetch_data.table_builders import SeatgeekData
import env
from flask import Flask
import pydantic
from pymongo import MongoClient
from src.schemas.models import WatchListResponse
import certifi

ca = certifi.where()


# pydantic class that represents the incoming request body:
# event_ids: list[str] (optional)
class TrackEvents(pydantic.BaseModel):
    event_ids: list[str] = None


app = Flask(__name__)


@app.route('/', methods=['POST'])
def handle_request():
    client = ScalpyrPro(env.SEATGEEK_CLIENT_ID)
    mongo_client = MongoClient(env.MONGO_URL, tlsCAFile=ca)
    db = mongo_client['event-tracking']
    collection = db['watchlist']
    latest_entry = collection.find_one({"username": "bjahnke71"}, sort=[("_id", -1)])
    watchlist = WatchListResponse(**latest_entry)
    seatgeek_data = SeatgeekData.from_watchlist(
        client,
        venue_id=watchlist.venue_id,
        performer_id=watchlist.performer_id,
        event_id=watchlist.event_id
    )
    engine = create_engine(env.PLANETSCALE_URL, echo=True)
    seatgeek_data.push_to_db(engine)
    return 'Updated database'


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
