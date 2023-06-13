from sqlalchemy import create_engine
from src.scalpyr import ScalpyrPro
from src.fetch_data.table_builders import SeatgeekData
import env
from flask import Flask
import pydantic


# pydantic class that represents the incoming request body:
# event_ids: list[str] (optional)
class TrackEvents(pydantic.BaseModel):
    event_ids: list[str] = None


app = Flask(__name__)


@app.route('/', methods=['POST'])
def handle_request():
    client = ScalpyrPro(env.SEATGEEK_CLIENT_ID)
    seatgeek_data = SeatgeekData.from_api(client)
    engine = create_engine(env.PLANETSCALE_URL, echo=True)
    seatgeek_data.push_to_db(engine)
    return 'Updated database'


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
