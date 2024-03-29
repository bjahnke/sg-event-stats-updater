"""
Desc:
env_auto.py is generated from .env by the `invoke buildenvpy` task.
it's purpose is to provide IDE support for environment variables.
"""

import os
from dotenv import load_dotenv
load_dotenv()


TICKETMASTER_API_KEY = os.environ.get('TICKETMASTER_API_KEY')
TICKETMASTER_API_SECRET = os.environ.get('TICKETMASTER_API_SECRET')
SEATGEEK_API_SECRET = os.environ.get('SEATGEEK_API_SECRET')
SEATGEEK_CLIENT_ID = os.environ.get('SEATGEEK_CLIENT_ID')
PLANETSCALE_URL = os.environ.get('PLANETSCALE_URL')
IMAGE_NAME = os.environ.get('IMAGE_NAME')
DOCKER_TOKEN = os.environ.get('DOCKER_TOKEN')
DOCKER_USERNAME = os.environ.get('DOCKER_USERNAME')
GCR_PROJECT_ID = os.environ.get('GCR_PROJECT_ID')
MONGO_URL = os.environ.get('MONGO_URL')
WATCHLIST_API_KEY = os.environ.get('WATCHLIST_API_KEY')
