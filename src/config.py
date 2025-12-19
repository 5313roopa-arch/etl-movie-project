import os
from pathlib import Path
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / '.env')

# API settings
OMDB_API_KEY = os.getenv('OMDB_API_KEY')
OMDB_BASE_URL = 'http://www.omdbapi.com/'
API_RATE_LIMIT_DELAY = float(os.getenv('API_RATE_LIMIT_DELAY', 0.25))
API_MAX_RETRIES = int(os.getenv('API_MAX_RETRIES', 3))

# Database settings
DEFAULT_DATABASE_PATH = BASE_DIR / 'database' / 'movies.db'
DATABASE_PATH = Path(os.getenv('DATABASE_PATH', DEFAULT_DATABASE_PATH))

# Data paths
DATA_DIR = Path(os.getenv('DATA_DIR', BASE_DIR / 'data' / 'ml-latest-small'))
MOVIES_CSV = DATA_DIR / 'movies.csv'
RATINGS_CSV = DATA_DIR / 'ratings.csv'
LINKS_CSV = DATA_DIR / 'links.csv'

# ETL settings
BATCH_SIZE = int(os.getenv('BATCH_SIZE', 1000))
TEST_MODE_LIMIT = int(os.getenv('TEST_MODE_LIMIT', 100))

# Logging and caching
LOG_FILE = BASE_DIR / 'logs' / 'etl.log'
API_CACHE_FILE = BASE_DIR / 'logs' / 'api_cache.json'
