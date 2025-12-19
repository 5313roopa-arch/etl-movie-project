import argparse
import logging
import sys
from time import perf_counter

from tqdm import tqdm

from .api_client import OMDbClient
from .config import (
    API_CACHE_FILE,
    LOG_FILE,
    OMDB_API_KEY,
    TEST_MODE_LIMIT,
)
from .database import (
    create_schema,
    drop_database,
    get_connection,
)
from .extract import extract_csv_data
from .load import (
    insert_genres,
    insert_movie_details,
    insert_movie_genres,
    insert_movies,
    insert_ratings,
)
from .transform import transform_data

logger = logging.getLogger(__name__)


def _setup_logging(verbose: bool) -> None:
    log_level = logging.DEBUG if verbose else logging.INFO
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    API_CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
    handlers = [
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
    ]
    logging.basicConfig(
        level=log_level,
        format='[%(asctime)s] %(levelname)s %(name)s: %(message)s',
        handlers=handlers,
    )


def _fetch_api_payloads(imdb_ids, client, limit=None):
    payloads = {}
    for idx, imdb_id in enumerate(tqdm(imdb_ids, desc='Fetching OMDb data', unit='movie')):
        if limit and idx >= limit:
            break
        payload = client.fetch_movie(imdb_id)
        if payload:
            payloads[imdb_id] = payload
    return payloads


def _verify_connection(connection) -> None:
    cursor = connection.cursor()
    cursor.execute('PRAGMA foreign_key_check;')
    violations = cursor.fetchall()
    if violations:
        logger.warning('Foreign key violations detected: %s', violations)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog='Movie Data Pipeline',
        description='Complete ETL pipeline for MovieLens + OMDb enrichment',
    )
    parser.add_argument('--test', action='store_true', help='Run with limited API calls')
    parser.add_argument('--skip-api', action='store_true', help='Skip OMDb enrichment entirely')
    parser.add_argument('--fresh', action='store_true', help='Recreate the database from scratch')
    parser.add_argument('--verbose', action='store_true', help='Enable DEBUG logging')
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    _setup_logging(args.verbose)
    start = perf_counter()
    logger.info('START ETL PIPELINE')

    if args.fresh:
        drop_database()
        logger.info('Dropped existing database')

    connection = get_connection()
    create_schema(connection)
    logger.info('Schema ensured')

    extracted = extract_csv_data()
    movies_df = extracted['movies']
    ratings_df = extracted['ratings']

    api_payloads = {}
    if not args.skip_api and OMDB_API_KEY:
        client = OMDbClient(api_key=OMDB_API_KEY)
        imdb_ids = movies_df['imdbId'].dropna().unique().tolist()
        limit = TEST_MODE_LIMIT if args.test else None
        api_payloads = _fetch_api_payloads(imdb_ids, client, limit)
        logger.info('Fetched API payloads for %d movies', len(api_payloads))
    elif not OMDB_API_KEY:
        logger.warning('OMDb API key missing; skipping enrichment')

    transformed = transform_data(movies_df, ratings_df, api_payloads)

    movie_rows = insert_movies(connection, transformed['movies'])
    genre_rows = insert_genres(connection, transformed['genres'])
    movie_genre_rows = insert_movie_genres(connection, transformed['movie_genres'])
    ratings_rows = insert_ratings(connection, transformed['ratings'])
    detail_rows = insert_movie_details(connection, transformed['movie_details'])

    _verify_connection(connection)

    elapsed = perf_counter() - start
    logger.info('END ETL PIPELINE (%.2fs)', elapsed)
    print(f'Total movies processed: {len(transformed["movies"])}')
    print(f'Movies with API data: {len(transformed["movie_details"])}')
    print(f'Movies without API data: {len(transformed["movies"]) - len(transformed["movie_details"])}')
    print(f'Total ratings loaded: {ratings_rows}')
    print(f'Total genres found: {len(transformed["genres"])}')
    print(f'Execution time: {elapsed:.2f}s')


if __name__ == '__main__':
    main()
