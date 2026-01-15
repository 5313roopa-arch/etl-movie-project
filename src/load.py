import logging
from itertools import islice
from typing import Iterable, List, Tuple

import pandas as pd

from src.database import transaction
from src.config import BATCH_SIZE

logger = logging.getLogger(__name__)


def _chunked(iterable: Iterable, size: int) -> Iterable[List]:
    iterator = iter(iterable)
    while True:
        chunk = list(islice(iterator, size))
        if not chunk:
            break
        yield chunk


def _execute_chunks(cursor, query: str, rows: Iterable[Tuple], batch_size: int) -> None:
    for chunk in _chunked(rows, batch_size):
        cursor.executemany(query, chunk)


def insert_movies(connection, movies: pd.DataFrame, batch_size: int = BATCH_SIZE) -> int:
    before = connection.total_changes
    rows = []
    for _, movie in movies.iterrows():
        rows.append(
            (
                int(movie['movieId']),
                movie['cleanTitle'],
                int(movie['year']) if pd.notna(movie['year']) else None,
                movie['imdbId'],
            )
        )
    with transaction(connection) as cursor:
        _execute_chunks(
            cursor,
            'INSERT OR IGNORE INTO movies (movieId, title, year, imdbId) VALUES (?, ?, ?, ?)',
            rows,
            batch_size,
        )
    inserted = connection.total_changes - before
    logger.info('Inserted %d movie rows', inserted)
    return inserted


def insert_genres(connection, genres: List[str], batch_size: int = BATCH_SIZE) -> int:
    before = connection.total_changes
    with transaction(connection) as cursor:
        _execute_chunks(
            cursor,
            'INSERT OR IGNORE INTO genres (genreName) VALUES (?)',
            ((genre,) for genre in genres),
            batch_size,
        )
    inserted = connection.total_changes - before
    logger.info('Inserted %d genre rows', inserted)
    return inserted


def _genre_map(connection) -> dict:
    cursor = connection.cursor()
    cursor.execute('SELECT genreId, genreName FROM genres')
    return {row['genreName']: row['genreId'] for row in cursor.fetchall()}


def insert_movie_genres(connection, movie_genres: pd.DataFrame, batch_size: int = BATCH_SIZE) -> int:
    genre_lookup = _genre_map(connection)
    before = connection.total_changes
    rows = []
    for _, row in movie_genres.iterrows():
        genre_id = genre_lookup.get(row['genreName'])
        if genre_id:
            rows.append((int(row['movieId']), genre_id))
    with transaction(connection) as cursor:
        _execute_chunks(
            cursor,
            'INSERT OR IGNORE INTO movie_genres (movieId, genreId) VALUES (?, ?)',
            rows,
            batch_size,
        )
    inserted = connection.total_changes - before
    logger.info('Inserted %d movie_genre rows', inserted)
    return inserted


def insert_ratings(connection, ratings: pd.DataFrame, batch_size: int = BATCH_SIZE) -> int:
    before = connection.total_changes
    rows = []
    for _, row in ratings.iterrows():
        rows.append(
            (
                int(row['userId']),
                int(row['movieId']),
                float(row['rating']),
                int(row['timestamp'].timestamp()),
            )
        )
    with transaction(connection) as cursor:
        _execute_chunks(
            cursor,
            'INSERT OR IGNORE INTO ratings (userId, movieId, rating, timestamp) VALUES (?, ?, ?, ?)',
            rows,
            batch_size,
        )
    inserted = connection.total_changes - before
    logger.info('Inserted %d rating rows', inserted)
    return inserted


def insert_movie_details(connection, movie_details: List[dict], batch_size: int = BATCH_SIZE) -> int:
    before = connection.total_changes
    rows = []
    for detail in movie_details:
        rows.append(
            (
                int(detail['movieId']),
                detail.get('director'),
                detail.get('plot'),
                detail.get('boxOffice'),
                detail.get('imdbRating'),
                detail.get('runtime'),
                detail.get('actors'),
                detail.get('country'),
                detail.get('language'),
                detail.get('awards'),
                detail.get('apiResponseJson'),
            )
        )
    with transaction(connection) as cursor:
        _execute_chunks(
            cursor,
            '''
            INSERT OR REPLACE INTO movie_details (
                movieId, director, plot, boxOffice, imdbRating,
                runtime, actors, country, language, awards, apiResponseJson
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            rows,
            batch_size,
        )
    inserted = connection.total_changes - before
    logger.info('Inserted %d movie detail rows', inserted)
    return inserted
