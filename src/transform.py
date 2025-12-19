import json
import logging
import re
from typing import Any, Dict, List

import pandas as pd

logger = logging.getLogger(__name__)


def _clean_title(title: str) -> str:
    if not isinstance(title, str):
        return ''
    normalized = re.sub(r'\s*\(\d{4}\)\s*$', '', title).strip()
    return re.sub(r'\s{2,}', ' ', normalized)


def _parse_genres(genres: str) -> List[str]:
    if not isinstance(genres, str):
        return []
    if genres.strip().lower() == '(no genres listed)':
        return []
    return [genre.strip() for genre in genres.split('|') if genre.strip()]


def _validate_ratings_dataframe(ratings: pd.DataFrame) -> pd.DataFrame:
    original_count = len(ratings)
    ratings = ratings.drop_duplicates(subset=['userId', 'movieId', 'timestamp'])
    ratings['rating'] = pd.to_numeric(ratings['rating'], errors='coerce')
    ratings = ratings[(ratings['rating'] >= 0.5) & (ratings['rating'] <= 5.0)]
    ratings['timestamp'] = pd.to_numeric(ratings['timestamp'], errors='coerce')
    ratings = ratings.dropna(subset=['rating', 'timestamp'])
    ratings['timestamp'] = pd.to_datetime(ratings['timestamp'], unit='s', origin='unix', errors='coerce')
    ratings = ratings.dropna(subset=['timestamp'])
    logger.info('Cleaned ratings: %d -> %d rows after validation', original_count, len(ratings))
    return ratings


def clean_movies_dataframe(movies: pd.DataFrame) -> pd.DataFrame:
    movies = movies.drop_duplicates(subset='movieId')
    movies['cleanTitle'] = movies['title'].apply(_clean_title)
    movies['year'] = pd.to_numeric(movies['year'], errors='coerce').astype('Int64')
    movies['genresList'] = movies['genres'].apply(_parse_genres)
    return movies


def build_genres_lookup(movies: pd.DataFrame) -> List[str]:
    unique_genres = sorted({genre for row in movies['genresList'] for genre in row})
    logger.info('Discovered %d unique genres', len(unique_genres))
    return unique_genres


def map_movie_genres(movies: pd.DataFrame) -> pd.DataFrame:
    records = []
    for movie_id, genres in zip(movies['movieId'], movies['genresList']):
        for genre in genres:
            records.append({'movieId': int(movie_id), 'genreName': genre})
    return pd.DataFrame(records)


def build_movie_details(movies: pd.DataFrame, api_data: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
    details = []
    movie_id_lookup: Dict[str, int] = dict(zip(movies['imdbId'], movies['movieId']))
    for imdb_id, payload in api_data.items():
        movie_id = movie_id_lookup.get(imdb_id)
        if not movie_id:
            continue
        if payload.get('Response') == 'False':
            continue
        details.append(
            {
                'movieId': int(movie_id),
                'director': payload.get('Director'),
                'plot': payload.get('Plot'),
                'boxOffice': payload.get('BoxOffice'),
                'imdbRating': payload.get('imdbRating'),
                'runtime': payload.get('Runtime'),
                'actors': payload.get('Actors'),
                'country': payload.get('Country'),
                'language': payload.get('Language'),
                'awards': payload.get('Awards'),
                'apiResponseJson': json.dumps(payload, ensure_ascii=False),
            }
        )
    logger.info('Prepared %d movie details from OMDb payloads', len(details))
    return details


def transform_data(raw_movies: pd.DataFrame, raw_ratings: pd.DataFrame, api_payloads: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    movies = clean_movies_dataframe(raw_movies)
    ratings = _validate_ratings_dataframe(raw_ratings)
    genres = build_genres_lookup(movies)
    movie_genres = map_movie_genres(movies)
    details = build_movie_details(movies, api_payloads)
    if len(genres) == 0:
        logger.warning('No genres parsed from dataset')
    if len(details) == 0:
        logger.warning('No API-enriched movies available')
    return {
        'movies': movies,
        'ratings': ratings,
        'genres': genres,
        'movie_genres': movie_genres,
        'movie_details': details,
    }
