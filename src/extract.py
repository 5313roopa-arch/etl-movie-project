import logging
import re
from os import PathLike
from typing import Dict

import pandas as pd

from .config import LINKS_CSV, MOVIES_CSV, RATINGS_CSV

logger = logging.getLogger(__name__)


def _extract_year(title: str) -> int | None:
    if not isinstance(title, str):
        return None
    match = re.search(r'\((\d{4})\)\s*$', title.strip())
    if match:
        try:
            return int(match.group(1))
        except ValueError:
            return None
    return None


def _format_imdb_id(imdb_id: str | float) -> str | None:
    if pd.isna(imdb_id):
        return None
    imdb_str = str(int(imdb_id)) if isinstance(imdb_id, float) else str(imdb_id)
    if not imdb_str:
        return None
    imdb_str = imdb_str.zfill(7)
    return f'tt{imdb_str}'


def _read_csv(path: str | PathLike, **read_kwargs) -> pd.DataFrame:
    try:
        return pd.read_csv(path, **read_kwargs)
    except FileNotFoundError:
        logger.error('CSV file not found: %s', path)
        return pd.DataFrame()


def extract_csv_data() -> Dict[str, pd.DataFrame]:
    movies = _read_csv(MOVIES_CSV)
    ratings = _read_csv(RATINGS_CSV)
    links = _read_csv(LINKS_CSV)

    if movies.empty or links.empty:
        logger.warning('Movie or link data missing; %d movies found, %d links found', len(movies), len(links))

    movie_link = movies.merge(
        links[['movieId', 'imdbId']],
        on='movieId',
        how='left',
        validate='one_to_one',
    )

    movie_link['year'] = movie_link['title'].apply(_extract_year)
    missing_years = movie_link['year'].isna().sum()
    if missing_years:
        logger.debug('%d movies missing parsed year', missing_years)

    movie_link['imdbId'] = movie_link['imdbId'].apply(_format_imdb_id)
    missing_imdb = movie_link['imdbId'].isna().sum()
    if missing_imdb:
        logger.debug('%d movies missing imdbId after formatting', missing_imdb)

    return {
        'movies': movie_link,
        'ratings': ratings,
    }
