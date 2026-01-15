import json
import logging
import time
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

import requests

from src.config import API_CACHE_FILE, API_MAX_RETRIES, API_RATE_LIMIT_DELAY, OMDB_BASE_URL, OMDB_API_KEY

logger = logging.getLogger(__name__)


class OMDbClient:
    def __init__(
        self,
        api_key: Optional[str] = None,
        rate_limit_delay: float = API_RATE_LIMIT_DELAY,
        max_retries: int = API_MAX_RETRIES,
        cache_file: Path = API_CACHE_FILE,
    ) -> None:
        if not api_key:
            raise ValueError('OMDb API key is required')
        self.api_key = api_key
        self.rate_limit_delay = rate_limit_delay
        self.max_retries = max_retries
        self.session = requests.Session()
        self.cache_file = cache_file
        self.cache_file.parent.mkdir(parents=True, exist_ok=True)
        self._cache = self._load_cache()

    def _load_cache(self) -> Dict[str, Dict[str, Any]]:
        if self.cache_file.exists():
            try:
                return json.loads(self.cache_file.read_text())
            except json.JSONDecodeError:
                logger.warning('Cache file %s is corrupt; starting fresh', self.cache_file)
        return {}

    def _persist_cache(self) -> None:
        self.cache_file.write_text(json.dumps(self._cache, ensure_ascii=False))

    def _build_params(self, imdb_id: str) -> Dict[str, str]:
        return {'i': imdb_id, 'apikey': self.api_key}

    def fetch_movie(self, imdb_id: str) -> Optional[Dict[str, Any]]:
        if imdb_id in self._cache:
            return self._cache[imdb_id]

        for attempt in range(1, self.max_retries + 1):
            try:
                response = self.session.get(OMDB_BASE_URL, params=self._build_params(imdb_id), timeout=10)
                response.raise_for_status()
                payload = response.json()
            except requests.RequestException as exc:
                logger.warning('OMDb request failed for %s (attempt %d/%d): %s', imdb_id, attempt, self.max_retries, exc)
                self._backoff(attempt)
                continue

            if payload.get('Response') == 'False':
                reason = payload.get('Error', 'unknown error')
                logger.info('OMDb responded False for %s: %s', imdb_id, reason)
                self._cache[imdb_id] = {'Response': 'False', 'Error': reason}
                self._persist_cache()
                return None

            self._cache[imdb_id] = payload
            self._persist_cache()
            time.sleep(self.rate_limit_delay)
            return payload

        logger.error('OMDb failed after %d attempts for %s', self.max_retries, imdb_id)
        return None

    def _backoff(self, attempt: int) -> None:
        delay = min(5, 0.5 * 2 ** (attempt - 1))
        time.sleep(delay)

    def fetch_bulk(self, imdb_ids: Iterable[str], limit: Optional[int] = None) -> Dict[str, Dict[str, Any]]:
        result: Dict[str, Dict[str, Any]] = {}
        counter = 0
        for imdb_id in imdb_ids:
            if limit is not None and counter >= limit:
                break
            if not imdb_id:
                continue
            payload = self.fetch_movie(imdb_id)
            if payload:
                result[imdb_id] = payload
            counter += 1
        return result
