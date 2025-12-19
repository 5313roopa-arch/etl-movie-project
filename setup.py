import argparse
import logging
import shutil
import subprocess
import sys
from pathlib import Path
from urllib.request import urlopen
from zipfile import ZipFile

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / 'data' / 'ml-latest-small'
ZIP_PATH = BASE_DIR / 'data' / 'ml-latest-small.zip'
ENV_FILE = BASE_DIR / '.env'
REQUIREMENTS = BASE_DIR / 'requirements.txt'
LOG_FILE = BASE_DIR / 'logs' / 'etl.log'

logger = logging.getLogger(__name__)


def ensure_directories() -> None:
    (BASE_DIR / 'data').mkdir(parents=True, exist_ok=True)
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    (BASE_DIR / 'database').mkdir(parents=True, exist_ok=True)
    (BASE_DIR / 'logs').mkdir(parents=True, exist_ok=True)


def download_dataset(force: bool = False) -> None:
    if ZIP_PATH.exists() and not force:
        logger.info('Dataset archive already exists at %s', ZIP_PATH)
        return

    url = 'https://files.grouplens.org/datasets/movielens/ml-latest-small.zip'
    logger.info('Downloading MovieLens dataset from %s', url)
    with urlopen(url, timeout=30) as response, open(ZIP_PATH, 'wb') as handle:
        shutil.copyfileobj(response, handle)
    logger.info('Downloaded dataset to %s', ZIP_PATH)


def extract_dataset(force: bool = False) -> None:
    if force and DATA_DIR.exists():
        shutil.rmtree(DATA_DIR)
    with ZipFile(ZIP_PATH) as archive:
        archive.extractall(BASE_DIR / 'data')
    logger.info('Extracted dataset to %s', DATA_DIR)


def install_dependencies() -> None:
    if REQUIREMENTS.exists():
        logger.info('Installing dependencies from %s', REQUIREMENTS)
        subprocess.run(
            [sys.executable, '-m', 'pip', 'install', '-r', str(REQUIREMENTS)],
            check=True,
        )


def validate_environment() -> None:
    if not ENV_FILE.exists():
        logger.warning('.env file missing; create one based on .env.example')
        return
    from dotenv import dotenv_values

    config = dotenv_values(ENV_FILE)
    if not config.get('OMDB_API_KEY'):
        logger.warning('OMDb API key missing from .env; the ETL will skip enrichment')


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Prepare movie data pipeline project')
    parser.add_argument('--force', action='store_true', help='Force download and extraction')
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s: %(message)s')
    args = parse_args()
    ensure_directories()
    download_dataset(force=args.force)
    extract_dataset(force=args.force)
    install_dependencies()
    validate_environment()
    LOG_FILE.touch(exist_ok=True)


if __name__ == '__main__':
    main()
