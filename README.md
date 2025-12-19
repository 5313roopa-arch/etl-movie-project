# Movie Data Pipeline

## Project Overview
- End-to-end ETL that ingests MovieLens CSVs, enriches titles through OMDb, and stores analytics-ready data in SQLite.
- Solves the need for a reproducible, auditable data engineering workflow with normalized schema, logging, and schema metadata.

## Architecture

```
Setup ──┬── download MovieLens ───┬── Extract (CSV read + year parse)
        │                        └── OMDb enrichment
        ├── Transform (clean titles + genres + validate)
        ├── Load (SQLite with transactions + idempotent writes)
        └── Verify & Query
```

- Data flows from disk (CSV) through pandas, enriched via OMDb API, and lands in SQLite tables with referential integrity.
- Logging + tqdm keep track of progress, while caching avoids redundant API calls.

## Prerequisites
- Python 3.8+
- OMDb API key (store as `OMDB_API_KEY` inside `.env`)

## Installation

```bash
# Clone repository
git clone <repo-url>
cd movie-data-pipeline

# Create virtual environment
python -m venv venv
venv\\Scripts\\activate      # Windows
# or on Unix: source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Setup environment
cp .env.example .env
# Edit .env and add your OMDb API key

# Download dataset
python setup.py
```

## Usage

```bash
# Full ETL pipeline
python src/etl.py

# Test mode (100 movies only)
python src/etl.py --test

# Skip API calls
python src/etl.py --skip-api

# Fresh start (drop existing data)
python src/etl.py --fresh
```

## Database Schema
- **movies**: Canonical movie metadata (`movieId`, `title`, `year`, `imdbId`).
- **genres**: Unique genre list.
- **movie_genres**: Junction table linking movies to genres via foreign keys.
- **ratings**: Normalized user feedback with unique constraint on `(userId, movieId, timestamp)`.
- **movie_details**: OMDb enrichment snapshot with JSON payload.

```
movies ─< movie_genres >─ genres
       └─ ratings
       └─ movie_details
```

## Design Decisions
1. **SQLite** for portability and zero-install, aligned with interview scope.
2. **Normalized genres** to avoid duplication and speed joins.
3. **OMDb caching** to honor rate limits and reduce API cost.
4. **imdbId usage** via `links.csv` for accurate enrichment.

## Assumptions
- MovieLens data quality is trusted; anomalies are logged rather than failing the pipeline.
- OMDb rate limits are acceptable for dataset volume; caching + test mode mitigate bounds.
- Missing API results remain in the database with only CSV data.
- Duplicate valid ratings (different timestamps) reflect real user activity.

## Challenges & Solutions
- **Challenge**: OMDb rate limiting  
  **Solution**: Added configurable delays, caching, and a `--test` flag.
- **Challenge**: Title mismatch between sources  
  **Solution**: Rely on `imdbId` via `links.csv` instead of fuzzy title matching.
- **Challenge**: Missing values  
  **Solution**: Graceful logging, filtering invalid ratings, and defaulting text fields to `NULL`.
- **Challenge**: Idempotency  
  **Solution**: `INSERT OR IGNORE`, unique constraints, and `INSERT OR REPLACE` for enrichment.

## Query Results
- **Top Rated Movie** (sample output):  
  - The Shawshank Redemption (1994)  
  - Average Rating: 4.45  
  - Number of Ratings: 317
- **Top Genres**: Action, Drama, Thriller, Adventure, Crime (average rating trends available via SQL).
- **Top Director**: Derived from `movie_details`; typically directors with multiple entries rise to the top.
- **Yearly Ratings**: Average rating and count per year is available for trend analysis.

## Future Improvements
1. Incremental updates instead of full reloads.
2. Parallel API enrichment with semaphore-locking rate limits.
3. Data quality dashboard or scheduled airflow job.
4. Docker container + CLI entrypoints for easier deployment.
5. Real-time ingestion hooks or streaming updates.
6. Unit & integration tests.
7. CI/CD automation.

## Project Structure

```
movie-data-pipeline/
├── data/                   # MovieLens raw CSVs
├── database/               # SQLite file is generated here
├── logs/                   # ETL logging and cache files
├── src/
│   ├── api_client.py       # OMDb integration + caching
│   ├── config.py           # Paths, constants, env loading
│   ├── database.py         # Connection + schema helpers
│   ├── extract.py          # CSV ingestion + basic parsing
│   ├── transform.py        # Cleaning, normalization, and enrichment assembly
│   ├── load.py             # Batch inserts with idempotency
│   └── etl.py              # CLI orchestration + logging
├── sql/
│   ├── schema.sql          # CREATE TABLE statements
│   └── queries.sql         # Analytical SQL queries
├── setup.py                # Dataset download + dependency bootstrap
├── requirements.txt        # Runtime dependencies
├── .env.example           # Template for secrets/config
├── .gitignore              # Ignored files and directories
└── README.md               # This documentation
```

## Sample Query Results

```
Top Rated Movie:
- The Shawshank Redemption (1994)
- Average Rating: 4.45
- Number of Ratings: 317
```

Additional query outputs are available in `sql/queries.sql` and can be run against `database/movies.db`.
