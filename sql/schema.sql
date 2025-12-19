PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS movies (
    movieId INTEGER PRIMARY KEY,
    title TEXT NOT NULL,
    year INTEGER,
    imdbId TEXT
);

CREATE TABLE IF NOT EXISTS genres (
    genreId INTEGER PRIMARY KEY AUTOINCREMENT,
    genreName TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS movie_genres (
    movieId INTEGER NOT NULL,
    genreId INTEGER NOT NULL,
    PRIMARY KEY (movieId, genreId),
    FOREIGN KEY (movieId) REFERENCES movies(movieId),
    FOREIGN KEY (genreId) REFERENCES genres(genreId)
);

CREATE TABLE IF NOT EXISTS ratings (
    ratingId INTEGER PRIMARY KEY AUTOINCREMENT,
    userId INTEGER NOT NULL,
    movieId INTEGER NOT NULL,
    rating REAL NOT NULL CHECK (rating BETWEEN 0.5 AND 5.0),
    timestamp INTEGER NOT NULL,
    FOREIGN KEY (movieId) REFERENCES movies(movieId),
    UNIQUE (userId, movieId, timestamp)
);

CREATE TABLE IF NOT EXISTS movie_details (
    movieId INTEGER PRIMARY KEY,
    director TEXT,
    plot TEXT,
    boxOffice TEXT,
    imdbRating TEXT,
    runtime TEXT,
    actors TEXT,
    country TEXT,
    language TEXT,
    awards TEXT,
    apiResponseJson TEXT,
    FOREIGN KEY (movieId) REFERENCES movies(movieId)
);
