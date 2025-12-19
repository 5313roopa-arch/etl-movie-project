-- Query 1: Movie with highest average rating (minimum 50 ratings)
SELECT
    m.movieId,
    m.title,
    ROUND(AVG(r.rating), 2) AS average_rating,
    COUNT(r.rating) AS rating_count
FROM movies m
JOIN ratings r ON m.movieId = r.movieId
GROUP BY m.movieId, m.title
HAVING COUNT(r.rating) >= 50
ORDER BY average_rating DESC
LIMIT 1;

-- Query 2: Top 5 genres by average rating
SELECT
    g.genreName,
    ROUND(AVG(r.rating), 2) AS average_rating
FROM genres g
JOIN movie_genres mg ON g.genreId = mg.genreId
JOIN ratings r ON mg.movieId = r.movieId
GROUP BY g.genreId, g.genreName
ORDER BY average_rating DESC
LIMIT 5;

-- Query 3: Director with the most movies
SELECT
    director,
    COUNT(*) AS movie_count
FROM movie_details
WHERE director IS NOT NULL AND director != ''
GROUP BY director
ORDER BY movie_count DESC
LIMIT 1;

-- Query 4: Average rating by release year
SELECT
    m.year,
    ROUND(AVG(r.rating), 2) AS average_rating,
    COUNT(r.rating) AS rating_count
FROM movies m
JOIN ratings r ON m.movieId = r.movieId
WHERE m.year IS NOT NULL
GROUP BY m.year
ORDER BY m.year ASC;
