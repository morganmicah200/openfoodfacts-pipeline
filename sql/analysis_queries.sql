-- queries for logical analysis
-- ─────────────────────────────────────────────────────────────────
-- TMDB Movie Analytics Queries
-- Schema: TMDB.ANALYTICS
-- ─────────────────────────────────────────────────────────────────

-- ── 1. Top 20 highest grossing movies of all time ─────────────────
SELECT
    title,
    release_date,
    budget,
    revenue,
    profit,
    roi_pct
FROM fact_movies
WHERE revenue > 0 AND budget > 0
ORDER BY revenue DESC
LIMIT 20;

-- ── 2. Most profitable genres ─────────────────────────────────────
SELECT
    g.genre_name,
    COUNT(*) AS total_movies,
    ROUND(AVG(f.profit), 2) AS avg_profit,
    ROUND(AVG(f.roi_pct), 2) AS avg_roi_pct,
    SUM(f.revenue) AS total_revenue
FROM fact_movies f
JOIN dim_genre g ON f.genre_key = g.genre_key
WHERE f.profit IS NOT NULL
GROUP BY g.genre_name
ORDER BY avg_profit DESC;

-- ── 3. Average ROI from decade ──────────────────────────────────────
SELECT
    FLOOR(d.year / 10) * 10 AS decade,
    COUNT(*) AS total_movies,
    ROUND(AVG(f.roi_pct), 2) AS avg_roi_pct,
    ROUND(AVG(f.budget), 2) AS avg_budget,
    ROUND(AVG(f.revenue), 2) AS avg_revenue
FROM fact_movies f
JOIN dim_release_date d ON f.date_key = d.date_key
WHERE f.roi_pct IS NOT NULL
GROUP BY decade
ORDER BY decade;

-- ── 4. Top production companies by total revenue ──────────────────
SELECT
    c.company_name,
    COUNT(*) AS total_movies,
    SUM(f.revenue) AS total_revenue,
    ROUND(AVG(f.vote_average), 2) AS avg_rating
FROM fact_movies f
JOIN dim_production_company c ON f.company_key = c.company_key
WHERE f.revenue > 0
GROUP BY c.company_name
ORDER BY total_revenue DESC
LIMIT 20;

-- ── 5. Highest rated movies with significant vote counts ──────────
SELECT
    title,
    release_date,
    vote_average,
    vote_count,
    runtime
FROM fact_movies
WHERE vote_count >= 10000
ORDER BY vote_average DESC
LIMIT 25;

-- ── 6. Most prolific languages in film ───────────────────────────
SELECT
    l.language_name,
    l.iso_639_1,
    COUNT(*) AS total_movies,
    ROUND(AVG(f.vote_average), 2) AS avg_rating,
    ROUND(AVG(f.popularity), 2) AS avg_popularity
FROM fact_movies f
JOIN dim_language l ON f.language_key = l.language_key
GROUP BY l.language_name, l.iso_639_1
ORDER BY total_movies DESC
LIMIT 20;

-- ── 7. Movies with highest budget that lost money ────────────────
SELECT
    title,
    release_date,
    budget,
    revenue,
    profit,
    roi_pct
FROM fact_movies
WHERE profit < 0
ORDER BY budget DESC
LIMIT 20;

-- ── 8. Movie output by year ───────────────────────────────────────
SELECT
    d.year,
    COUNT(*) AS total_movies,
    ROUND(AVG(f.vote_average), 2) AS avg_rating,
    ROUND(AVG(f.budget), 2) AS avg_budget,
    ROUND(AVG(f.runtime), 2) AS avg_runtime
FROM fact_movies f
JOIN dim_release_date d ON f.date_key = d.date_key
WHERE d.year >= 1980
GROUP BY d.year
ORDER BY d.year;

-- ── 9. Best month to release a movie by avg revenue ──────────────
SELECT
    d.month,
    COUNT(*) AS total_movies,
    ROUND(AVG(f.revenue), 2) AS avg_revenue,
    ROUND(AVG(f.roi_pct), 2) AS avg_roi_pct
FROM fact_movies f
JOIN dim_release_date d ON f.date_key = d.date_key
WHERE f.revenue > 0 AND f.budget > 0
GROUP BY d.month
ORDER BY avg_revenue DESC;

-- ── 10. Runtime vs rating correlation ────────────────────────────
SELECT
    CASE
        WHEN runtime < 60 THEN 'Under 60 min'
        WHEN runtime BETWEEN 60 AND 90 THEN '60-90 min'
        WHEN runtime BETWEEN 91 AND 120 THEN '91-120 min'
        WHEN runtime BETWEEN 121 AND 150 THEN '121-150 min'
        WHEN runtime > 150 THEN 'Over 150 min'
    END AS runtime_bucket,
    COUNT(*) AS total_movies,
    ROUND(AVG(vote_average), 2) AS avg_rating,
    ROUND(AVG(popularity), 2) AS avg_popularity
FROM fact_movies
WHERE runtime > 0 AND vote_count >= 100
GROUP BY runtime_bucket
ORDER BY avg_rating DESC;

-- ── 11. Top 10 most popular movies released in the last 5 years ──
SELECT
    title,
    release_date,
    popularity,
    vote_average,
    vote_count
FROM fact_movies f
JOIN dim_release_date d ON f.date_key = d.date_key
WHERE d.year >= 2020
ORDER BY popularity DESC
LIMIT 10;

-- ── 12. Budget tier performance ───────────────────────────────────
SELECT
    CASE
        WHEN budget < 1000000 THEN 'Micro (<$1M)'
        WHEN budget BETWEEN 1000000 AND 10000000 THEN 'Low ($1M-$10M)'
        WHEN budget BETWEEN 10000001 AND 50000000 THEN 'Mid ($10M-$50M)'
        WHEN budget BETWEEN 50000001 AND 100000000 THEN 'High ($50M-$100M)'
        WHEN budget > 100000000 THEN 'Blockbuster (>$100M)'
    END AS budget_tier,
    COUNT(*) AS total_movies,
    ROUND(AVG(roi_pct), 2) AS avg_roi_pct,
    ROUND(AVG(vote_average), 2) AS avg_rating,
    SUM(profit) AS total_profit
FROM fact_movies
WHERE budget > 0 AND revenue > 0
GROUP BY budget_tier
ORDER BY avg_roi_pct DESC;