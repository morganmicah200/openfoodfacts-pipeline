-- Staging model: selects directly from the raw stg_movies table loaded by load.py
-- Casts and cleans columns before dimensional modeling in marts

with source as (
    select * from {{ source('raw', 'stg_movies') }}
)

select
    movie_id,
    imdb_id,
    title,
    original_title,
    original_language,
    release_date::date as release_date,
    status,
    budget,
    revenue,
    runtime,
    vote_average,
    vote_count,
    popularity,
    genre_ids,
    genre_names,
    language_codes,
    language_names,
    company_ids,
    company_names,
    country_codes,
    overview,
    tagline,
    homepage
from source