-- Fact table: movies
-- Joins movies_staging to all dimension tables via surrogate keys
-- Contains all measurable metrics for analysis

with movies as (
    select * from {{ ref('movies_staging') }}
),

dim_genre as (
    select * from {{ ref('dim_genre') }}
),

dim_language as (
    select * from {{ ref('dim_language') }}
),

dim_release_date as (
    select * from {{ ref('dim_release_date') }}
),

dim_production_company as (
    select * from {{ ref('dim_production_company') }}
)

select
    m.movie_id,
    m.imdb_id,
    m.title,
    m.original_title,
    m.status,
    m.overview,
    m.tagline,
    m.homepage,

    -- Dimension foreign keys set
    g.genre_key,
    l.language_key,
    d.date_key,
    c.company_key,

    -- Measures
    m.budget,
    m.revenue,
    m.runtime,
    m.vote_average,
    m.vote_count,
    m.popularity,

    -- Derived measures
    case when m.budget > 0 and m.revenue > 0
        then m.revenue - m.budget
        else null
    end as profit,

    case when m.budget > 0 and m.revenue > 0
        then round((m.revenue - m.budget) / nullif(m.budget, 0) * 100, 2)
        else null
    end as roi_pct

from movies m
left join dim_genre g
    on try_cast(split_part(m.genre_ids, '|', 1) as integer) = g.genre_id
left join dim_language l
    on m.original_language = l.iso_639_1
left join dim_release_date d
    on m.release_date = d.release_date
left join dim_production_company c
    on try_cast(split_part(m.company_ids, '|', 1) as integer) = c.company_id