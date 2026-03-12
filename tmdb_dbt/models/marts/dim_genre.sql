-- Dimension table: genres
-- Unpacks the pipe-delimited genre_ids and genre_names from movies_staging

with genres_raw as (
    select
        trim(f.value::string) as genre_id,
        trim(g.value::string) as genre_name
    from {{ ref('movies_staging') }},
        lateral flatten(input => split(genre_ids, '|')) f,
        lateral flatten(input => split(genre_names, '|')) g
    where genre_ids != ''
        and genre_names != ''
        and f.index = g.index
),

deduped as (
    select distinct
        try_cast(genre_id as integer) as genre_id,
        min(genre_name) as genre_name
    from genres_raw
    where try_cast(genre_id as integer) is not null
    group by genre_id
)

select
    row_number() over (order by genre_id) as genre_key,
    genre_id,
    genre_name
from deduped