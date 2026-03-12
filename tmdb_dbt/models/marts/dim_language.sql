-- Dimension table: languages
-- Unpacks the pipe-delimited language codes and names from movies_staging

with languages_raw as (
    select
        trim(c.value::string) as iso_639_1,
        trim(n.value::string) as language_name
    from {{ ref('movies_staging') }},
        lateral flatten(input => split(language_codes, '|')) c,
        lateral flatten(input => split(language_names, '|')) n
    where language_codes != ''
        and language_names != ''
        and c.index = n.index
),

deduped as (
    select distinct
        iso_639_1,
        min(language_name) as language_name
    from languages_raw
    where iso_639_1 is not null
        and iso_639_1 != ''
    group by iso_639_1
)

select
    row_number() over (order by iso_639_1) as language_key,
    iso_639_1,
    language_name
from deduped