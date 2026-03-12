-- Dimension table: release dates
-- Extracts year, month, quarter, and day from release_date in movies_staging

with dates as (
    select distinct
        release_date
    from {{ ref('movies_staging') }}
    where release_date is not null
)

select
    row_number() over (order by release_date) as date_key,
    release_date,
    year(release_date)      as year,
    month(release_date)     as month,
    quarter(release_date)   as quarter,
    day(release_date)       as day
from dates