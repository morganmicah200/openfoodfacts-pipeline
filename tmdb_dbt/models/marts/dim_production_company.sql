-- Dimension table: production companies
-- Unpacks the pipe-delimited company_ids and company_names from movies_staging

with companies_raw as (
    select
        trim(c.value::string) as company_id,
        trim(n.value::string) as company_name
    from {{ ref('movies_staging') }},
        lateral flatten(input => split(company_ids, '|')) c,
        lateral flatten(input => split(company_names, '|')) n
    where company_ids != ''
        and company_names != ''
        and c.index = n.index
),

deduped as (
    select distinct
        try_cast(company_id as integer) as company_id,
        min(company_name) as company_name
    from companies_raw
    where try_cast(company_id as integer) is not null
    group by company_id
)

select
    row_number() over (order by company_id) as company_key,
    company_id,
    company_name
from deduped