{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = 'day'
    )
}}

select
    date(date_time) AS day,
    safe_cast(value as integer) as steps,
    timestamp_trunc(current_timestamp(), second) as transformed_at,
from {{ source('fitbit_ingest', 'steps') }}
{% if is_incremental() %}
    where date(date_time) >= (select max(day) from {{ this }})
{% endif %}
