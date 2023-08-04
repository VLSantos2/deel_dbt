{{
    config(
        materialized='view'
    )
}}

SELECT
    external_ref,
    status,
    source,
    ref,
    TO_TIMESTAMP(date_time, 'YYYY-MM-DD"T"HH24:MI:SS.USZ') AS date_time,
    state,
    cvv_provided,
    amount,
    country,
    rates,
    currency
FROM {{source("globepay","acceptance_report")}}