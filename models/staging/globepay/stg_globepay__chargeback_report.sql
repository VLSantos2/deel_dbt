{{
    config(
        materialized='view'
    )
}}
SELECT 
    external_ref,
    status,
    source,
    chargeback
FROM {{source("globepay","chargeback_report")}}