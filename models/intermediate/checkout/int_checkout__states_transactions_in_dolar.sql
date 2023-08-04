{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key='id',
        partition_by = ['country']
    )
}}

SELECT
    acceptance.external_ref,
    acceptance.source,
    acceptance.date_time,
    acceptance.state = 'ACCEPTED' AS is_accepted,
    acceptance.cvv_provided,
    acceptance.amount / (acceptance.rates::json->>acceptance.currency)::NUMERIC AS amount_in_dolar,
    acceptance.country,
    acceptance.currency,
    chargeback.chargeback
FROM {{ref("stg_globepay__acceptance_report")}} acceptance

INNER JOIN {{ref('stg_globepay__chargeback_report')}} chargeback ON
chargeback.external_ref = acceptance.external_ref

{% if is_incremental() %}
    WHERE acceptance.date_time >= (SELECT MAX(date_time) FROM {{ this }})
{% endif %}


