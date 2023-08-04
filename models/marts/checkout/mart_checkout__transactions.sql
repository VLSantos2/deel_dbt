{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key='id'
    )
}}

SELECT
    -- Dimensions
    MD5(date_time::DATE::TEXT || EXTRACT(HOUR FROM date_time)::TEXT || country || cvv_provided::TEXT || chargeback::TEXT || currency) AS id,
    date_time::DATE AS ref_date,
    EXTRACT(HOUR FROM date_time) AS hour,
    country,
    cvv_provided,
    chargeback,
    currency,

    -- Totals
    COUNT(*) AS total_transactions,
    SUM(amount_in_dolar) AS total_amount,

    -- Amounts
    SUM(CASE WHEN NOT is_accepted THEN amount_in_dolar ELSE 0 END) AS declined_volume,
    SUM(CASE WHEN is_accepted THEN amount_in_dolar ELSE 0 END) AS accepeted_volume,
    AVG(NULLIF(CASE WHEN NOT is_accepted THEN amount_in_dolar END, 0)) AS avg_declined_volume,
    AVG(NULLIF(CASE WHEN is_accepted THEN amount_in_dolar END, 0)) AS avg_accepted_volume,

    -- Accept 
    SUM(CASE WHEN is_accepted THEN 1 ELSE 0 END) AS total_accepted,
    SUM(CASE WHEN is_accepted THEN 0 ELSE 1 END) AS total_declined,
    AVG(CASE WHEN is_accepted THEN 1 ELSE 0 END) AS acceptance_rate

FROM {{ref("int_checkout__states_transactions_in_dolar")}} 
{% if is_incremental() %}
    WHERE date_time::DATE >= (SELECT MAX(ref_date) - INTERVAL '1' DAY FROM {{ this }})
{% endif %}
GROUP BY 1, 2, 3, 4, 5, 6, 7