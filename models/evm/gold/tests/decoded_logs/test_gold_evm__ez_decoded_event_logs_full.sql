{{ config (
    materialized = "view",
    tags = ['full_evm_test']
) }}

SELECT
    *
FROM
    {{ ref('core_evm__ez_decoded_event_logs') }}