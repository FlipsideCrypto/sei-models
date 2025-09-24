{{ config (
    materialized = 'view'
) }}

SELECT
    VALUE,
    partition_key,
    metadata,
    DATA,
    file_name,
    _INSERTED_TIMESTAMP
FROM
    {{ ref('bronze__streamline_FR_tx_counts_v2') }}
