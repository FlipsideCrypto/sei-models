-- depends_on: {{ ref('bronze__streamline_tx_counts') }}
{{ config (
    materialized = "incremental",
    unique_key = "block_number",
    cluster_by = "ROUND(block_number, -3)",
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(block_number)"
) }}

SELECT
    REPLACE(
        COALESCE(
            metadata :request :data :params [0],
            metadata :request :params [0]
        ),
        'tx.height='
    ) :: INT AS block_number,
    DATA :: INT AS tx_count,
    {{ dbt_utils.generate_surrogate_key(
        ['block_number']
    ) }} AS complete_tx_counts_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    _inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM

{% if is_incremental() %}
{{ ref('bronze__streamline_tx_counts') }}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) _inserted_timestamp
        FROM
            {{ this }}
    )
{% else %}
    {{ ref('bronze__streamline_FR_tx_counts') }}
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY block_number
ORDER BY
    _inserted_timestamp DESC)) = 1
