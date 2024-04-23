-- depends_on: {{ ref('bronze__streamline_transactions') }}
{{ config (
    materialized = "incremental",
    unique_key = "complete_transactions_id",
    cluster_by = "ROUND(block_number, -3)",
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(block_number)"
) }}

SELECT
    DATA :height :: INT AS block_number,
    COALESCE(
        VALUE :PAGE_NUMBER,
        metadata :request :data :params [2],
        metadata :request :params [2]
    ) :: INT AS page_number,
    {{ dbt_utils.generate_surrogate_key(
        ['block_number','page_number']
    ) }} AS complete_transactions_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    _inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM

{% if is_incremental() %}
{{ ref('bronze__streamline_transactions') }}
{% else %}
    {{ ref('bronze__streamline_FR_transactions') }}
{% endif %}
WHERE
    DATA <> '[]'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) _inserted_timestamp
    FROM
        {{ this }}
)
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY complete_transactions_id
ORDER BY
    _inserted_timestamp DESC)) = 1
