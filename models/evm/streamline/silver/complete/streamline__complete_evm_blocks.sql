-- depends_on: {{ ref('bronze_evm__streamline_blocks') }}
{{ config (
    materialized = "incremental",
    unique_key = "block_number",
    cluster_by = "ROUND(block_number, -3)",
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(block_number)",
    tags = ['streamline_core_evm_complete']
) }}

SELECT
    VALUE :BLOCK_NUMBER :: INT AS block_number,
    {{ dbt_utils.generate_surrogate_key(
        ['block_number']
    ) }} AS complete_blocks_evm_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    _inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM

{% if is_incremental() %}
{{ ref('bronze_evm__streamline_blocks') }}
WHERE
    _inserted_timestamp >= (
        SELECT
            COALESCE(MAX(_inserted_timestamp), '1970-01-01' :: TIMESTAMP) _inserted_timestamp
        FROM
            {{ this }})
        {% else %}
            {{ ref('bronze_evm__streamline_FR_blocks') }}
        {% endif %}

        qualify(ROW_NUMBER() over (PARTITION BY block_number
        ORDER BY
            _inserted_timestamp DESC)) = 1
