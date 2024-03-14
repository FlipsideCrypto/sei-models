{{ config(
    materialized = 'incremental',
    unique_key = "contract_address",
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    tags = ['noncore','recent_test']
) }}

SELECT
    A.contract_address,
    COALESCE(
        b.admin,
        C.admin
    ) AS admin,
    COALESCE(
        b.init_by_account_address,
        C.init_by_account_address
    ) AS init_by_account_address,
    A.init_by_block_timestamp,
    COALESCE(
        b.label,
        C.label
    ) AS label,
    {{ dbt_utils.generate_surrogate_key(
        ['a.contract_address']
    ) }} AS contracts_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    GREATEST(
        A._inserted_timestamp,
        COALESCE(
            b._inserted_timestamp,
            '2000-01-01'
        ),
        COALESCE(
            C._inserted_timestamp,
            '2000-01-01'
        )
    ) AS _inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ ref('silver__contracts_instantiate') }} A
    LEFT JOIN {{ ref('bronze_api__get_contract_list') }}
    b
    ON A.contract_address = b.contract_address
    LEFT JOIN {{ ref('silver__contract_labels') }} C
    ON A.contract_address = C.contract_address

{% if is_incremental() %}
WHERE
    GREATEST(
        A._inserted_timestamp,
        COALESCE(
            b._inserted_timestamp,
            '2000-01-01'
        ),
        COALESCE(
            C._inserted_timestamp,
            '2000-01-01'
        )
    ) :: DATE >= (
        SELECT
            MAX(
                _inserted_timestamp
            )
        FROM
            {{ this }}
    ) :: DATE - 7
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY A.contract_address
ORDER BY
    GREATEST(A._inserted_timestamp, b._inserted_timestamp) DESC) = 1)
