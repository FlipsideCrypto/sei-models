{{ config(
    materialized = 'view',
    tags = ['core']
) }}

SELECT
    block_id,
    block_timestamp,
    tx_id,
    tx_succeeded,
    CONCAT(
        msg_group,
        ':',
        msg_sub_group
    ) AS msg_group,
    msg_index,
    msg_type,
    attribute_index,
    attribute_key,
    attribute_value,
    COALESCE (
        msg_attributes_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_id','msg_index','attribute_index']
        ) }}
    ) AS fact_msg_attributes_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__msg_attributes') }}
