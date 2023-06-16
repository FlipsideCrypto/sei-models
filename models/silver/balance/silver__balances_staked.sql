{{ config(
    materialized = 'view'
) }}

SELECT
    block_id,
    block_timestamp,
    tx_id,
    msg_group,
    delegator_address AS address,
    CASE
        WHEN action = 'delegate' THEN amount
        ELSE - amount
    END AS amount,
    SUM(amount) over(
        PARTITION BY address,
        currency
        ORDER BY
            block_timestamp,
            msg_group rows unbounded preceding
    ) AS balance,
    currency,
    _inserted_timestamp
FROM
    {{ ref('silver__staking') }}
