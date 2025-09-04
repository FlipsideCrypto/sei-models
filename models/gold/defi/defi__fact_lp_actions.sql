{{ config(
    materialized = 'view',
    tags = ['noncore','recent_test']
) }}

SELECT
    'astroport' AS platform,
    A.block_id,
    A.block_timestamp,
    A.tx_succeeded,
    A.tx_id,
    A.msg_group,
    A.msg_sub_group,
    A.msg_index,
    A.liquidity_provider_address,
    CASE
        A.lp_action
        WHEN 'provide_liquidity' THEN 'add_liquidity'
        WHEN 'withdraw_liquidity' THEN 'remove_liquidity'
    END AS lp_action,
    A.pool_address,
    A.pool_name,
    A.token1_amount,
    A.token1_currency,
    A.token2_amount,
    A.token2_currency,
    lp_token_address,
    A._inserted_timestamp,
    COALESCE (
        lp_actions_astroport_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_id','msg_index']
        ) }}
    ) AS fact_lp_actions_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ source(
        'silver',
        'lp_actions_astroport'
    ) }} A
UNION ALL
SELECT
    'fuzio' AS platform,
    A.block_id,
    A.block_timestamp,
    A.tx_succeeded,
    A.tx_id,
    A.msg_group,
    A.msg_sub_group,
    A.msg_index,
    A.liquidity_provider_address,
    A.lp_action,
    A.pool_address,
    A.pool_name,
    A.token1_amount,
    A.token1_currency,
    A.token2_amount,
    A.token2_currency,
    lp_token_address,
    A._inserted_timestamp,
    COALESCE (
        lp_actions_fuzio_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_id','msg_index']
        ) }}
    ) AS fact_lp_actions_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ source(
        'silver',
        'lp_actions_fuzio'
    ) }} A
UNION ALL
SELECT
    'seaswap' AS platform,
    A.block_id,
    A.block_timestamp,
    A.tx_succeeded,
    A.tx_id,
    A.msg_group,
    A.msg_sub_group,
    A.msg_index,
    A.liquidity_provider_address,
    A.lp_action,
    A.pool_address,
    A.pool_name,
    A.token1_amount,
    A.token1_currency,
    A.token2_amount,
    A.token2_currency,
    lp_token_address,
    A._inserted_timestamp,
    COALESCE (
        lp_actions_seaswap_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_id','msg_index']
        ) }}
    ) AS fact_lp_actions_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ source(
        'silver',
        'lp_actions_seaswap'
    ) }} A
UNION ALL
SELECT
    'levana' AS platform,
    A.block_id,
    A.block_timestamp,
    A.tx_succeeded,
    A.tx_id,
    A.msg_group,
    A.msg_sub_group,
    A.msg_index,
    A.liquidity_provider_address,
    CASE
        A.lp_action
        WHEN 'deposit-lp' THEN 'add_liquidity'
        WHEN 'deposit-xlp' THEN 'add_liquidity'
        WHEN 'reinvest-yield-lp' THEN 'add_liquidity'
        WHEN 'reinvest-yield-xlp' THEN 'add_liquidity'
        WHEN 'withdraw' THEN 'remove_liquidity'
    END AS lp_action,
    A.pool_address,
    A.pool_name,
    A.token1_amount,
    A.token1_currency,
    A.token2_amount,
    A.token2_currency,
    lp_token_address,
    A._inserted_timestamp,
    COALESCE (
        lp_actions_levana_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_id','msg_index']
        ) }}
    ) AS fact_lp_actions_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__lp_actions_levana') }} A
WHERE
    lp_action IN (
        'deposit-lp',
        'deposit-xlp',
        'reinvest-yield-lp',
        'reinvest-yield-xlp',
        'withdraw'
    )
