{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'pool_address',
    tags = ['silver_dex','defi','dex','curated']
) }}

WITH contract_mapping AS (
SELECT * FROM VALUES
        ('0x595a6b190ba0163eac1da988955563635ab3aa60', 'uniswap', 'v2', 'uni_v2_pair_created'),
        ('0x4b4746216214f9e972c5d35d3fe88e6ec4c28a6b', 'donkeswap', 'v2', 'uni_v2_pair_created'),
        ('0x5d370a6189f89603fab67e9c68383e63f7b6a262', 'dragonswap', 'v1', 'uni_v2_pair_created'),
        ('0x71b27deea549cf3e9293544b2af7a193a34be29a', 'dragonswap', 'v1', 'uni_v2_pair_created')
    AS t(contract_address, protocol, version, type)
),
pools AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        l.contract_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS token0,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS token1,
        CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 25, 40)) AS pool_address,
        utils.udf_hex_to_int(
            segmented_data [1] :: STRING
        ) :: INT AS pool_id,
        m.protocol,
        m.version,
        m.type,
        CONCAT(
            m.protocol,
            '-',
            m.version
        ) AS platform,
        'PairCreated' AS event_name,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
        modified_timestamp
    FROM
        {{ ref('core_evm__fact_event_logs') }}
        l
        INNER JOIN contract_mapping m
        ON l.contract_address = m.contract_address
    WHERE
        topics [0] :: STRING = '0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9' --PairCreated
        AND tx_succeeded

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(modified_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND modified_timestamp >= SYSDATE() - INTERVAL '7 days'
{% endif %}
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    contract_address,
    event_index,
    event_name,
    token0,
    token1,
    pool_address,
    pool_id,
    platform,
    protocol,
    version,
    type,
    _log_id,
    modified_timestamp
FROM
    pools qualify(ROW_NUMBER() over (PARTITION BY pool_address
ORDER BY
    modified_timestamp DESC)) = 1
