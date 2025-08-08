{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'pool_address',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['silver_dex','defi','dex','curated']
) }}

WITH contract_mapping AS (
    SELECT * FROM VALUES
        ('0x3976ccb6603f4af4f1f21ac1ac993e3776b8d590', 'uniswap', 'v3', 'uni_v3_pool_created'),
        ('0x385488ffbff2265dac31d38497b616fa6cc81231', 'uniswap', 'v3', 'uni_v3_pool_created'),
        ('0x75fc67473a91335b5b8f8821277262a13b38c9b3', 'uniswap', 'v3', 'uni_v3_pool_created'),
        ('0x23f1822ede8b1fa63c1652c2212a74481a6e6130', 'uniswap', 'v3', 'uni_v3_pool_created'),
        ('0x179d9a5592bc77050796f7be28058c51ca575df4', 'dragonswap', 'v2', 'uni_v3_pool_created'),
        ('0x0bcea088e977a03113a880cf7c5b6165d8304b16', 'dragonswap', 'v2', 'uni_v3_pool_created'),
        ('0x0596a0469d5452f876523487251bdde73d4b2597', 'xei', 'v3', 'uni_v3_pool_created'),
        ('0xdde4ff846706236eaae4367fff233c0e35a0613f', 'yeiswap', 'v1', 'uni_v3_pool_created'),
        ('0xa51136931fdd3875902618bf6b3abe38ab2d703b', 'sailor', 'v1', 'uni_v3_pool_created'),
        ('0x0a6816112d8fd84ae2111ab23a7e524865d923c5', 'sailor', 'v1', 'uni_v3_pool_created')
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
        LOWER(CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40))) AS token0_address,
        LOWER(CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40))) AS token1_address,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                's2c',
                topics [3] :: STRING
            )
        ) AS fee,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                's2c',
                segmented_data [0] :: STRING
            )
        ) AS tick_spacing,
        CONCAT('0x', SUBSTR(segmented_data [1] :: STRING, 25, 40)) AS pool_address,
        m.protocol,
        m.version,
        m.type,
        CONCAT(
            m.protocol,
            '-',
            m.version
        ) AS platform,
        'PoolCreated' AS event_name,
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
        topics [0] = '0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118' --PoolCreated
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
),
initial_info AS (
    SELECT
        contract_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        utils.udf_hex_to_int('s2c', CONCAT('0x', segmented_data [0] :: STRING)) :: FLOAT AS init_sqrtPriceX96,
        utils.udf_hex_to_int('s2c', CONCAT('0x', segmented_data [1] :: STRING)) :: FLOAT AS init_tick,
        pow(
            1.0001,
            init_tick
        ) AS init_price_1_0_unadj,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
        modified_timestamp
    FROM
        {{ ref('core_evm__fact_event_logs') }}
    WHERE
        topics [0] :: STRING = '0x98636036cb66a9c19a37435efc1e90142190214e8abeb821bdba3f2990dd4c95' --Initialize
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
),
FINAL AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        event_name,
        p.contract_address,
        token0_address,
        token1_address,
        fee :: INTEGER AS fee,
        (
            fee / 10000
        ) :: FLOAT AS fee_percent,
        tick_spacing,
        pool_address,
        platform,
        protocol,
        version,
        type,
        p._log_id,
        p.modified_timestamp
    FROM
        pools p
)
SELECT
    *
FROM
    FINAL qualify(ROW_NUMBER() over(PARTITION BY pool_address
ORDER BY
    modified_timestamp DESC)) = 1