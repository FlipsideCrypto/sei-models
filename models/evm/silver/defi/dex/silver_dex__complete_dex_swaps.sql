-- depends_on: {{ ref('price__ez_asset_metadata') }}
{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
  materialized = 'incremental',
  incremental_strategy = 'delete+insert',
  unique_key = ['block_number','platform','version'],
  cluster_by = ['block_timestamp::DATE','platform'],
  incremental_predicates = [fsc_evm.standard_predicate()],
  post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_hash, origin_function_signature, origin_from_address, origin_to_address, contract_address, pool_name, event_name, sender, tx_to, token_in, token_out, symbol_in, symbol_out)",
  tags = ['silver_dex','defi','dex','curated','heal','complete','swap']
) }}

WITH contracts AS (

  SELECT
    address AS contract_address,
    symbol AS token_symbol,
    decimals AS token_decimals,
    modified_timestamp AS _inserted_timestamp
  FROM
    {{ ref('core__dim_contracts') }}
  UNION ALL
  SELECT
    '0x0000000000000000000000000000000000000000' AS contract_address,
    '{{ vars.GLOBAL_NATIVE_ASSET_SYMBOL }}' AS token_symbol,
    decimals AS token_decimals,
    modified_timestamp AS _inserted_timestamp
  FROM
    {{ ref('core__dim_contracts') }}
  WHERE
    address = '{{ vars.GLOBAL_WRAPPED_NATIVE_ASSET_ADDRESS }}'
),
prices AS (
  SELECT
    token_address,
    price,
    HOUR,
    is_verified,
    modified_timestamp AS _inserted_timestamp
  FROM
    {{ ref('price__ez_prices_hourly') }}
  UNION ALL
  SELECT
    '0x0000000000000000000000000000000000000000' AS token_address,
    price,
    HOUR,
    is_verified,
    modified_timestamp AS _inserted_timestamp
  FROM
    {{ ref('price__ez_prices_hourly') }}
  WHERE
    token_address = '{{ vars.GLOBAL_WRAPPED_NATIVE_ASSET_ADDRESS }}'
),
swap_evt_v3 AS (

  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    pool_address AS contract_address,
    '0x' AS pool_id,
    event_name,
    amount_in_unadj,
    amount_out_unadj,
    token_in,
    token_out,
    sender,
    recipient AS tx_to,
    event_index,
    platform,
    protocol,
    version,
    type,
    _log_id,
    modified_timestamp AS _inserted_timestamp
  FROM
    {{ ref('silver_dex__swap_evt_v3_swaps') }}

{% if is_incremental() and 'swap_evt_v3' not in vars.CURATED_FR_MODELS %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ vars.CURATED_COMPLETE_LOOKBACK_HOURS }}'
    FROM
      {{ this }}
  )
{% endif %}
),
swap_evt_v2 AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    '0x' AS pool_id,
    event_name,
    amount_in_unadj,
    amount_out_unadj,
    token_in,
    token_out,
    sender,
    tx_to,
    event_index,
    platform,
    protocol,
    version,
    type,
    _log_id,
    modified_timestamp AS _inserted_timestamp
  FROM
    {{ ref('silver_dex__swap_evt_v2_swaps') }}

{% if is_incremental() and 'swap_evt_v2' not in vars.CURATED_FR_MODELS %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ vars.CURATED_COMPLETE_LOOKBACK_HOURS }}'
    FROM
      {{ this }}
  )
{% endif %}
),
jellyswap AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    '0x' AS pool_id,
    event_name,
    amount_in_unadj,
    amount_out_unadj,
    token_in,
    token_out,
    sender,
    tx_to,
    event_index,
    platform,
    protocol,
    version,
    type,
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver_dex__jellyswap_swaps') }}

{% if is_incremental() and 'jellyswap' not in vars.CURATED_FR_MODELS %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ vars.CURATED_COMPLETE_LOOKBACK_HOURS }}'
    FROM
      {{ this }}
  )
{% endif %}
),
oxium AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    '0x' AS pool_id,
    event_name,
    amount_in_unadj,
    amount_out_unadj,
    token_in,
    token_out,
    sender,
    tx_to,
    event_index,
    platform,
    protocol,
    version,
    type,
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver_dex__oxium_swaps') }}

{% if is_incremental() and 'oxium' not in vars.CURATED_FR_MODELS %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ vars.CURATED_COMPLETE_LOOKBACK_HOURS }}'
    FROM
      {{ this }}
  )
{% endif %}
),
all_dex AS (
  SELECT
    *
  FROM
    jellyswap
  UNION ALL
  SELECT
    *
  FROM
    swap_evt_v3
  UNION ALL
  SELECT
    *
  FROM
    swap_evt_v2
  UNION ALL
  SELECT
    *
  FROM
    oxium
),
complete_dex_swaps AS (
  SELECT
    s.block_number,
    s.block_timestamp,
    s.tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    s.contract_address,
    s.pool_id,
    event_name,
    token_in,
    p1.is_verified AS token_in_is_verified,
    c1.token_decimals AS decimals_in,
    c1.token_symbol AS symbol_in,
    amount_in_unadj,
    CASE
      WHEN decimals_in IS NULL THEN amount_in_unadj
      ELSE (amount_in_unadj / pow(10, decimals_in))
    END AS amount_in,
    CASE
      WHEN decimals_in IS NOT NULL THEN amount_in * p1.price
      ELSE NULL
    END AS amount_in_usd,
    token_out,
    p2.is_verified AS token_out_is_verified,
    c2.token_decimals AS decimals_out,
    c2.token_symbol AS symbol_out,
    amount_out_unadj,
    CASE
      WHEN decimals_out IS NULL THEN amount_out_unadj
      ELSE (amount_out_unadj / pow(10, decimals_out))
    END AS amount_out,
    CASE
      WHEN decimals_out IS NOT NULL THEN amount_out * p2.price
      ELSE NULL
    END AS amount_out_usd,
    CASE
      WHEN lp.pool_name IS NULL THEN CONCAT(
        LEAST(
          COALESCE(
            symbol_in,
            CONCAT(SUBSTRING(token_in, 1, 5), '...', SUBSTRING(token_in, 39, 42))
          ),
          COALESCE(
            symbol_out,
            CONCAT(SUBSTRING(token_out, 1, 5), '...', SUBSTRING(token_out, 39, 42))
          )
        ),
        '-',
        GREATEST(
          COALESCE(
            symbol_in,
            CONCAT(SUBSTRING(token_in, 1, 5), '...', SUBSTRING(token_in, 39, 42))
          ),
          COALESCE(
            symbol_out,
            CONCAT(SUBSTRING(token_out, 1, 5), '...', SUBSTRING(token_out, 39, 42))
          )
        )
      )
      ELSE lp.pool_name
    END AS pool_name,
    sender,
    tx_to,
    event_index,
    s.platform,
    s.protocol,
    s.version,
    s.type,
    s._log_id,
    s._inserted_timestamp
  FROM
    all_dex s
    LEFT JOIN contracts
    c1
    ON s.token_in = c1.contract_address
    LEFT JOIN contracts
    c2
    ON s.token_out = c2.contract_address
    LEFT JOIN prices
    p1
    ON s.token_in = p1.token_address
    AND DATE_TRUNC(
      'hour',
      block_timestamp
    ) = p1.hour
    LEFT JOIN prices
    p2
    ON s.token_out = p2.token_address
    AND DATE_TRUNC(
      'hour',
      block_timestamp
    ) = p2.hour
    LEFT JOIN {{ ref('silver_dex__complete_dex_liquidity_pools') }}
    lp
    ON s.contract_address = lp.pool_address
    AND s.pool_id = lp.pool_id
),

{% if is_incremental() and var(
  'HEAL_MODEL'
) %}
heal_model AS (
  SELECT
    t0.block_number,
    t0.block_timestamp,
    t0.tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    t0.contract_address,
    t0.pool_id,
    event_name,
    token_in,
    p1.is_verified AS token_in_is_verified,
    c1.token_decimals AS decimals_in,
    c1.token_symbol AS symbol_in,
    amount_in_unadj,
    CASE
      WHEN c1.token_decimals IS NULL THEN amount_in_unadj
      ELSE (amount_in_unadj / pow(10, c1.token_decimals))
    END AS amount_in_heal,
    CASE
      WHEN c1.token_decimals IS NOT NULL THEN amount_in_heal * p1.price
      ELSE NULL
    END AS amount_in_usd_heal,
    token_out,
    p2.is_verified AS token_out_is_verified,
    c2.token_decimals AS decimals_out,
    c2.token_symbol AS symbol_out,
    amount_out_unadj,
    CASE
      WHEN c2.token_decimals IS NULL THEN amount_out_unadj
      ELSE (amount_out_unadj / pow(10, c2.token_decimals))
    END AS amount_out_heal,
    CASE
      WHEN c2.token_decimals IS NOT NULL THEN amount_out_heal * p2.price
      ELSE NULL
    END AS amount_out_usd_heal,
    CASE
      WHEN lp.pool_name IS NULL THEN CONCAT(
        LEAST(
          COALESCE(
            c1.token_symbol,
            CONCAT(SUBSTRING(token_in, 1, 5), '...', SUBSTRING(token_in, 39, 42))
          ),
          COALESCE(
            c2.token_symbol,
            CONCAT(SUBSTRING(token_out, 1, 5), '...', SUBSTRING(token_out, 39, 42))
          )
        ),
        '-',
        GREATEST(
          COALESCE(
            c1.token_symbol,
            CONCAT(SUBSTRING(token_in, 1, 5), '...', SUBSTRING(token_in, 39, 42))
          ),
          COALESCE(
            c2.token_symbol,
            CONCAT(SUBSTRING(token_out, 1, 5), '...', SUBSTRING(token_out, 39, 42))
          )
        )
      )
      ELSE lp.pool_name
    END AS pool_name_heal,
    sender,
    tx_to,
    event_index,
    t0.platform,
    t0.protocol,
    t0.version,
    t0.type,
    t0._log_id,
    t0._inserted_timestamp
  FROM
    {{ this }}
    t0
    LEFT JOIN contracts
    c1
    ON t0.token_in = c1.contract_address
    LEFT JOIN contracts
    c2
    ON t0.token_out = c2.contract_address
    LEFT JOIN prices
    p1
    ON t0.token_in = p1.token_address
    AND DATE_TRUNC(
      'hour',
      block_timestamp
    ) = p1.hour
    LEFT JOIN prices
    p2
    ON t0.token_out = p2.token_address
    AND DATE_TRUNC(
      'hour',
      block_timestamp
    ) = p2.hour
    LEFT JOIN {{ ref('silver_dex__complete_dex_liquidity_pools') }}
    lp
    ON t0.contract_address = lp.pool_address
    AND t0.pool_id = lp.pool_id
  WHERE
    CONCAT(
      t0.block_number,
      '-',
      t0.platform,
      '-',
      t0.version
    ) IN (
      SELECT
        CONCAT(
          t1.block_number,
          '-',
          t1.platform,
          '-',
          t1.version
        )
      FROM
        {{ this }}
        t1
      WHERE
        t1.decimals_in IS NULL
        AND t1._inserted_timestamp < (
          SELECT
            MAX(
              _inserted_timestamp
            ) - INTERVAL '{{ vars.CURATED_COMPLETE_LOOKBACK_HOURS }}'
          FROM
            {{ this }}
        )
        AND EXISTS (
          SELECT
            1
          FROM
            contracts C
          WHERE
            C._inserted_timestamp > DATEADD('DAY', -14, SYSDATE())
            AND C.token_decimals IS NOT NULL
            AND C.contract_address = t1.token_in)
          GROUP BY
            1
        )
        OR CONCAT(
          t0.block_number,
          '-',
          t0.platform,
          '-',
          t0.version
        ) IN (
          SELECT
            CONCAT(
              t2.block_number,
              '-',
              t2.platform,
              '-',
              t2.version
            )
          FROM
            {{ this }}
            t2
          WHERE
            t2.decimals_out IS NULL
            AND t2._inserted_timestamp < (
              SELECT
                MAX(
                  _inserted_timestamp
                ) - INTERVAL '{{ vars.CURATED_COMPLETE_LOOKBACK_HOURS }}'
              FROM
                {{ this }}
            )
            AND EXISTS (
              SELECT
                1
              FROM
                contracts C
              WHERE
                C._inserted_timestamp > DATEADD('DAY', -14, SYSDATE())
                AND C.token_decimals IS NOT NULL
                AND C.contract_address = t2.token_out)
              GROUP BY
                1
            )
            OR CONCAT(
              t0.block_number,
              '-',
              t0.platform,
              '-',
              t0.version
            ) IN (
              SELECT
                CONCAT(
                  t3.block_number,
                  '-',
                  t3.platform,
                  '-',
                  t3.version
                )
              FROM
                {{ this }}
                t3
              WHERE
                t3.amount_in_usd IS NULL
                AND t3._inserted_timestamp < (
                  SELECT
                    MAX(
                      _inserted_timestamp
                    ) - INTERVAL '{{ vars.CURATED_COMPLETE_LOOKBACK_HOURS }}'
                  FROM
                    {{ this }}
                )
                AND EXISTS (
                  SELECT
                    1
                  FROM
                    prices
                    p
                  WHERE
                    p._inserted_timestamp > DATEADD('DAY', -14, SYSDATE())
                    AND p.price IS NOT NULL
                    AND p.token_address = t3.token_in
                    AND p.hour = DATE_TRUNC(
                      'hour',
                      t3.block_timestamp
                    )
                )
              GROUP BY
                1
            )
            OR CONCAT(
              t0.block_number,
              '-',
              t0.platform,
              '-',
              t0.version
            ) IN (
              SELECT
                CONCAT(
                  t4.block_number,
                  '-',
                  t4.platform,
                  '-',
                  t4.version
                )
              FROM
                {{ this }}
                t4
              WHERE
                t4.amount_out_usd IS NULL
                AND t4._inserted_timestamp < (
                  SELECT
                    MAX(
                      _inserted_timestamp
                    ) - INTERVAL '{{ vars.CURATED_COMPLETE_LOOKBACK_HOURS }}'
                  FROM
                    {{ this }}
                )
                AND EXISTS (
                  SELECT
                    1
                  FROM
                    prices
                    p
                  WHERE
                    p._inserted_timestamp > DATEADD('DAY', -14, SYSDATE())
                    AND p.price IS NOT NULL
                    AND p.token_address = t4.token_out
                    AND p.hour = DATE_TRUNC(
                      'hour',
                      t4.block_timestamp
                    )
                )
              GROUP BY
                1
            )
            OR     
            CONCAT(
              t0.block_number,
              '-',
              t0.platform,
              '-',
              t0.version
            ) IN (
                select concat(
                  t5.block_number,
                  '-',
                  t5.platform,
                  '-',
                  t5.version
                )
                from {{ this }} t5
                where t5.token_in in (
                  select token_address
                  from {{ ref('price__ez_asset_metadata') }}
                  where ifnull(is_verified_modified_timestamp, '1970-01-01' :: TIMESTAMP) > dateadd('day', -10, SYSDATE())
                )
              )
            OR concat(
              t0.block_number,
              '-',
              t0.platform,
              '-',
              t0.version
            ) IN (  
              select concat(
                t6.block_number,
                '-',
                t6.platform,
                '-',
                t6.version
              )
              from {{ this }} t6
              where t6.token_out in (
                select token_address
                from {{ ref('price__ez_asset_metadata') }}
                where ifnull(is_verified_modified_timestamp, '1970-01-01' :: TIMESTAMP) > dateadd('day', -10, SYSDATE())
              )
            )
        ),
      {% endif %}

      FINAL AS (
        SELECT
          *
        FROM
          complete_dex_swaps

{% if is_incremental() and var(
  'HEAL_MODEL'
) %}
UNION ALL
SELECT
  block_number,
  block_timestamp,
  tx_hash,
  origin_function_signature,
  origin_from_address,
  origin_to_address,
  contract_address,
  pool_id,
  event_name,
  token_in,
  token_in_is_verified,
  decimals_in,
  symbol_in,
  amount_in_unadj,
  amount_in_heal AS amount_in,
  amount_in_usd_heal AS amount_in_usd,
  token_out,
  token_out_is_verified,
  decimals_out,
  symbol_out,
  amount_out_unadj,
  amount_out_heal AS amount_out,
  amount_out_usd_heal AS amount_out_usd,
  pool_name_heal AS pool_name,
  sender,
  tx_to,
  event_index,
  platform,
  protocol,
  version,
  type,
  _log_id,
  _inserted_timestamp
FROM
  heal_model
{% endif %}
)
SELECT
  block_number,
  block_timestamp,
  tx_hash,
  origin_function_signature,
  origin_from_address,
  origin_to_address,
  contract_address,
  pool_name,
  pool_id,
  event_name,
  amount_in_unadj,
  amount_in,
  amount_in_usd,
  amount_out_unadj,
  amount_out,
  amount_out_usd,
  sender,
  tx_to,
  event_index,
  platform,
  protocol,
  version,
  type,
  token_in,
  IFNULL(
    token_in_is_verified,
    FALSE
  ) AS token_in_is_verified,
  token_out,
  IFNULL(
    token_out_is_verified,
    FALSE
  ) AS token_out_is_verified,
  symbol_in,
  symbol_out,
  decimals_in,
  decimals_out,
  _log_id,
  _inserted_timestamp,
  {{ dbt_utils.generate_surrogate_key(
    ['tx_hash','event_index']
  ) }} AS complete_dex_swaps_id,
  SYSDATE() AS inserted_timestamp,
  SYSDATE() AS modified_timestamp,
  '{{ invocation_id }}' AS _invocation_id
FROM
  FINAL qualify (ROW_NUMBER() over (PARTITION BY _log_id
ORDER BY
  _inserted_timestamp DESC)) = 1
