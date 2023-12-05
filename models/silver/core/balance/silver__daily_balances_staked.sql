{{ config(
    materialized = 'incremental',
    unique_key = ['date', 'address', 'currency'],
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(address);",
    tags = ['daily']
) }}

WITH all_staked AS (

    SELECT
        block_id,
        block_timestamp,
        address,
        balance,
        currency
    FROM
        {{ ref('silver__balances_staked') }}
)

{% if is_incremental() %},
recent AS (
    SELECT
        DATE,
        address,
        balance,
        currency
    FROM
        {{ this }}
    WHERE
        DATE = (
            SELECT
                DATEADD('day', -1, MAX(DATE))
            FROM
                {{ this }})
        ),
        NEW AS (
            SELECT
                block_timestamp :: DATE AS DATE,
                address,
                balance,
                currency,
                1 AS RANK
            FROM
                all_staked
            WHERE
                block_timestamp :: DATE >= (
                    SELECT
                        DATEADD('day', -1, MAX(DATE))
                    FROM
                        {{ this }}) qualify(ROW_NUMBER() over (PARTITION BY block_timestamp :: DATE, address, currency
                    ORDER BY
                        block_timestamp DESC)) = 1
                ),
                incremental AS (
                    SELECT
                        DATE,
                        address,
                        balance,
                        currency
                    FROM
                        (
                            SELECT
                                DATE,
                                address,
                                balance,
                                currency,
                                2 AS RANK
                            FROM
                                recent
                            UNION
                            SELECT
                                DATE,
                                address,
                                balance,
                                currency,
                                1 AS RANK
                            FROM
                                NEW
                        ) qualify(ROW_NUMBER() over (PARTITION BY DATE, address, currency
                    ORDER BY
                        RANK ASC)) = 1
                )
            {% endif %},
            base AS (

{% if is_incremental() %}
SELECT
    DATE AS block_timestamp, address, balance, currency
FROM
    incremental
{% else %}
SELECT
    block_timestamp, address, balance, currency
FROM
    all_staked
{% endif %}),
address_ranges AS (
    SELECT
        address,
        currency,
        MIN(
            block_timestamp :: DATE
        ) AS min_block_date,
        MAX (
            CURRENT_TIMESTAMP :: DATE
        ) AS max_block_date
    FROM
        base
    GROUP BY
        address,
        currency
),
ddate AS (
    SELECT
        date_day :: DATE AS DATE
    FROM
        {{ source(
            'crosschain',
            'dim_dates'
        ) }}
    GROUP BY
        DATE
),
all_dates AS (
    SELECT
        d.date,
        A.address,
        A.currency
    FROM
        ddate d
        LEFT JOIN address_ranges A
        ON d.date BETWEEN A.min_block_date
        AND A.max_block_date
    WHERE
        A.address IS NOT NULL
),
sei_balances AS (
    SELECT
        block_timestamp,
        address,
        balance,
        currency
    FROM
        base qualify(ROW_NUMBER() over (PARTITION BY block_timestamp :: DATE, address, currency
    ORDER BY
        block_timestamp DESC)) = 1
),
balance_temp AS (
    SELECT
        d.date,
        d.address,
        b.balance,
        d.currency
    FROM
        all_dates d
        LEFT JOIN sei_balances b
        ON d.date = b.block_timestamp :: DATE
        AND d.address = b.address
        AND d.currency = b.currency
)
SELECT
    DATE,
    'staked' AS balance_type,
    address,
    currency,
    LAST_VALUE(
        balance ignore nulls
    ) over(
        PARTITION BY address,
        currency,
        balance_type
        ORDER BY
            DATE ASC rows unbounded preceding
    ) AS balance,
    {{ dbt_utils.generate_surrogate_key(
        ['address','currency','date']
    ) }} AS daily_balances_staked_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    balance_temp
