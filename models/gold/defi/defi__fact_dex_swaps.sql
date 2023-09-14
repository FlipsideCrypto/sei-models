{{ config(
    materialized = 'view',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'SWAPS' }} }
) }}


{% set models = [ 
    ('astroport', ref('silver__dex_swaps_astroport')),
    ('fuzio', ref('silver__dex_swaps_fuzio')), 
    ('seaswap', ref('silver__dex_swaps_seaswap')) ] %}

SELECT
    *
FROM
    ({% for models in models %}
    SELECT
        '{{ models[0] }}' AS platform, 
        block_id,
        block_timestamp,
        tx_succeeded,
        tx_id,
        swapper,
        msg_group,
        msg_sub_group,
        msg_index,
        amount_in,
        currency_in,
        amount_out,
        currency_out,
        pool_address,
        pool_name,
        _inserted_timestamp
    FROM
        {{ models [1] }} 
                {% if not loop.last %}
        {% if is_incremental() %}
        {% endif %}
        UNION ALL
        {% endif %}
        {% endfor %}
 )